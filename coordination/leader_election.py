# leader_election.py
import asyncio
import json
import logging
import os
import random
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

# Constants
MIN_ELECTION_TIMEOUT = 1500  # ms
MAX_ELECTION_TIMEOUT = 3000  # ms
HEARTBEAT_INTERVAL = 500  # ms
RPC_RETRY_DELAY = 100  # ms

class RaftState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftError(Exception):
    """Base exception for Raft consensus failures"""

@dataclass
class LogEntry:
    term: int
    command: bytes
    index: int

@dataclass
class RaftConfig:
    node_id: str
    peers: List[str]  # Other node IDs
    data_dir: str = "./raft_data"
    snapshot_threshold: int = 1000  # Num logs before snapshot

@dataclass
class PersistentState:
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

@dataclass
class VolatileState:
    commit_index: int = 0
    last_applied: int = 0
    leader_id: Optional[str] = None

class RaftNode(ABC):
    def __init__(self, config: RaftConfig):
        self.config = config
        self.state = RaftState.FOLLOWER
        self.persistent = PersistentState()
        self.volatile = VolatileState()
        self.election_timer = None
        self.heartbeat_timer = None
        self.lock = threading.RLock()
        self.logger = logging.getLogger(f"RaftNode[{config.node_id}]")
        self._load_persistent_state()

        # Leader-specific state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

    def _load_persistent_state(self) -> None:
        path = os.path.join(self.config.data_dir, f"{self.config.node_id}_state.json")
        if not os.path.exists(path):
            return
            
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                self.persistent.current_term = data['current_term']
                self.persistent.voted_for = data['voted_for']
                self.persistent.log = [
                    LogEntry(term=e['term'], command=e['command'].encode(), index=e['index'])
                    for e in data['log']
                ]
        except Exception as e:
            self.logger.error(f"Failed to load persistent state: {str(e)}")

    def _save_persistent_state(self) -> None:
        path = os.path.join(self.config.data_dir, f"{self.config.node_id}_state.json")
        data = {
            'current_term': self.persistent.current_term,
            'voted_for': self.persistent.voted_for,
            'log': [
                {'term': e.term, 'command': e.command.decode(), 'index': e.index}
                for e in self.persistent.log
            ]
        }
        try:
            with open(path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            self.logger.error(f"Failed to save persistent state: {str(e)}")

    def _reset_election_timer(self) -> None:
        if self.election_timer:
            self.election_timer.cancel()
        timeout = random.randint(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT) / 1000
        self.election_timer = threading.Timer(timeout, self._start_election)
        self.election_timer.start()

    def _start_election(self) -> None:
        with self.lock:
            if self.state != RaftState.FOLLOWER:
                return
                
            self.state = RaftState.CANDIDATE
            self.persistent.current_term += 1
            self.persistent.voted_for = self.config.node_id
            self._save_persistent_state()
            
            self.logger.info(f"Starting election for term {self.persistent.current_term}")
            self._request_votes()

    def _request_votes(self) -> None:
        # Implement RPC to peers (simplified example)
        votes_received = 1  # Self-vote
        for peer in self.config.peers:
            # Simulated RPC call
            try:
                response = self._send_request_vote_rpc(peer)
                if response['term'] > self.persistent.current_term:
                    self._step_down(response['term'])
                    return
                if response['vote_granted']:
                    votes_received += 1
            except Exception as e:
                self.logger.error(f"Vote request to {peer} failed: {str(e)}")

        if votes_received > len(self.config.peers) // 2:
            self._become_leader()

    def _become_leader(self) -> None:
        with self.lock:
            self.state = RaftState.LEADER
            self.volatile.leader_id = self.config.node_id
            self.next_index = {peer: len(self.persistent.log) + 1 for peer in self.config.peers}
            self.match_index = {peer: 0 for peer in self.config.peers}
            self._reset_heartbeat_timer()
            self.logger.info(f"Elected leader for term {self.persistent.current_term}")

    def _reset_heartbeat_timer(self) -> None:
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(HEARTBEAT_INTERVAL / 1000, self._send_heartbeats)
        self.heartbeat_timer.start()

    def _send_heartbeats(self) -> None:
        for peer in self.config.peers:
            try:
                self._send_append_entries_rpc(peer)
            except Exception as e:
                self.logger.error(f"Heartbeat to {peer} failed: {str(e)}")
        self._reset_heartbeat_timer()

    def _send_append_entries_rpc(self, peer: str) -> None:
        # Implement actual RPC logic
        pass

    def _step_down(self, term: int) -> None:
        with self.lock:
            if term > self.persistent.current_term:
                self.persistent.current_term = term
                self.persistent.voted_for = None
                self._save_persistent_state()
                
            self.state = RaftState.FOLLOWER
            self.volatile.leader_id = None
            self._reset_election_timer()
            self.logger.info(f"Stepped down to follower at term {self.persistent.current_term}")

    @abstractmethod
    def _send_request_vote_rpc(self, peer: str) -> Dict:
        """Implement actual RPC to peer node"""
        pass

    @abstractmethod
    def apply_log(self, command: bytes) -> None:
        """Apply command to state machine"""
        pass

    def start(self) -> None:
        self._reset_election_timer()

    def stop(self) -> None:
        if self.election_timer:
            self.election_timer.cancel()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()

class SampleRaftNode(RaftNode):
    def _send_request_vote_rpc(self, peer: str) -> Dict:
        # Simplified RPC simulation
        return {
            'term': self.persistent.current_term,
            'vote_granted': True
        }

    def apply_log(self, command: bytes) -> None:
        # Implement state machine logic
        pass

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    config = RaftConfig(
        node_id="node1",
        peers=["node2", "node3"],
        data_dir="./raft_data"
    )
    
    node = SampleRaftNode(config)
    node.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
