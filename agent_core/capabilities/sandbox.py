# sandbox.py
import ctypes
import logging
import os
import resource
import signal
import subprocess
import sys
from dataclasses import dataclass
from enum import IntEnum
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

# libseccomp setup
LIBSECCOMP = ctypes.CDLL("libseccomp.so.2")

class scmp_arch(IntEnum):
    NATIVE = 0
    X86_64 = 1
    X86 = 2
    ARM = 3

class scmp_action(IntEnum):
    KILL_PROCESS = 0x00000000
    TRAP = 0x00030000
    ERRNO = 0x00050000
    TRACE = 0x7FF00000
    ALLOW = 0x7FFF0000

class ScmpFilterContext(ctypes.Structure):
    pass

# libseccomp bindings
LIBSECCOMP.seccomp_init.argtypes = [ctypes.c_uint]
LIBSECCOMP.seccomp_init.restype = ctypes.POINTER(ScmpFilterContext)
LIBSECCOMP.seccomp_rule_add.argtypes = [ctypes.POINTER(ScmpFilterContext), ctypes.c_uint, ctypes.c_int, ctypes.c_uint]
LIBSECCOMP.seccomp_load.argtypes = [ctypes.POINTER(ScmpFilterContext)]
LIBSECCOMP.seccomp_arch_add.argtypes = [ctypes.POINTER(ScmpFilterContext), ctypes.c_uint]
LIBSECCOMP.seccomp_release.argtypes = [ctypes.POINTER(ScmpFilterContext)]

# Sandbox configuration
@dataclass
class SandboxPolicy:
    max_cpu_seconds: int = 5
    max_memory_mb: int = 100
    allowed_syscalls: set = frozenset([
        "read", "write", "open", "close", "fstat",
        "mmap", "mprotect", "munmap", "brk", "exit_group"
    ])
    read_only_paths: set = frozenset(["/lib", "/usr/lib", "/etc/localtime"])
    writable_tmp: bool = True

class SandboxViolation(Exception):
    pass

class Sandbox:
    def __init__(self, policy: SandboxPolicy = SandboxPolicy()):
        self.policy = policy
        self.logger = logging.getLogger("Sandbox")
        
    def _set_resource_limits(self):
        # CPU time limit
        resource.setrlimit(
            resource.RLIMIT_CPU,
            (self.policy.max_cpu_seconds, self.policy.max_cpu_seconds + 1)
        )
        # Memory limit
        resource.setrlimit(
            resource.RLIMIT_AS,
            (self.policy.max_memory_mb * 1024 * 1024, 
             self.policy.max_memory_mb * 1024 * 1024)
        )
        # Disable fork
        resource.setrlimit(
            resource.RLIMIT_NPROC,
            (0, 0)
        )
        
    def _setup_seccomp(self):
        ctx = LIBSECCOMP.seccomp_init(scmp_action.KILL_PROCESS.value)
        
        # Allow base syscalls
        for syscall in self.policy.allowed_syscalls:
            syscall_num = ctypes.c_int(LIBSECCOMP.seccomp_syscall_resolve_name(syscall.encode()))
            LIBSECCOMP.seccomp_rule_add(
                ctx, scmp_action.ALLOW.value, syscall_num, 0
            )
            
        # Load seccomp filter
        LIBSECCOMP.seccomp_load(ctx)
        LIBSECCOMP.seccomp_release(ctx)
        
    def _create_namespace(self):
        # Unshare PID, network, mount, IPC
        os.unshare(os.CLONE_NEWPID | os.CLONE_NEWNS | os.CLONE_NEWNET | os.CLONE_NEWIPC)
        
        # Mount tmpfs for /tmp
        if self.policy.writable_tmp:
            os.mount("tmpfs", "/tmp", "tmpfs", 0, "size=100M,nr_inodes=1k,mode=777")
            
        # Remount paths read-only
        for path in self.policy.read_only_paths:
            if os.path.exists(path):
                os.mount(path, path, "", os.MS_REMOUNT | os.MS_RDONLY, "")
                
    def _child_execution(self, func: Callable, *args, **kwargs):
        # Apply security layers
        self._set_resource_limits()
        self._setup_seccomp()
        self._create_namespace()
        
        # Execute wrapped function
        try:
            return func(*args, **kwargs)
        except Exception as e:
            self.logger.error(f"Sandboxed execution failed: {str(e)}")
            sys.exit(1)
            
    def run(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Fork into isolated child
            pid = os.fork()
            if pid == 0:
                self._child_execution(func, *args, **kwargs)
                os._exit(0)
                
            # Parent process monitors child
            _, status = os.waitpid(pid, 0)
            
            if os.WIFSIGNALED(status):
                sig = os.WTERMSIG(status)
                raise SandboxViolation(f"Process killed by signal {sig}")
                
            if os.WEXITSTATUS(status) != 0:
                raise SandboxViolation("Sandbox policy violation detected")
                
            return None
        return wrapper

# Example Usage
if __name__ == "__main__":
    sandbox = Sandbox()
    
    @sandbox.run
    def untrusted_code():
        print("Running in secure sandbox!")
        # This would be terminated:
        # import os; os.fork()
        
    try:
        untrusted_code()
    except SandboxViolation as e:
        print(f"Security violation: {str(e)}")
