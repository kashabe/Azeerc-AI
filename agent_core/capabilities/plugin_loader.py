# plugin_loader.py
import importlib
import importlib.util
import logging
import os
import pkgutil
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Type, TypeVar

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# Type variables
T = TypeVar('T')

# Constants
PLUGIN_BASE_CLASS_NAME = "PluginBase"
MANIFEST_FILE = "manifest.json"
SIGNATURE_FILE = "signature.sig"
PUBLIC_KEY_PATH = "keys/plugin_public.pem"

class PluginBase:
    """Base class for all Azeerc AI plugins"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def initialize(self) -> None:
        """Initialize plugin resources"""
        raise NotImplementedError
        
    def execute(self, data: Any) -> Any:
        """Main plugin execution logic"""
        raise NotImplementedError
        
    def cleanup(self) -> None:
        """Release plugin resources"""
        raise NotImplementedError

class PluginSecurityError(Exception):
    """Plugin signature validation failure"""

class PluginLoader:
    def __init__(self):
        self.loaded_plugins: Dict[str, PluginBase] = {}
        self._public_key = self._load_public_key()
        
    def _load_public_key(self):
        """Load RSA public key for signature verification"""
        with open(PUBLIC_KEY_PATH, "rb") as key_file:
            return serialization.load_pem_public_key(
                key_file.read(),
                backend=default_backend()
            )
    
    def _verify_signature(self, plugin_path: Path) -> bool:
        """Verify plugin package signature"""
        try:
            with open(plugin_path / SIGNATURE_FILE, "rb") as sig_file:
                signature = sig_file.read()
                
            manifest_data = (plugin_path / MANIFEST_FILE).read_bytes()
            
            self._public_key.verify(
                signature,
                manifest_data,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except InvalidSignature:
            return False
        except Exception as e:
            logging.error(f"Signature verification failed: {str(e)}")
            return False

    def _load_plugin_config(self, plugin_path: Path) -> Dict[str, Any]:
        """Load plugin manifest configuration"""
        manifest_path = plugin_path / MANIFEST_FILE
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing {MANIFEST_FILE} in {plugin_path}")
            
        return json.loads(manifest_path.read_text())

    def _validate_plugin_class(self, module: ModuleType) -> Type[PluginBase]:
        """Validate plugin implementation class"""
        if not hasattr(module, PLUGIN_BASE_CLASS_NAME):
            raise AttributeError(f"Missing {PLUGIN_BASE_CLASS_NAME} implementation")
            
        plugin_class = getattr(module, PLUGIN_BASE_CLASS_NAME)
        if not issubclass(plugin_class, PluginBase):
            raise TypeError(f"{plugin_class.__name__} must inherit from PluginBase")
            
        return plugin_class

    def _load_single_plugin(self, plugin_path: Path) -> Optional[PluginBase]:
        """Load and validate a single plugin package"""
        try:
            if not self._verify_signature(plugin_path):
                raise PluginSecurityError("Invalid plugin signature")
                
            config = self._load_plugin_config(plugin_path)
            sys.path.insert(0, str(plugin_path.parent))
            
            # Dynamic module import
            spec = importlib.util.spec_from_file_location(
                config["package_name"], 
                plugin_path / config["entry_point"]
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            plugin_class = self._validate_plugin_class(module)
            instance = plugin_class(config)
            instance.initialize()
            return instance
            
        except Exception as e:
            logging.error(f"Failed to load plugin {plugin_path.name}: {str(e)}")
            return None

    def load_all_plugins(self, plugin_dir: Path) -> None:
        """Load all valid plugins from directory"""
        if not plugin_dir.is_dir():
            raise NotADirectoryError(f"Invalid plugin directory: {plugin_dir}")
            
        for plugin_path in plugin_dir.iterdir():
            if plugin_path.is_dir() and (plugin_path / MANIFEST_FILE).exists():
                plugin = self._load_single_plugin(plugin_path)
                if plugin:
                    self.loaded_plugins[plugin_path.name] = plugin

    def get_plugin(self, name: str) -> PluginBase:
        """Retrieve loaded plugin instance"""
        if name not in self.loaded_plugins:
            raise KeyError(f"Plugin {name} not loaded")
        return self.loaded_plugins[name]

    def unload_plugin(self, name: str) -> None:
        """Cleanup and remove a loaded plugin"""
        if name in self.loaded_plugins:
            try:
                self.loaded_plugins[name].cleanup()
            except Exception as e:
                logging.error(f"Plugin {name} cleanup failed: {str(e)}")
            del self.loaded_plugins[name]

# Example Usage
if __name__ == "__main__":
    loader = PluginLoader()
    plugins_dir = Path(__file__).parent / "plugins"
    
    # Load plugins
    loader.load_all_plugins(plugins_dir)
    
    # Use a data processing plugin
    try:
        data_plugin = loader.get_plugin("data_cleaner")
        processed = data_plugin.execute({"raw_data": "example"})
    except KeyError:
        logging.warning("Data cleaner plugin not available")
        
    # Unload all plugins on shutdown
    for name in list(loader.loaded_plugins.keys()):
        loader.unload_plugin(name)
