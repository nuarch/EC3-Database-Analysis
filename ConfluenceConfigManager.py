import os
from typing import Dict, Any, List
import json

class ConfluenceConfigManager:
    """Configuration manager for Confluence Page Creator"""
    
    def __init__(self, config_file: str = "confluence_config.json"):
        self.config_file = config_file
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or environment variables"""
        config = {}
        
        # Try to load from the JSON file first
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load config file {self.config_file}: {e}")
        
        # Override with environment variables if they exist
        config.update({
            'confluence_url': os.getenv('CONFLUENCE_URL', config.get('confluence_url', '')),
            'username': os.getenv('CONFLUENCE_USERNAME', config.get('username', '')),
            'api_token': os.getenv('CONFLUENCE_API_TOKEN', config.get('api_token', '')),
            'space_key': os.getenv('CONFLUENCE_SPACE_KEY', config.get('space_key', '')),
            'default_parent_title': config.get('default_parent_title', ''),
            'default_labels': config.get('default_labels', []),
        })
        
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)
    
    def get_default_labels(self) -> List[str]:
        """Get default labels as a list"""
        labels = self.get('default_labels', [])
        if isinstance(labels, list):
            return labels
        elif isinstance(labels, str):
            # Handle case where labels are stored as comma-separated string
            return [label.strip() for label in labels.split(',') if label.strip()]
        else:
            return []
    
    def is_complete(self) -> bool:
        """Check if all required configuration is present"""
        required_keys = ['confluence_url', 'username', 'api_token']
        return all(self._config.get(key) for key in required_keys)
