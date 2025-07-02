from xml.etree.ElementTree import indent

import requests
import base64
import json
import os
import glob
from typing import Optional, Dict, Any, List
from ConfluenceConfigManager import ConfluenceConfigManager

class ConfluencePageCreator:
    # ... existing methods ...
    def __init__(self, base_url: str, username: str, api_token: str):
        """
        Initialize the Confluence page creator.
        
        Args:
            base_url: Your Confluence instance URL (e.g., 'https://yourcompany.atlassian.net')
            username: Your Confluence username/email
            api_token: Your Confluence API token
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.api_token = api_token
        self.session = requests.Session()
        
        # Set up authentication
        auth_string = f"{username}:{api_token}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        
        self.session.headers.update({
            'Authorization': f'Basic {auth_b64}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    @classmethod
    def from_config(cls, config: ConfluenceConfigManager) -> 'ConfluencePageCreator':
        """
        Create ConfluencePageCreator instance from configuration.
        
        Args:
            config: Configuration instance
            
        Returns:
            ConfluencePageCreator instance
        """
        return cls(
            base_url=config.get('confluence_url'),
            username=config.get('username'),
            api_token=config.get('api_token')
        )
    
    def add_labels_to_page(self, page_id: str, labels: List[str]) -> bool:
        """
        Add labels to an existing Confluence page using REST API v2.
        
        Args:
            page_id: The ID of the page
            labels: List of label names to add
            
        Returns:
            True if successful, False otherwise
        """
        if not labels:
            return True
        
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}/label"
        
        # Prepare labels in the format required by Confluence API v2
        label_data = [{"prefix": "global", "name": label} for label in labels]
        
        try:
            response = self.session.post(url, json=label_data)
            response.raise_for_status()
            
            print(f"   ✅ Added labels: {', '.join(labels)}")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Failed to add labels {labels}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"      Response: {e.response.text[:200]}")
            return False
    
    def get_page_labels(self, page_id: str) -> List[str]:
        """
        Get all labels for a specific page using REST API v2.
        
        Args:
            page_id: The ID of the page
            
        Returns:
            List of label names
        """
        url = f"{self.base_url}/wiki/api/v2/pages/{page_id}/labels"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            return [label['name'] for label in data.get('results', [])]
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching labels for page {page_id}: {e}")
            return []
    
    def get_space_id_from_key(self, space_key: str) -> Optional[str]:
        """
        Get space ID from space key (needed for v2 API).
        
        Args:
            space_key: The space key
            
        Returns:
            Space ID if found, None otherwise
        """
        url = f"{self.base_url}/wiki/api/v2/spaces?keys={space_key}"

        try:
            response = self.session.get(url)
            response.raise_for_status()

            space_data = response.json()
            if space_data.get('results') and len(space_data['results']) > 0:
                return space_data['results'][0].get('id')
            return None

        except requests.exceptions.RequestException as e:
            print(f"Error fetching space ID for key {space_key}: {e}")
            return None
    
    def get_page_by_title(self, space_key: str, title: str) -> Optional[Dict[str, Any]]:
        """
        Get a page by its title in a specific space.
        Helper method for backward compatibility - uses v2 API.
        
        Args:
            space_key: The space key where the page is located
            title: The title of the page
            
        Returns:
            Page information if found, None otherwise
        """
        # First get the space ID from space key
        space_id = self.get_space_id_from_key(space_key)
        if not space_id:
            print(f"Error: Could not retrieve space ID for space key '{space_key}'")
            return None
        
        url = f"{self.base_url}/wiki/api/v2/pages"
        params = {
            'title': title,
            'space-id': space_id,
            'body-format': 'storage'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('results') and len(data['results']) > 0:
                page = data['results'][0]
                # Add computed fields for compatibility
                page['spaceId'] = space_id
                if page.get('parentId'):
                    page['parentPageId'] = page['parentId']
                return page
            
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def get_page_by_id_with_space_id(self, space_id: str, page_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a page by its ID in a specific space using space ID.
        
        Args:
            space_id: The space ID where the page is located
            page_id: The ID of the page
            
        Returns:
            Page information if found, None otherwise
        """
        url = f"{self.base_url}/wiki/api/v2/pages/{page_id}"
        params = {
            'body-format': 'storage'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            page = response.json()
            
            # Verify the page belongs to the specified space
            if page.get('spaceId') != space_id:
                print(f"Error: Page {page_id} does not belong to space {space_id}")
                return None
            
            # Add computed fields for compatibility
            page['spaceId'] = space_id
            if page.get('parentId'):
                page['parentPageId'] = page['parentId']
            
            return page
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_id} from space {space_id}: {e}")
            return None

    def get_page_by_id(self, page_id: str, expand: str = "body,version,space") -> Optional[Dict[str, Any]]:
        """
        Get a page by its ID using v2 API.
        
        Args:
            page_id: The ID of the page
            expand: Comma-separated list of properties to expand
            
        Returns:
            Page information if found, None otherwise
        """
        url = f"{self.base_url}/wiki/api/v2/pages/{page_id}"
        params = {'body-format': 'storage'}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            page = response.json()
            
            # Add computed fields for compatibility
            if page.get('parentId'):
                page['parentPageId'] = page['parentId']
            
            return page
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page by ID {page_id}: {e}")
            return None

    def get_available_content_files(self, content_dir: str = "./confluence_docs") -> List[Dict[str, str]]:
        """
        Get available content files from the confluence_docs directory.
        Updated to handle ADF JSON files instead of XML files.
        
        Args:
            content_dir: Directory containing pre-generated content files
            
        Returns:
            List of dictionaries containing file information
        """
        available_files = []
        
        if not os.path.exists(content_dir):
            print(f"⚠️  Content directory '{content_dir}' does not exist")
            return available_files
        
        # Look for ADF JSON files and their corresponding metadata files
        json_files = glob.glob(os.path.join(content_dir, "**/*.json"), recursive=True)
        
        # Filter out metadata files and only process ADF content files
        adf_files = [f for f in json_files if not f.endswith('_metadata.json')]
        
        for adf_file in adf_files:
            base_name = os.path.splitext(adf_file)[0]
            metadata_file = f"{base_name}_metadata.json"
            
            file_info = {
                'adf_file': adf_file,
                'metadata_file': metadata_file if os.path.exists(metadata_file) else None,
                'name': os.path.basename(base_name),
                'relative_path': os.path.relpath(adf_file, content_dir)
            }
            
            # Try to read metadata if a metadata file exists
            file_info['title'] = file_info['name']
            if file_info['metadata_file']:
                try:
                    with open(file_info['metadata_file'], 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        file_info['title'] = metadata.get('title', file_info['name'])
                        file_info['name'] = metadata.get('stored_procedure_name', 'Unknown')
                        file_info['schema'] = metadata.get('schema', 'Unknown')
                        file_info['complexity'] = metadata.get('complexity', 'Unknown')
                        file_info['description'] = metadata.get('description', '')
                        file_info['metadata'] = metadata
                except Exception as e:
                    print(f"Warning: Could not read metadata from {file_info['metadata_file']}: {e}")
                    file_info['metadata'] = {}
            else:
                file_info['metadata'] = {}

            # Add properties so that the page is full width
            file_info['metadata']['content-appearance-draft'] = 'full-width'
            file_info['metadata']['content-appearance-published'] = 'full-width'

            available_files.append(file_info)
        
        return sorted(available_files, key=lambda x: x['title'])
    
    def load_content_from_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Load ADF content from a JSON file.
        Updated to handle ADF JSON format instead of XML.
        
        Args:
            file_path: Path to the ADF JSON content file
            
        Returns:
            ADF document as dictionary, or None if error
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                adf_content = json.load(f)
                
            # Validate that this is a proper ADF document
            if not isinstance(adf_content, dict) or adf_content.get('type') != 'doc':
                print(f"Warning: File {file_path} does not appear to be a valid ADF document")
                return None
                
            return adf_content
        except Exception as e:
            print(f"Error loading ADF content from {file_path}: {e}")
            return None
    
    def set_page_properties(self, page_id: str, properties: Dict[str, Any]) -> bool:
        """
        Set properties for a Confluence page.
        
        Args:
            page_id: The ID of the page
            properties: Dictionary of properties to set
            
        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/wiki/api/v2/pages/{page_id}/properties"
        
        success_count = 0
        total_properties = len(properties)
        
        for key, value in properties.items():
            # Skip certain keys that shouldn't be page properties
            if key.lower() in ['title', 'id', 'content', 'body']:
                continue
                
            property_data = {
                'key': key,
                'value': value
            }
            
            try:
                response = self.session.post(url, data=json.dumps(property_data))

                if response.status_code in [200, 201]:
                    success_count += 1
                    print(f"   ✅ Set property '{key}': {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
                else:
                    print(f"   ❌ Failed to set property '{key}': {response.status_code} - {response.text[:100]}")

            except requests.exceptions.RequestException as e:
                print(f"   ❌ Error setting property '{key}': {e}")
        
        return success_count > 0
    
    def read_page_content_by_id_with_space_id(self, space_id: str, page_id: str) -> Optional[str]:
        """
        Read the content of a page by ID in a specific space.
        
        Args:
            space_id: The space ID where the page is located
            page_id: The ID of the page
            
        Returns:
            Page content (HTML) if found, None otherwise
        """
        page = self.get_page_by_id_with_space_id(space_id, page_id)
        if page and 'body' in page and 'storage' in page['body']:
            return page['body']['storage']['value']
        return None

    def read_page_content(self, space_key: str, title: str) -> Optional[str]:
        """
        Read the content of a page by title.
        Helper method for backward compatibility.
        
        Args:
            space_key: The space key where the page is located
            title: The title of the page
            
        Returns:
            Page content (HTML) if found, None otherwise
        """
        page = self.get_page_by_title(space_key, title)
        if page and 'body' in page and 'storage' in page['body']:
            return page['body']['storage']['value']
        return None
    
    def read_page_content_by_id(self, page_id: str) -> Optional[str]:
        """
        Read the content of a page by ID.
        
        Args:
            page_id: The ID of the page
            
        Returns:
            Page content (HTML) if found, None otherwise
        """
        page = self.get_page_by_id(page_id)
        if page and 'body' in page and 'storage' in page['body']:
            return page['body']['storage']['value']
        return None
    
    def get_page_info_by_id_with_space_id(self, space_id: str, page_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a page by ID in a specific space.
        
        Args:
            space_id: The space ID where the page is located
            page_id: The ID of the page
            
        Returns:
            Dictionary with page information if found, None otherwise
        """
        page = self.get_page_by_id_with_space_id(space_id, page_id)
        if not page:
            return None
        
        info = {
            'id': page['id'],
            'title': page['title'],
            'type': 'page',
            'status': page['status'],
            'spaceId': space_id,
            'version': {
                'number': page.get('version', {}).get('number', 1),
                'when': page.get('version', {}).get('when', 'Unknown'),
                'by': page.get('version', {}).get('by', {}).get('displayName', 'Unknown')
            },
            'url': f"{self.base_url}/pages/{page['id']}",
            'edit_url': f"{self.base_url}/pages/editpage.action?pageId={page['id']}"
        }
        
        # Add parent information if available
        if page.get('parentPageId'):
            info['parentPageId'] = page['parentPageId']
        
        return info

    def get_page_info(self, space_key: str, title: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a page.
        Helper method for backward compatibility.
        
        Args:
            space_key: The space key where the page is located
            title: The title of the page
            
        Returns:
            Dictionary with page information if found, None otherwise
        """
        # Get space ID first
        space_id = self.get_space_id_from_key(space_key)
        if not space_id:
            return None
        
        # Get page by title to find the page ID
        page = self.get_page_by_title(space_key, title)
        if not page:
            return None
        
        # Use the ID-based method
        return self.get_page_info_by_id_with_space_id(space_id, page['id'])
    
    def search_pages_by_space_id(self, space_id: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for pages in a space by space ID using v2 API.
        
        Args:
            space_id: The space ID to search in
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of page information dictionaries
        """
        url = f"{self.base_url}/wiki/api/v2/pages"
        params = {
            'space-id': space_id,
            'title': query,
            'limit': limit
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for page in data.get('results', []):
                results.append({
                    'id': page['id'],
                    'title': page['title'],
                    'type': 'page',
                    'status': page['status'],
                    'spaceId': space_id,
                    'url': f"{self.base_url}/pages/{page['id']}"
                })
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"Error searching pages: {e}")
            return []

    def search_pages(self, space_key: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for pages in a space.
        Helper method for backward compatibility.
        
        Args:
            space_key: The space key to search in
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of page information dictionaries
        """
        # Get space ID first
        space_id = self.get_space_id_from_key(space_key)
        if not space_id:
            return []
        
        return self.search_pages_by_space_id(space_id, query, limit)
    
    def get_child_pages_by_id(self, page_id: str) -> List[Dict[str, Any]]:
        """
        Get child pages of a specific page by page ID using v2 API.
        
        Args:
            page_id: The ID of the parent page
            
        Returns:
            List of child page information
        """
        url = f"{self.base_url}/wiki/api/v2/pages"
        params = {'parent-id': page_id}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for page in data.get('results', []):
                results.append({
                    'id': page['id'],
                    'title': page['title'],
                    'type': 'page',
                    'status': page['status'],
                    'parentPageId': page_id,
                    'url': f"{self.base_url}/pages/{page['id']}"
                })
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching child pages: {e}")
            return []

    def get_child_pages(self, page_id: str) -> List[Dict[str, Any]]:
        """
        Get child pages of a specific page.
        
        Args:
            page_id: The ID of the parent page
            
        Returns:
            List of child page information
        """
        return self.get_child_pages_by_id(page_id)

    def create_page_by_space_id(self, space_id: str, title: str, content: Any,
        parent_page_id: Optional[str] = None, labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new page in Confluence using space ID and optional parent page ID.
        Updated to handle both ADF (dict) and HTML/storage format (str) content.

        Args:
            space_id: The space ID where the page will be created
            title: The title of the new page
            content: The content of the page (ADF dict or HTML/storage format string)
            parent_page_id: Optional parent page ID to create the page under
            labels: Optional list of labels to add to the page

        Returns:
            Created page information if successful, None otherwise
        """
        # Determine if content is ADF (dict) or storage format (str)
        if isinstance(content, dict):
            # ADF format
            body_data = {
                'value': json.dumps(content),
                'representation': 'atlas_doc_format'
            }
        else:
            # HTML/Storage format (legacy)
            body_data = {
                'value': content,
                'representation': 'storage'
            }

        # For v2 API, use the pages endpoint
        url = f"{self.base_url}/wiki/api/v2/pages"

        page_data = {
            'spaceId': space_id,
            'status': 'current',
            'title': title,
            'body': body_data
        }
        
        if parent_page_id:
            page_data['parentId'] = parent_page_id

        try:
            response = self.session.post(url, json=page_data)
            response.raise_for_status()

            page = response.json()
            # Add space ID and parent page ID to response
            page['spaceId'] = space_id
            page['parentPageId'] = parent_page_id

            # Add labels if provided
            if labels:
                page_id = page['id']
                print(f"📝 Adding labels to page '{title}' (ID: {page_id})...")
                if self.add_labels_to_page(page_id, labels):
                    print(f"✅ Labels added successfully to page '{title}'")
                else:
                    print(f"⚠️  Page created but some labels may not have been added")

            return page

        except requests.exceptions.RequestException as e:
            print(f"Error creating page: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print("Response content:", e.response.text)
            return None

    def create_page(self, space_key: str, title: str, content: Any,
        parent_page_id: Optional[str] = None, labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new page in Confluence using the v2 API.
        Helper method for backward compatibility.

        Args:
            space_key: The space key where the page will be created
            title: The title of the new page
            content: The content of the page (ADF dict or HTML/storage format string)
            parent_page_id: Optional parent page ID to create the page under
            labels: Optional list of labels to add to the page

        Returns:
            Created page information if successful, None otherwise
        """
        # Get space ID first
        space_id = self.get_space_id_from_key(space_key)
        if not space_id:
            print(f"Error: Could not retrieve space ID for space key '{space_key}'")
            return None

        return self.create_page_by_space_id(space_id, title, content, parent_page_id, labels)

    def create_page_with_properties_by_space_id(self, space_id: str, title: str, content: Any, 
                                  properties: Dict[str, Any], parent_page_id: Optional[str] = None,
                                  labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new page in Confluence using space ID and set its properties.
        
        Args:
            space_id: The space ID where the page will be created
            title: The title of the new page
            content: The content of the page (ADF dict or HTML/storage format string)
            properties: Dictionary of properties to set for the page
            parent_page_id: Optional parent page ID to create the page under
            labels: Optional list of labels to add to the page
            
        Returns:
            Created page information if successful, None otherwise
        """
        # First create the page with labels
        result = self.create_page_by_space_id(space_id, title, content, parent_page_id, labels)
        
        if result and properties:
            page_id = result['id']
            print(f"📝 Setting page properties for '{title}' (ID: {page_id})...")
            
            # Set the properties
            if self.set_page_properties(page_id, properties):
                print(f"✅ Properties set successfully for page '{title}'")
            else:
                print(f"⚠️  Page created but some properties may not have been set")
        
        return result

    def create_page_with_properties(self, space_key: str, title: str, content: Any, 
                                  properties: Dict[str, Any], parent_page_id: Optional[str] = None,
                                  labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new page in Confluence and set its properties.
        Helper method for backward compatibility.
        
        Args:
            space_key: The space key where the page will be created
            title: The title of the new page
            content: The content of the page (ADF dict or HTML/storage format string)
            properties: Dictionary of properties to set for the page
            parent_page_id: Optional parent page ID to create the page under
            labels: Optional list of labels to add to the page
            
        Returns:
            Created page information if successful, None otherwise
        """
        # Get space ID first
        space_id = self.get_space_id_from_key(space_key)
        if not space_id:
            print(f"Error: Could not retrieve space ID for space key '{space_key}'")
            return None

        return self.create_page_with_properties_by_space_id(space_id, title, content, properties, parent_page_id, labels)

    def create_child_page_by_ids(self, space_id: str, parent_page_id: str,
        child_title: str, child_content: Any, labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a child page under a specified parent page using IDs.

        Args:
            space_id: The space ID
            parent_page_id: ID of the parent page
            child_title: Title of the new child page
            child_content: Content of the child page (ADF dict or HTML/storage format string)
            labels: Optional list of labels to add to the page

        Returns:
            Created page information if successful, None otherwise
        """
        print(f"✅ Creating child page under parent ID: {parent_page_id}")

        # Create the child page using the parent ID with labels
        return self.create_page_by_space_id(space_id, child_title, child_content, parent_page_id, labels)

    def create_child_page(self, space_key: str, parent_title: str,
        child_title: str, child_content: Any, labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a child page under a specified parent page.
        Helper method for backward compatibility.

        Args:
            space_key: The space key
            parent_title: Title of the parent page
            child_title: Title of the new child page
            child_content: Content of the child page (ADF dict or HTML/storage format string)
            labels: Optional list of labels to add to the page

        Returns:
            Created page information if successful, None otherwise
        """
        # First, find the parent page ID from title
        parent_page = self.get_page_by_title(space_key, parent_title)
        if not parent_page:
            print(f"❌ Parent page '{parent_title}' not found in space '{space_key}'")
            return None

        parent_id = parent_page['id']
        space_id = parent_page['spaceId']
        print(f"✅ Found parent page: {parent_title} (ID: {parent_id})")

        # Create the child page using IDs
        return self.create_child_page_by_ids(space_id, parent_id, child_title, child_content, labels)

    def create_child_page_with_properties_by_ids(self, space_id: str, parent_page_id: str,
        child_title: str, child_content: Any,
        properties: Dict[str, Any], labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a child page under a specified parent page using IDs and set its properties.

        Args:
            space_id: The space ID
            parent_page_id: ID of the parent page
            child_title: Title of the new child page
            child_content: Content of the child page (ADF dict or HTML/storage format string)
            properties: Dictionary of properties to set for the page
            labels: Optional list of labels to add to the page

        Returns:
            Created page information if successful, None otherwise
        """
        print(f"✅ Creating child page with properties under parent ID: {parent_page_id}")

        # Create the child page with properties using IDs
        return self.create_page_with_properties_by_space_id(space_id, child_title, child_content, properties, parent_page_id, labels)

    def create_child_page_with_properties(self, space_key: str, parent_title: str,
        child_title: str, child_content: Any,
        properties: Dict[str, Any], labels: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Create a child page under a specified parent page and set its properties.
        Helper method for backward compatibility.

        Args:
            space_key: The space key
            parent_title: Title of the parent page
            child_title: Title of the new child page
            child_content: Content of the child page (ADF dict or HTML/storage format string)
            properties: Dictionary of properties to set for the page
            labels: Optional list of labels to add to the page

        Returns:
            Created page information if successful, None otherwise
        """
        # First, find the parent page ID from title
        parent_page = self.get_page_by_title(space_key, parent_title)
        if not parent_page:
            print(f"❌ Parent page '{parent_title}' not found in space '{space_key}'")
            return None

        parent_id = parent_page['id']
        space_id = parent_page['spaceId']
        print(f"✅ Found parent page: {parent_title} (ID: {parent_id})")

        # Create the child page with properties using IDs
        return self.create_child_page_with_properties_by_ids(space_id, parent_id, child_title, child_content, properties, labels)

# Helper functions for bulk operations
def create_child_pages_from_directory_by_ids(creator: ConfluencePageCreator, space_id: str, parent_page_id: str, 
                                            content_dir: str = "./confluence_docs", labels: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Create child pages from all ADF files in a directory using space ID and parent page ID.
    Updated to support labels and use REST API v2.
    
    Args:
        creator: ConfluencePageCreator instance
        space_id: The space ID where pages will be created
        parent_page_id: ID of the parent page under which child pages will be created
        content_dir: Directory containing ADF content files
        labels: Optional list of labels to add to all created pages
        
    Returns:
        Dictionary with operation results
    """
    print(f"🚀 Starting bulk creation of child pages from '{content_dir}'")
    print(f"   Parent Page ID: {parent_page_id}")
    print(f"   Space ID: {space_id}")
    if labels:
        print(f"   Labels: {', '.join(labels)}")
    
    available_files = creator.get_available_content_files(content_dir)
    
    if not available_files:
        return {
            'success': False,
            'message': f"No ADF content files found in {content_dir}",
            'created_pages': [],
            'failed_pages': []
        }
    
    print(f"📁 Found {len(available_files)} ADF content files")
    
    created_pages = []
    failed_pages = []
    
    for i, file_info in enumerate(available_files, 1):
        print(f"\n📄 Processing file {i}/{len(available_files)}: {file_info['title']}")
        
        # Load ADF content from JSON file
        adf_content = creator.load_content_from_file(file_info['adf_file'])
        if not adf_content:
            print(f"   ❌ Failed to load ADF content from {file_info['adf_file']}")
            failed_pages.append({
                'file': file_info['adf_file'],
                'title': file_info['title'],
                'error': 'Failed to load ADF content'
            })
            continue
        
        # Create child page with properties and labels
        properties = file_info.get('metadata', {})
        result = creator.create_child_page_with_properties_by_ids(
            space_id, parent_page_id, file_info['title'], adf_content, properties, labels
        )
        
        if result:
            created_pages.append({
                'id': result['id'],
                'title': file_info['title'],
                'file': file_info['adf_file'],
                'url': f"{creator.base_url}/pages/{result['id']}"
            })
            print(f"   ✅ Successfully created: {file_info['title']} (ID: {result['id']})")
        else:
            failed_pages.append({
                'file': file_info['adf_file'],
                'title': file_info['title'],
                'error': 'Failed to create page'
            })
            print(f"   ❌ Failed to create: {file_info['title']}")
    
    return {
        'success': len(created_pages) > 0,
        'message': f"Created {len(created_pages)} pages, {len(failed_pages)} failed",
        'created_pages': created_pages,
        'failed_pages': failed_pages
    }

def create_child_pages_from_directory(creator: ConfluencePageCreator, space_key: str, parent_title: str, 
                                    content_dir: str = "./confluence_docs", labels: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Create child pages from all ADF files in a directory.
    Helper function for backward compatibility.
    Updated to support labels.
    
    Args:
        creator: ConfluencePageCreator instance
        space_key: The space key where pages will be created
        parent_title: Title of the parent page under which child pages will be created
        content_dir: Directory containing ADF content files
        labels: Optional list of labels to add to all created pages
        
    Returns:
        Dictionary with operation results
    """
    # Get space ID from space key
    space_id = creator.get_space_id_from_key(space_key)
    if not space_id:
        return {
            'success': False,
            'message': f"Could not retrieve space ID for space key '{space_key}'",
            'created_pages': [],
            'failed_pages': []
        }
    
    # Get parent page ID from title
    parent_page = creator.get_page_by_title(space_key, parent_title)
    if not parent_page:
        return {
            'success': False,
            'message': f"Parent page '{parent_title}' not found in space '{space_key}'",
            'created_pages': [],
            'failed_pages': []
        }
    
    parent_page_id = parent_page['id']
    
    # Use the ID-based function
    return create_child_pages_from_directory_by_ids(creator, space_id, parent_page_id, content_dir, labels)

def create_child_pages_from_directory_by_ids_with_schema_hierarchy(creator: ConfluencePageCreator, space_id: str, parent_page_id: str, 
                                                                  content_dir: str = "./confluence_docs", labels: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Create child pages from all ADF files in a directory with schema hierarchy.
    Creates schema pages as children of the parent, then procedures as children of schema pages.
    
    Args:
        creator: ConfluencePageCreator instance
        space_id: The space ID where pages will be created
        parent_page_id: ID of the parent page under which schema pages will be created
        content_dir: Directory containing ADF content files
        labels: Optional list of labels to add to all created pages
        
    Returns:
        Dictionary with operation results including schema pages and procedure pages
    """
    print(f"🚀 Starting schema hierarchy creation from '{content_dir}'")
    print(f"   Parent Page ID: {parent_page_id}")
    print(f"   Space ID: {space_id}")
    searchLabels = ""
    if labels:
        print(f"   Labels: {', '.join(labels)}")
        temp = labels.copy()
        temp = [f'\"{label}\"' for label in temp]
        searchLabels = ', '.join(temp)
    
    available_files = creator.get_available_content_files(content_dir)
    
    if not available_files:
        return {
            'success': False,
            'message': f"No ADF content files found in {content_dir}",
            'created_schema_pages': [],
            'created_pages': [],
            'failed_pages': []
        }
    
    print(f"📁 Found {len(available_files)} ADF content files")
    
    # Group files by schema
    schema_groups = {}
    for file_info in available_files:
        schema = file_info.get('schema', 'Unknown')
        if schema not in schema_groups:
            schema_groups[schema] = []
        schema_groups[schema].append(file_info)
    
    print(f"📊 Found {len(schema_groups)} schemas: {', '.join(schema_groups.keys())}")
    
    created_schema_pages = []
    created_pages = []
    failed_pages = []
    
    # Create schema pages first
    for schema_name, files in schema_groups.items():
        print(f"\n📂 Creating schema page for: {schema_name}")
        
        # Create schema page content
        schema_content = {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type":"extension",
                    "attrs":{
                        "layout":"full-width",
                        "extensionType":"com.atlassian.confluence.macro.core",
                        "extensionKey":"detailssummary",
                        "parameters":{
                            "macroParams":{
                                "cql":{
                                    "value":"label in ("+ searchLabels + ") and space = currentSpace ( ) and ancestor = "
                                            "currentContent ( )"
                                }
                            },
                            "macroMetadata":{
                                "schemaVersion":{
                                    "value":"3"
                                },
                                "title":"Page Properties Report"
                            }
                        }
                    }
                }
            ]
        }
        
        # Create schema page
        schema_result = creator.create_child_page_by_ids(
            space_id, parent_page_id, f"{schema_name}", schema_content, labels
        )
        
        if schema_result:
            schema_page_info = {
                'id': schema_result['id'],
                'title': f"{schema_name}",
                'url': f"{creator.base_url}/pages/{schema_result['id']}"
            }
            created_schema_pages.append(schema_page_info)
            print(f"   ✅ Created schema page: {schema_name} (ID: {schema_result['id']})")
            
            # Now create procedure pages under this schema page
            schema_page_id = schema_result['id']
            
            for i, file_info in enumerate(files, 1):
                print(f"\n   📄 Processing procedure {i}/{len(files)}: {file_info['title']}")
                
                # Load ADF content from JSON file
                adf_content = creator.load_content_from_file(file_info['adf_file'])
                if not adf_content:
                    print(f"      ❌ Failed to load ADF content from {file_info['adf_file']}")
                    failed_pages.append({
                        'file': file_info['adf_file'],
                        'title': file_info['title'],
                        'schema': schema_name,
                        'error': 'Failed to load ADF content'
                    })
                    continue
                
                # Create procedure page with properties and labels under schema page
                properties = file_info.get('metadata', {})
                result = creator.create_child_page_with_properties_by_ids(
                    space_id, schema_page_id, file_info['title'], adf_content, properties, labels
                )
                
                if result:
                    created_pages.append({
                        'id': result['id'],
                        'title': file_info['title'],
                        'schema': schema_name,
                        'schema_page_id': schema_page_id,
                        'file': file_info['adf_file'],
                        'url': f"{creator.base_url}/pages/{result['id']}"
                    })
                    print(f"      ✅ Successfully created: {file_info['title']} (ID: {result['id']})")
                else:
                    failed_pages.append({
                        'file': file_info['adf_file'],
                        'title': file_info['title'],
                        'schema': schema_name,
                        'error': 'Failed to create page'
                    })
                    print(f"      ❌ Failed to create: {file_info['title']}")
        else:
            print(f"   ❌ Failed to create schema page for: {schema_name}")
            # Mark all files in this schema as failed
            for file_info in files:
                failed_pages.append({
                    'file': file_info['adf_file'],
                    'title': file_info['title'],
                    'schema': schema_name,
                    'error': f'Failed to create parent schema page for {schema_name}'
                })
    
    return {
        'success': len(created_pages) > 0 or len(created_schema_pages) > 0,
        'message': f"Created {len(created_schema_pages)} schema pages and {len(created_pages)} procedure pages, {len(failed_pages)} failed",
        'created_schema_pages': created_schema_pages,
        'created_pages': created_pages,
        'failed_pages': failed_pages
    }

def create_child_pages_from_directory_with_schema_hierarchy(creator: ConfluencePageCreator, space_key: str, parent_title: str, 
                                                           content_dir: str = "./confluence_docs", labels: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Create child pages from all ADF files in a directory with schema hierarchy.
    Helper function for backward compatibility.
    
    Args:
        creator: ConfluencePageCreator instance
        space_key: The space key where pages will be created
        parent_title: Title of the parent page under which schema pages will be created
        content_dir: Directory containing ADF content files
        labels: Optional list of labels to add to all created pages
        
    Returns:
        Dictionary with operation results including schema pages and procedure pages
    """
    # Get space ID from space key
    space_id = creator.get_space_id_from_key(space_key)
    if not space_id:
        return {
            'success': False,
            'message': f"Could not retrieve space ID for space key '{space_key}'",
            'created_schema_pages': [],
            'created_pages': [],
            'failed_pages': []
        }
    
    # Get parent page ID from title
    parent_page = creator.get_page_by_title(space_key, parent_title)
    if not parent_page:
        return {
            'success': False,
            'message': f"Parent page '{parent_title}' not found in space '{space_key}'",
            'created_schema_pages': [],
            'created_pages': [],
            'failed_pages': []
        }
    
    parent_page_id = parent_page['id']
    
    # Use the ID-based function with schema hierarchy
    return create_child_pages_from_directory_by_ids_with_schema_hierarchy(creator, space_id, parent_page_id, content_dir, labels)

def interactive_mode():
    """
    Interactive mode for creating and reading pages with user input.
    Enhanced to work directly with space ID and page ID for better performance.
    Updated to support ADF JSON content files and labels.
    """
    print("=== Confluence Page Creator & Reader ===")
    
    # Load configuration
    config = ConfluenceConfigManager()

    # Get missing configuration from user if needed
    confluence_url = config.get('confluence_url')
    if not confluence_url:
        confluence_url = input("Enter your Confluence URL (e.g., https://yourcompany.atlassian.net): ").strip()
    
    username = config.get('username')
    if not username:
        username = input("Enter your username/email: ").strip()
    
    api_token = config.get('api_token')
    if not api_token:
        api_token = input("Enter your API token: ").strip()
    
    # Request space key and convert to space ID
    space_key = config.get('space_key')
    if not space_key:
        space_key = input("Enter the space key: ").strip()
    
    creator = ConfluencePageCreator(confluence_url, username, api_token)
    
    # Get space ID from space key
    space_id = creator.get_space_id_from_key(space_key)
    if not space_id:
        print(f"❌ Could not retrieve space ID for space key '{space_key}'")
        return
    
    print(f"✅ Using space: {space_key} (ID: {space_id})")
    
    # Get default labels from config
    default_labels = config.get_default_labels()
    if default_labels:
        print(f"🏷️  Default labels configured: {', '.join(default_labels)}")
    
    def get_labels_from_user(default_labels: List[str]) -> List[str]:
        """Helper function to get labels from user input"""
        if default_labels:
            labels_prompt = f"Enter labels (comma-separated) [{', '.join(default_labels)}]: "
            labels_input = input(labels_prompt).strip()
            if not labels_input:
                return default_labels
            else:
                return [label.strip() for label in labels_input.split(',') if label.strip()]
        else:
            labels_input = input("Enter labels (comma-separated, optional): ").strip()
            if labels_input:
                return [label.strip() for label in labels_input.split(',') if label.strip()]
            else:
                return []
    
    def select_confluence_docs_subdirectory():
        """Helper function to select a subdirectory from ./confluence_docs"""
        base_dir = "./confluence_docs"
        
        if not os.path.exists(base_dir):
            print(f"❌ Base directory '{base_dir}' does not exist")
            return None
        
        # Get all subdirectories
        subdirs = [d for d in os.listdir(base_dir) 
                  if os.path.isdir(os.path.join(base_dir, d)) and not d.startswith('.')]
        
        if not subdirs:
            print(f"❌ No subdirectories found in '{base_dir}'")
            return None
        
        print(f"\n📁 Available subdirectories in {base_dir}:")
        for i, subdir in enumerate(subdirs, 1):
            subdir_path = os.path.join(base_dir, subdir)
            # Count JSON files in subdirectory
            json_count = len([f for f in os.listdir(subdir_path) 
                            if f.endswith('.json') and not f.endswith('_metadata.json')])
            print(f"   {i}. {subdir} ({json_count} content files)")
        
        try:
            selection = int(input(f"\nSelect subdirectory (1-{len(subdirs)}): ").strip())
            if 1 <= selection <= len(subdirs):
                selected_subdir = os.path.join(base_dir, subdirs[selection - 1])
                print(f"✅ Selected directory: {selected_subdir}")
                return selected_subdir
            else:
                print("❌ Invalid selection")
                return None
        except ValueError:
            print("❌ Please enter a valid number")
            return None
    
    while True:
        print("\n" + "="*50)
        print("1. Create a standalone page")
        print("2. Create a child page under existing page")
        print("3. Create page from confluence_docs content (ADF)")
        print("4. Create child page from confluence_docs content (ADF)")
        print("5. Create child pages for ALL ADF files in directory")
        print("6. Browse available content files")
        print("7. Read a page by ID")
        print("8. Get page information by ID")
        print("9. Search pages")
        print("10. List child pages by parent ID")
        print("11. Exit")
        
        choice = input("\nSelect an option (1-11): ").strip()
        
        if choice == "1":
            title = input("Enter page title: ").strip()
            content = input("Enter page content (HTML): ").strip()
            labels = get_labels_from_user(default_labels)
            
            result = creator.create_page_by_space_id(space_id, title, content, labels=labels)
            if result:
                print(f"✅ Page '{title}' created successfully!")
                print(f"   ID: {result['id']}")
                print(f"   URL: {confluence_url}/pages/{result['id']}")
            else:
                print("❌ Failed to create page")
                
        elif choice == "2":
            default_parent_id = config.get('default_parent_page_id', '')
            parent_prompt = f"Enter parent page ID{f' [{default_parent_id}]' if default_parent_id else ''}: "
            parent_page_id = input(parent_prompt).strip() or default_parent_id
            
            if not parent_page_id:
                print("❌ Parent page ID is required")
                continue
            
            child_title = input("Enter child page title: ").strip()
            child_content = input("Enter child page content (HTML): ").strip()
            labels = get_labels_from_user(default_labels)
            
            result = creator.create_child_page_by_ids(space_id, parent_page_id, child_title, child_content, labels)
            if result:
                print(f"✅ Child page '{child_title}' created successfully!")
                print(f"   ID: {result['id']}")
                print(f"   URL: {confluence_url}/pages/{result['id']}")
            else:
                print("❌ Failed to create child page")
        
        elif choice == "3":
            # Create standalone page from confluence_docs ADF content
            selected_dir = select_confluence_docs_subdirectory()
            if not selected_dir:
                continue
                
            available_files = creator.get_available_content_files(selected_dir)
            if not available_files:
                print(f"❌ No ADF content files found in {selected_dir}")
                continue
            
            print(f"\n📁 Available ADF Content Files ({len(available_files)}):")
            for i, file_info in enumerate(available_files, 1):
                schema_info = f" [{file_info.get('schema', 'Unknown')}]" if file_info.get('schema') else ""
                print(f"   {i}. {file_info['title']}{schema_info}")
                if file_info.get('description'):
                    print(f"      Description: {file_info['description'][:100]}...")
            
            try:
                selection = int(input(f"\nSelect content file (1-{len(available_files)}): ").strip())
                if 1 <= selection <= len(available_files):
                    selected_file = available_files[selection - 1]
                    
                    # Load ADF content from JSON file
                    adf_content = creator.load_content_from_file(selected_file['adf_file'])
                    if adf_content:
                        # Use the title from metadata or allow user to override
                        default_title = selected_file['title']
                        title = input(f"Enter page title [{default_title}]: ").strip() or default_title
                        
                        # Get labels from user
                        labels = get_labels_from_user(default_labels)
                        
                        # Create page with properties from metadata
                        properties = selected_file.get('metadata', {})
                        result = creator.create_page_with_properties_by_space_id(space_id, title, adf_content, properties, labels=labels)
                        
                        if result:
                            print(f"✅ Page '{title}' created successfully from {selected_file['name']}!")
                            print(f"   ID: {result['id']}")
                            print(f"   URL: {confluence_url}/pages/{result['id']}")
                        else:
                            print("❌ Failed to create page")
                    else:
                        print("❌ Failed to load ADF content from selected file")
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        elif choice == "4":
            # Create child page from confluence_docs ADF content WITH SCHEMA HIERARCHY
            selected_dir = select_confluence_docs_subdirectory()
            if not selected_dir:
                continue
                
            available_files = creator.get_available_content_files(selected_dir)
            if not available_files:
                print(f"❌ No ADF content files found in {selected_dir}")
                continue
            
            # Get parent page ID
            default_parent_id = config.get('default_parent_page_id', '')
            parent_prompt = f"Enter parent page ID{f' [{default_parent_id}]' if default_parent_id else ''}: "
            parent_page_id = input(parent_prompt).strip() or default_parent_id
            
            if not parent_page_id:
                print("❌ Parent page ID is required")
                continue
            
            # Ask user if they want schema hierarchy or single page
            print("\nChoose creation mode:")
            print("1. Create single page directly under parent")
            print("2. Create with schema hierarchy (parent -> schema -> procedure)")
            
            mode_choice = input("Select mode (1-2): ").strip()
            
            if mode_choice == "2":
                # Use schema hierarchy for all files
                print(f"\n📁 Available ADF Content Files ({len(available_files)}):")
                for i, file_info in enumerate(available_files, 1):
                    schema_info = f" [{file_info.get('schema', 'Unknown')}]" if file_info.get('schema') else ""
                    print(f"   {i}. {file_info['title']}{schema_info}")
                    if file_info.get('description'):
                        print(f"      Description: {file_info['description'][:100]}...")
                
                # Get labels from user
                labels = get_labels_from_user(default_labels)
                
                # Create all pages with schema hierarchy
                result = create_child_pages_from_directory_by_ids_with_schema_hierarchy(
                    creator, space_id, parent_page_id, selected_dir, labels
                )
                
                if result and result['success']:
                    print(f"✅ Successfully created pages with schema hierarchy!")
                    print(f"   Created {len(result['created_schema_pages'])} schema pages")
                    print(f"   Created {len(result['created_pages'])} procedure pages")
                    if result['failed_pages']:
                        print(f"   ⚠️  {len(result['failed_pages'])} pages failed to create")
                    
                    # Show created schema pages
                    if result['created_schema_pages']:
                        print(f"\n📄 Created schema pages:")
                        for schema_page in result['created_schema_pages']:
                            print(f"   - {schema_page['title']} (ID: {schema_page['id']})")
                            print(f"     URL: {schema_page['url']}")
                else:
                    print("❌ Failed to create pages with schema hierarchy")
                    if result and result['failed_pages']:
                        print("Failed pages:")
                        for failed in result['failed_pages'][:5]:  # Show first 5
                            print(f"   - {failed['title']}: {failed['error']}")
                
            else:
                # Original single page creation mode
                print(f"\n📁 Available ADF Content Files ({len(available_files)}):")
                for i, file_info in enumerate(available_files, 1):
                    schema_info = f" [{file_info.get('schema', 'Unknown')}]" if file_info.get('schema') else ""
                    print(f"   {i}. {file_info['title']}{schema_info}")
                    if file_info.get('description'):
                        print(f"      Description: {file_info['description'][:100]}...")
                
                try:
                    selection = int(input(f"\nSelect content file (1-{len(available_files)}): ").strip())
                    if 1 <= selection <= len(available_files):
                        selected_file = available_files[selection - 1]
                        
                        # Load ADF content from JSON file
                        adf_content = creator.load_content_from_file(selected_file['adf_file'])
                        if adf_content:
                            # Use the title from metadata or allow user to override
                            default_title = selected_file['title']
                            child_title = input(f"Enter child page title [{default_title}]: ").strip() or default_title
                            
                            # Get labels from user
                            labels = get_labels_from_user(default_labels)
                            
                            # Create child page with properties from metadata
                            properties = selected_file.get('metadata', {})
                            result = creator.create_child_page_with_properties_by_ids(space_id, parent_page_id, child_title, adf_content, properties, labels)
                            
                            if result:
                                print(f"✅ Child page '{child_title}' created successfully from {selected_file['name']}!")
                                print(f"   ID: {result['id']}")
                                print(f"   URL: {confluence_url}/pages/{result['id']}")
                            else:
                                print("❌ Failed to create child page")
                        else:
                            print("❌ Failed to load ADF content from selected file")
                    else:
                        print("❌ Invalid selection")
                except ValueError:
                    print("❌ Please enter a valid number")
        
        elif choice == "5":
            # Create child pages for ALL ADF files in directory
            selected_dir = select_confluence_docs_subdirectory()
            if not selected_dir:
                continue
                
            # Get parent page ID
            default_parent_id = config.get('default_parent_page_id', '')
            parent_prompt = f"Enter parent page ID{f' [{default_parent_id}]' if default_parent_id else ''}: "
            parent_page_id = input(parent_prompt).strip() or default_parent_id
            
            if not parent_page_id:
                print("❌ Parent page ID is required")
                continue
            
            # Ask user if they want schema hierarchy or flat structure
            print("\nChoose creation mode:")
            print("1. Flat structure (all pages directly under parent)")
            print("2. Schema hierarchy (parent -> schema -> procedures)")
            
            mode_choice = input("Select mode (1-2): ").strip()
            
            # Get labels from user
            labels = get_labels_from_user(default_labels)
            
            if mode_choice == "2":
                # Use schema hierarchy
                result = create_child_pages_from_directory_by_ids_with_schema_hierarchy(
                    creator, space_id, parent_page_id, selected_dir, labels
                )
                
                if result and result['success']:
                    print(f"✅ Successfully created pages with schema hierarchy!")
                    print(f"   Created {len(result['created_schema_pages'])} schema pages")
                    print(f"   Created {len(result['created_pages'])} procedure pages")
                    if result['failed_pages']:
                        print(f"   ⚠️  {len(result['failed_pages'])} pages failed to create")
                    
                    # Show created schema pages
                    if result['created_schema_pages']:
                        print(f"\n📄 Created schema pages:")
                        for schema_page in result['created_schema_pages']:
                            print(f"   - {schema_page['title']} (ID: {schema_page['id']})")
                else:
                    print("❌ Failed to create pages with schema hierarchy")
            else:
                # Use flat structure (original behavior)
                result = create_child_pages_from_directory_by_ids(creator, space_id, parent_page_id, selected_dir, labels)
                
                if result and result['success']:
                    print(f"✅ Successfully created {len(result['created_pages'])} child pages!")
                    if result['failed_pages']:
                        print(f"   ⚠️  {len(result['failed_pages'])} pages failed to create")
                    print(f"   Parent URL: {confluence_url}/pages/{parent_page_id}")
                else:
                    print("❌ Failed to create child pages")
        
        elif choice == "6":
            # Browse available content files
            selected_dir = select_confluence_docs_subdirectory()
            if not selected_dir:
                continue
                
            available_files = creator.get_available_content_files(selected_dir)
            if not available_files:
                print(f"❌ No ADF content files found in {selected_dir}")
                continue
            
            print(f"\n📁 Available ADF Content Files in {selected_dir} ({len(available_files)}):")
            for i, file_info in enumerate(available_files, 1):
                schema_info = f" [{file_info.get('schema', 'Unknown')}]" if file_info.get('schema') else ""
                complexity_info = f" (Complexity: {file_info.get('complexity', 'Unknown')})" if file_info.get('complexity') else ""
                print(f"   {i}. {file_info['title']}{schema_info}{complexity_info}")
                print(f"      File: {file_info['relative_path']}")
                if file_info.get('description'):
                    print(f"      Description: {file_info['description']}")
                print()
        
        elif choice == "7":
            # Read a page by ID
            page_id = input("Enter page ID to read: ").strip()
            if page_id:
                content = creator.read_page_content_by_id_with_space_id(space_id, page_id)
                if content:
                    print("\n" + "="*50)
                    print(f"📄 Page Content (ID: {page_id}):")
                    print("="*50)
                    print(content)
                    print("="*50)
                else:
                    print("❌ Failed to read page content")
            else:
                print("❌ Page ID is required")
        
        elif choice == "8":
            # Get page information by ID
            page_id = input("Enter page ID to get info: ").strip()
            if page_id:
                page_info = creator.get_page_info_by_id_with_space_id(space_id, page_id)
                if page_info:
                    print("\n" + "="*50)
                    print(f"📄 Page Information (ID: {page_id}):")
                    print("="*50)
                    print(f"Title: {page_info.get('title', 'N/A')}")
                    print(f"Status: {page_info.get('status', 'N/A')}")
                    print(f"Created: {page_info.get('createdAt', 'N/A')}")
                    print(f"Version: {page_info.get('version', {}).get('number', 'N/A')}")
                    print(f"URL: {confluence_url}/pages/{page_id}")
                    
                    # Show labels if they exist
                    labels = creator.get_page_labels(page_id)
                    if labels:
                        print(f"Labels: {', '.join(labels)}")
                    
                    print("="*50)
                else:
                    print("❌ Failed to get page information")
            else:
                print("❌ Page ID is required")
        
        elif choice == "9":
            # Search pages
            search_query = input("Enter search query: ").strip()
            if search_query:
                results = creator.search_pages_by_space_id(space_id, search_query)
                if results:
                    print(f"\n🔍 Search Results for '{search_query}' ({len(results)} found):")
                    print("="*70)
                    for i, result in enumerate(results, 1):
                        print(f"{i}. {result.get('title', 'No title')}")
                        print(f"   ID: {result.get('id', 'N/A')}")
                        print(f"   URL: {confluence_url}/pages/{result.get('id', '')}")
                        if result.get('excerpt'):
                            print(f"   Excerpt: {result['excerpt'][:100]}...")
                        print()
                else:
                    print("❌ No results found or search failed")
            else:
                print("❌ Search query is required")
        
        elif choice == "10":
            # List child pages by parent ID
            parent_id = input("Enter parent page ID: ").strip()
            if parent_id:
                children = creator.get_child_pages_by_id(parent_id)
                if children:
                    print(f"\n👶 Child Pages of {parent_id} ({len(children)} found):")
                    print("="*70)
                    for i, child in enumerate(children, 1):
                        print(f"{i}. {child.get('title', 'No title')}")
                        print(f"   ID: {child.get('id', 'N/A')}")
                        print(f"   URL: {confluence_url}/pages/{child.get('id', '')}")
                        print()
                else:
                    print("❌ No child pages found")
            else:
                print("❌ Parent page ID is required")
        
        elif choice == "11":
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid option. Please select 1-11.")

def main():
    """Main function to run the script"""
    print("=== Confluence Page Creator ===")
    print("Starting interactive mode...")
    interactive_mode()

if __name__ == "__main__":
    main()
