
import requests
import json
import base64
import os
import glob
from typing import Optional, Dict, Any, List
from ConfluenceConfigManager import ConfluenceConfigManager

class ConfluencePageCreator:
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
    
    def get_available_content_files(self, content_dir: str = "./confluence_docs") -> List[Dict[str, str]]:
        """
        Get available content files from the confluence_docs directory.
        
        Args:
            content_dir: Directory containing pre-generated content files
            
        Returns:
            List of dictionaries containing file information
        """
        available_files = []
        
        if not os.path.exists(content_dir):
            print(f"⚠️  Content directory '{content_dir}' does not exist")
            return available_files
        
        # Look for XML files (Confluence storage format) and their corresponding JSON metadata
        xml_files = glob.glob(os.path.join(content_dir, "**/*.xml"), recursive=True)
        
        for xml_file in xml_files:
            base_name = os.path.splitext(xml_file)[0]
            json_file = f"{base_name}.json"
            
            file_info = {
                'xml_file': xml_file,
                'json_file': json_file if os.path.exists(json_file) else None,
                'name': os.path.basename(base_name),
                'relative_path': os.path.relpath(xml_file, content_dir)
            }
            
            # Try to read metadata if a JSON file exists'
            file_info['title'] = file_info['name']
            if file_info['json_file']:
                try:
                    with open(file_info['json_file'], 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                        file_info['name'] = metadata.get('stored_procedure_name', 'Unknown')
                        file_info['schema'] = metadata.get('schema_name', 'Unknown')
                        file_info['complexity'] = metadata.get('complexity', 'Unknown')
                        file_info['metadata'] = metadata
                except Exception as e:
                    print(f"Warning: Could not read metadata from {file_info['json_file']}: {e}")
                    file_info['metadata'] = {}
            else:
                file_info['metadata'] = {}

            # Add properties so that the page is full width
            file_info['metadata']['content-appearance-draft'] = 'full-width'
            file_info['metadata']['content-appearance-published'] = 'full-width'

            available_files.append(file_info)
        
        return sorted(available_files, key=lambda x: x['title'])
    
    def load_content_from_file(self, file_path: str) -> Optional[str]:
        """
        Load content from a file.
        
        Args:
            file_path: Path to the content file
            
        Returns:
            File content as string, or None if error
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"Error loading content from {file_path}: {e}")
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
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}/property"
        
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
                # Check if property already exists
                get_response = self.session.get(f"{url}/{key}")
                
                if get_response.status_code == 200:
                    # Property exists, update it
                    existing_property = get_response.json()
                    property_data['version'] = {
                        'number': existing_property['version']['number'] + 1
                    }
                    response = self.session.put(f"{url}/{key}", data=json.dumps(property_data))
                else:
                    # Property doesn't exist, create it
                    response = self.session.post(url, data=json.dumps(property_data))
                
                if response.status_code in [200, 201]:
                    success_count += 1
                    print(f"   ✅ Set property '{key}': {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
                else:
                    print(f"   ❌ Failed to set property '{key}': {response.status_code} - {response.text[:100]}")
                    
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Error setting property '{key}': {e}")
        
        return success_count > 0
    
    def get_page_by_title(self, space_key: str, title: str) -> Optional[Dict[str, Any]]:
        """
        Get a page by its title in a specific space.
        
        Args:
            space_key: The space key where the page is located
            title: The title of the page
            
        Returns:
            Page information if found, None otherwise
        """
        url = f"{self.base_url}/wiki/rest/api/content"
        params = {
            'title': title,
            'spaceKey': space_key,
            'expand': 'version,body.storage,space,ancestors'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data['results']:
                return data['results'][0]
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page: {e}")
            return None
    
    def get_page_by_id(self, page_id: str, expand: str = "body.storage,version,space,ancestors") -> Optional[Dict[str, Any]]:
        """
        Get a page by its ID.
        
        Args:
            page_id: The ID of the page
            expand: Comma-separated list of properties to expand
            
        Returns:
            Page information if found, None otherwise
        """
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}"
        params = {'expand': expand}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page by ID {page_id}: {e}")
            return None
    
    def read_page_content(self, space_key: str, title: str) -> Optional[str]:
        """
        Read the content of a page by title.
        
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
    
    def get_page_info(self, space_key: str, title: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a page.
        
        Args:
            space_key: The space key where the page is located
            title: The title of the page
            
        Returns:
            Dictionary with page information if found, None otherwise
        """
        page = self.get_page_by_title(space_key, title)
        if not page:
            return None
        
        info = {
            'id': page['id'],
            'title': page['title'],
            'type': page['type'],
            'status': page['status'],
            'space': {
                'key': page['space']['key'],
                'name': page['space']['name']
            },
            'version': {
                'number': page['version']['number'],
                'when': page['version']['when'],
                'by': page['version']['by']['displayName'] if 'by' in page['version'] else 'Unknown'
            },
            'created': page.get('history', {}).get('createdDate', 'Unknown'),
            'url': f"{self.base_url}/spaces/{page['space']['key']}/pages/{page['id']}",
            'edit_url': f"{self.base_url}/pages/editpage.action?pageId={page['id']}"
        }
        
        # Add parent information if available
        if 'ancestors' in page and page['ancestors']:
            parent = page['ancestors'][-1]  # Get the immediate parent
            info['parent'] = {
                'id': parent['id'],
                'title': parent['title']
            }
        
        return info
    
    def search_pages(self, space_key: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for pages in a space.
        
        Args:
            space_key: The space key to search in
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of page information dictionaries
        """
        url = f"{self.base_url}/wiki/rest/api/content/search"
        params = {
            'cql': f'space = "{space_key}" AND type = "page" AND title ~ "{query}"',
            'limit': limit,
            'expand': 'version,space'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for page in data['results']:
                results.append({
                    'id': page['id'],
                    'title': page['title'],
                    'type': page['type'],
                    'status': page['status'],
                    'url': f"{self.base_url}/spaces/{space_key}/pages/{page['id']}"
                })
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"Error searching pages: {e}")
            return []
    
    def get_child_pages(self, page_id: str) -> List[Dict[str, Any]]:
        """
        Get child pages of a specific page.
        
        Args:
            page_id: The ID of the parent page
            
        Returns:
            List of child page information
        """
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}/child/page"
        params = {'expand': 'version,space'}
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for page in data['results']:
                results.append({
                    'id': page['id'],
                    'title': page['title'],
                    'type': page['type'],
                    'status': page['status'],
                    'url': f"{self.base_url}/spaces/{page['space']['key']}/pages/{page['id']}"
                })
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching child pages: {e}")
            return []
    
    def create_page(self, space_key: str, title: str, content: str, 
                   parent_page_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new page in Confluence.
        
        Args:
            space_key: The space key where the page will be created
            title: The title of the new page
            content: The HTML content of the page
            parent_page_id: Optional parent page ID to create the page under
            
        Returns:
            Created page information if successful, None otherwise
        """
        url = f"{self.base_url}/wiki/rest/api/content"
        
        page_data = {
            'type': 'page',
            'title': title,
            'space': {
                'key': space_key
            },
            'body': {
                'storage': {
                    'value': content,
                    'representation': 'storage'
                }
            }
        }
        
        # Add parent page if specified
        if parent_page_id:
            page_data['ancestors'] = [{'id': parent_page_id}]
        
        try:
            response = self.session.post(url, data=json.dumps(page_data))
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error creating page: {e}")
            if response.status_code == 400:
                print("Response content:", response.text)
            return None
    
    def create_page_with_properties(self, space_key: str, title: str, content: str, 
                                  properties: Dict[str, Any], parent_page_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Create a new page in Confluence and set its properties.
        
        Args:
            space_key: The space key where the page will be created
            title: The title of the new page
            content: The HTML content of the page
            properties: Dictionary of properties to set for the page
            parent_page_id: Optional parent page ID to create the page under
            
        Returns:
            Created page information if successful, None otherwise
        """
        # First create the page
        result = self.create_page(space_key, title, content, parent_page_id)
        
        if result and properties:
            page_id = result['id']
            print(f"📝 Setting page properties for '{title}' (ID: {page_id})...")
            
            # Set the properties
            if self.set_page_properties(page_id, properties):
                print(f"✅ Properties set successfully for page '{title}'")
            else:
                print(f"⚠️  Page created but some properties may not have been set")
        
        return result
    
    def create_child_page(self, space_key: str, parent_title: str, 
                         child_title: str, child_content: str) -> Optional[Dict[str, Any]]:
        """
        Create a child page under a specified parent page.
        
        Args:
            space_key: The space key
            parent_title: Title of the parent page
            child_title: Title of the new child page
            child_content: HTML content of the child page
            
        Returns:
            Created page information if successful, None otherwise
        """
        # First, find the parent page
        parent_page = self.get_page_by_title(space_key, parent_title)
        if not parent_page:
            print(f"Parent page '{parent_title}' not found in space '{space_key}'")
            return None
        
        parent_id = parent_page['id']
        print(f"Found parent page: {parent_title} (ID: {parent_id})")
        
        # Create the child page
        return self.create_page(space_key, child_title, child_content, parent_id)
    
    def create_child_page_with_properties(self, space_key: str, parent_title: str, 
                                        child_title: str, child_content: str, 
                                        properties: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a child page under a specified parent page and set its properties.
        
        Args:
            space_key: The space key
            parent_title: Title of the parent page
            child_title: Title of the new child page
            child_content: HTML content of the child page
            properties: Dictionary of properties to set for the page
            
        Returns:
            Created page information if successful, None otherwise
        """
        # First, find the parent page
        parent_page = self.get_page_by_title(space_key, parent_title)
        if not parent_page:
            print(f"Parent page '{parent_title}' not found in space '{space_key}'")
            return None
        
        parent_id = parent_page['id']
        print(f"Found parent page: {parent_title} (ID: {parent_id})")
        
        # Create the child page with properties
        return self.create_page_with_properties(space_key, child_title, child_content, properties, parent_id)


def main():
    """
    Main function to demonstrate the Confluence page creator.
    """
    # Load configuration
    config = ConfluenceConfigManager()
    
    # Check if configuration is complete
    if not config.is_complete():
        print("❌ Configuration is incomplete!")
        print("Please ensure you have either:")
        print("1. A confluence_config.json file with all required fields")
        print("2. Environment variables: CONFLUENCE_URL, CONFLUENCE_USERNAME, CONFLUENCE_API_TOKEN")
        
        # Offer to create sample config
        create_sample = input("Would you like to create a sample config file? (y/n): ").strip().lower()
        if create_sample == 'y':
            config.create_sample_config()
        return
    
    # Initialize the page creator from config
    creator = ConfluencePageCreator.from_config(config)
    
    # Get configuration values
    space_key = config.get('space_key')
    if not space_key:
        space_key = input("Enter the space key: ").strip()
    
    # Example: Read a page
    print("=== Reading Page Example ===")
    page_title = input("Enter page title to read (or press Enter to skip): ").strip()
    
    if page_title:
        # Get page information
        page_info = creator.get_page_info(space_key, page_title)
        if page_info:
            print(f"\n📄 Page Information:")
            print(f"   Title: {page_info['title']}")
            print(f"   ID: {page_info['id']}")
            print(f"   Version: {page_info['version']['number']}")
            print(f"   Last Modified: {page_info['version']['when']}")
            print(f"   Modified By: {page_info['version']['by']}")
            print(f"   URL: {page_info['url']}")
            
            if 'parent' in page_info:
                print(f"   Parent: {page_info['parent']['title']}")
            
            # Read page content
            content = creator.read_page_content(space_key, page_title)
            if content:
                print(f"\n📝 Page Content (first 200 characters):")
                print(f"   {content[:200]}{'...' if len(content) > 200 else ''}")
                
                # Show child pages if any
                child_pages = creator.get_child_pages(page_info['id'])
                if child_pages:
                    print(f"\n👶 Child Pages ({len(child_pages)}):")
                    for child in child_pages:
                        print(f"   - {child['title']} (ID: {child['id']})")
        else:
            print(f"❌ Page '{page_title}' not found in space '{space_key}'")
    
    # Example: Create a child page
    print("\n=== Creating Page Example ===")
    create_page = input("Would you like to create a new page? (y/n): ").strip().lower()
    
    if create_page == 'y':
        parent_title = config.get('default_parent_title', "Parent Page Name")
        child_title = "New Child Page"
        child_content = """
        <h1>Welcome to the New Child Page</h1>
        <p>This page was created automatically using the Python Confluence API.</p>
        <h2>Features</h2>
        <ul>
            <li>Automatic page creation</li>
            <li>Parent-child relationships</li>
            <li>HTML content support</li>
            <li>Configuration file support</li>
            <li>Page reading capabilities</li>
        </ul>
        <p>Created on: <time datetime="2025-01-01">January 1, 2025</time></p>
        """
        
        print(f"Creating child page '{child_title}' under '{parent_title}'...")
        
        result = creator.create_child_page(space_key, parent_title, child_title, child_content)
        
        if result:
            print(f"✅ Page created successfully!")
            print(f"   Title: {result['title']}")
            print(f"   ID: {result['id']}")
            print(f"   URL: {config.get('confluence_url')}/spaces/{space_key}/pages/{result['id']}")
        else:
            print("❌ Failed to create page")


def interactive_mode():
    """
    Interactive mode for creating and reading pages with user input.
    Enhanced to support content selection from ./confluence_docs directory.
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
    
    space_key = config.get('space_key')
    if not space_key:
        space_key = input("Enter the space key: ").strip()
    
    creator = ConfluencePageCreator(confluence_url, username, api_token)
    
    while True:
        print("\n" + "="*50)
        print("1. Create a standalone page")
        print("2. Create a child page under existing page")
        print("3. Create page from confluence_docs content")
        print("4. Create child page from confluence_docs content")
        print("5. Browse available content files")
        print("6. Read a page")
        print("7. Get page information")
        print("8. Search pages")
        print("9. List child pages")
        print("10. Exit")
        
        choice = input("\nSelect an option (1-10): ").strip()
        
        if choice == "1":
            title = input("Enter page title: ").strip()
            content = input("Enter page content (HTML): ").strip()
            
            result = creator.create_page(space_key, title, content)
            if result:
                print(f"✅ Page '{title}' created successfully!")
                print(f"   URL: {confluence_url}/spaces/{space_key}/pages/{result['id']}")
            else:
                print("❌ Failed to create page")
                
        elif choice == "2":
            default_parent = config.get('default_parent_title', '')
            parent_prompt = f"Enter parent page title{f' [{default_parent}]' if default_parent else ''}: "
            parent_title = input(parent_prompt).strip() or default_parent
            
            child_title = input("Enter child page title: ").strip()
            child_content = input("Enter child page content (HTML): ").strip()
            
            result = creator.create_child_page(space_key, parent_title, child_title, child_content)
            if result:
                print(f"✅ Child page '{child_title}' created successfully!")
                print(f"   URL: {confluence_url}/spaces/{space_key}/pages/{result['id']}")
            else:
                print("❌ Failed to create child page")
        
        elif choice == "3":
            # Create standalone page from confluence_docs content
            available_files = creator.get_available_content_files()
            if not available_files:
                print("❌ No content files found in ./confluence_docs directory")
                continue
            
            print(f"\n📁 Available Content Files ({len(available_files)}):")
            for i, file_info in enumerate(available_files, 1):
                schema_info = f" [{file_info.get('schema', 'Unknown')}]" if file_info.get('schema') else ""
                print(f"   {i}. {file_info['title']}{schema_info}")
                if file_info.get('description'):
                    print(f"      Description: {file_info['description'][:100]}...")
            
            try:
                selection = int(input(f"\nSelect content file (1-{len(available_files)}): ").strip())
                if 1 <= selection <= len(available_files):
                    selected_file = available_files[selection - 1]
                    
                    # Load content from XML file
                    content = creator.load_content_from_file(selected_file['xml_file'])
                    if content:
                        # Use the title from metadata or allow user to override
                        default_title = selected_file['title']
                        title = input(f"Enter page title [{default_title}]: ").strip() or default_title
                        
                        # Create page with properties from JSON metadata
                        properties = selected_file.get('metadata', {})
                        result = creator.create_page_with_properties(space_key, title, content, properties)
                        
                        if result:
                            print(f"✅ Page '{title}' created successfully from {selected_file['name']}!")
                            print(f"   URL: {confluence_url}/spaces/{space_key}/pages/{result['id']}")
                        else:
                            print("❌ Failed to create page")
                    else:
                        print("❌ Failed to load content from selected file")
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        elif choice == "4":
            # Create child page from confluence_docs content
            available_files = creator.get_available_content_files()
            if not available_files:
                print("❌ No content files found in ./confluence_docs directory")
                continue
            
            # Get parent page first
            default_parent = config.get('default_parent_title', '')
            parent_prompt = f"Enter parent page title{f' [{default_parent}]' if default_parent else ''}: "
            parent_title = input(parent_prompt).strip() or default_parent
            
            if not parent_title:
                print("❌ Parent page title is required")
                continue
            
            print(f"\n📁 Available Content Files ({len(available_files)}):")
            for i, file_info in enumerate(available_files, 1):
                schema_info = f" [{file_info.get('schema', 'Unknown')}]" if file_info.get('schema') else ""
                print(f"   {i}. {file_info['title']}{schema_info}")
                if file_info.get('description'):
                    print(f"      Description: {file_info['description'][:100]}...")
            
            try:
                selection = int(input(f"\nSelect content file (1-{len(available_files)}): ").strip())
                if 1 <= selection <= len(available_files):
                    selected_file = available_files[selection - 1]
                    
                    # Load content from XML file
                    content = creator.load_content_from_file(selected_file['xml_file'])
                    if content:
                        # Use the title from metadata or allow user to override
                        default_title = selected_file['title']
                        child_title = input(f"Enter child page title [{default_title}]: ").strip() or default_title
                        
                        # Create child page with properties from JSON metadata
                        properties = selected_file.get('metadata', {})
                        result = creator.create_child_page_with_properties(space_key, parent_title, child_title, content, properties)
                        
                        if result:
                            print(f"✅ Child page '{child_title}' created successfully from {selected_file['name']}!")
                            print(f"   URL: {confluence_url}/spaces/{space_key}/pages/{result['id']}")
                        else:
                            print("❌ Failed to create child page")
                    else:
                        print("❌ Failed to load content from selected file")
                else:
                    print("❌ Invalid selection")
            except ValueError:
                print("❌ Please enter a valid number")
        
        elif choice == "5":
            # Browse available content files
            available_files = creator.get_available_content_files()
            if not available_files:
                print("❌ No content files found in ./confluence_docs directory")
                continue
            
            print(f"\n📁 Available Content Files ({len(available_files)}):")
            print("-" * 80)
            
            # Group by schema if available
            by_schema = {}
            for file_info in available_files:
                schema = file_info.get('schema', 'Unknown')
                if schema not in by_schema:
                    by_schema[schema] = []
                by_schema[schema].append(file_info)
            
            for schema, files in sorted(by_schema.items()):
                print(f"\n📊 Schema: {schema} ({len(files)} files)")
                for file_info in files:
                    print(f"   • {file_info['title']}")
                    print(f"     XML: {file_info['relative_path']}")
                    if file_info.get('json_file'):
                        json_relative = os.path.relpath(file_info['json_file'], './confluence_docs')
                        print(f"     JSON: {json_relative}")
                    if file_info.get('description'):
                        print(f"     Description: {file_info['description'][:100]}...")
                    if file_info.get('type'):
                        print(f"     Type: {file_info['type']}")
                    
                    # Show some properties from metadata
                    metadata = file_info.get('metadata', {})
                    if metadata:
                        properties_preview = []
                        for key, value in list(metadata.items())[:3]:  # Show first 3 properties
                            if key not in ['title', 'description', 'schema', 'type']:
                                properties_preview.append(f"{key}: {str(value)[:30]}{'...' if len(str(value)) > 30 else ''}")
                        if properties_preview:
                            print(f"     Properties: {', '.join(properties_preview)}")

        elif choice == "6":
            page_title = input("Enter page title to read: ").strip()
            content = creator.read_page_content(space_key, page_title)
            if content == "" or content:
                print(f"\n📝 Content of '{page_title}':")
                print("-" * 50)
                print(content)
                print("-" * 50)
            else:
                print(f"❌ Could not read page '{page_title}'")

        elif choice == "7":
            page_title = input("Enter page title: ").strip()
            info = creator.get_page_info(space_key, page_title)
            if info:
                print(f"\n📄 Page Information:")
                print(f"   Title: {info['title']}")
                print(f"   ID: {info['id']}")
                print(f"   Status: {info['status']}")
                print(f"   Version: {info['version']['number']}")
                print(f"   Last Modified: {info['version']['when']}")
                print(f"   Modified By: {info['version']['by']}")
                print(f"   URL: {info['url']}")
                if 'parent' in info:
                    print(f"   Parent: {info['parent']['title']}")
            else:
                print(f"❌ Page '{page_title}' not found")

        elif choice == "8":
            query = input("Enter search query: ").strip()
            limit = input("Enter number of results (default 1   0): ").strip()
            limit = int(limit) if limit.isdigit() else 10

            results = creator.search_pages(space_key, query, limit)
            if results:
                print(f"\n🔍 Search Results ({len(results)}):")
                for i, page in enumerate(results, 1):
                    print(f"   {i}. {page['title']} (ID: {page['id']})")
            else:
                print("❌ No pages found")

        elif choice == "9":
            page_title = input("Enter parent page title: ").strip()
            parent_page = creator.get_page_by_title(space_key, page_title)
            if parent_page:
                children = creator.get_child_pages(parent_page['id'])
                if children:
                    print(f"\n👶 Child Pages of '{page_title}' ({len(children)}):")
                    for i, child in enumerate(children, 1):
                        print(f"   {i}. {child['title']} (ID: {child['id']})")
                else:
                    print(f"ℹ️  No child pages found for '{page_title}'")
            else:
                print(f"❌ Parent page '{page_title}' not found")

        elif choice == "10":
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-10.")


if __name__ == "__main__":
    # You can choose to run in interactive mode or modify the main() function
    print("Choose mode:")
    print("1. Interactive mode")
    print("2. Script mode (uses configuration file)")

    mode = input("Select mode (1 or 2): ").strip()

    if mode == "1":
        interactive_mode()
    else:
        main()

