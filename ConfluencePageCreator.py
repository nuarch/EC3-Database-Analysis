import requests
import json
import base64
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
        print("3. Read a page")
        print("4. Get page information")
        print("5. Search pages")
        print("6. List child pages")
        print("7. Exit")
        
        choice = input("\nSelect an option (1-7): ").strip()
        
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
            page_title = input("Enter page title to read: ").strip()
            content = creator.read_page_content(space_key, page_title)
            if content == "" or content:
                print(f"\n📝 Content of '{page_title}':")
                print("-" * 50)
                print(content)
                print("-" * 50)
            else:
                print(f"❌ Could not read page '{page_title}'")
        
        elif choice == "4":
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
        
        elif choice == "5":
            query = input("Enter search query: ").strip()
            limit = input("Enter number of results (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            
            results = creator.search_pages(space_key, query, limit)
            if results:
                print(f"\n🔍 Search Results ({len(results)}):")
                for i, page in enumerate(results, 1):
                    print(f"   {i}. {page['title']} (ID: {page['id']})")
            else:
                print("❌ No pages found")
        
        elif choice == "6":
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
                
        elif choice == "7":
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-7.")


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
