#!/usr/bin/env python3
"""
Confluence Page Updated
Connects to Confluence via REST v2 API and updates pages based on Excel data
"""

import pandas as pd
import os
from typing import Dict, Any, Optional
from ConfluencePageCreator import ConfluencePageCreator
from ConfluenceConfigManager import ConfluenceConfigManager


class ConfluencePageUpdater:
    """Find Confluence pages based on schema.table format"""
    
    def __init__(self, parent_page_id: str):
        """
        Initialize the page finder.
        
        Args:
            parent_page_id: The ID of the parent page to search under
        """
        self.config_manager = ConfluenceConfigManager()
        self.confluence = ConfluencePageCreator.from_config(self.config_manager)
        self.parent_page_id = parent_page_id
        
    def find_page_by_schema_table(self, schema_name: str, table_name: str) -> Optional[str]:
        """
        Find a Confluence page based on schema and table name.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            Page ID if found, None otherwise
        """
        try:
            # Get child pages of the parent page to find the schema page
            parent_children = self.confluence.get_child_pages(self.parent_page_id)
            
            # Find the schema page
            schema_page_id = None
            for child in parent_children:
                if child.get('title', '').strip().lower() == schema_name.strip().lower():
                    schema_page_id = child.get('id')
                    break
            
            if not schema_page_id:
                return None
            
            # Get child pages of the schema page to find the table page
            schema_children = self.confluence.get_child_pages(schema_page_id)
            
            # Find the table page
            for child in schema_children:
                if child.get('title', '').strip().lower() == table_name.strip().lower():
                    return child.get('id')
            
            return None
            
        except Exception as e:
            print(f"Error searching for {schema_name}.{table_name}: {e}")
            return None
    
    def process_excel_file(self, excel_file_path: str, parent_page_id: str):
        """
        Process the Excel file and find corresponding Confluence pages.
        
        Args:
            excel_file_path: Path to the Excel file
            parent_page_id: ID of the parent page to search under
        """
        try:
            # Read the Excel file
            df = pd.read_excel(excel_file_path)
            
            # Check if 'Table Name' column exists
            if 'Table Name' not in df.columns:
                print("Error: 'Table Name' column not found in Excel file")
                return
            
            print(f"Processing {len(df)} table entries from Excel file...")
            print("=" * 60)
            
            # Process each table name
            for index, row in df.iterrows():
                table_full_name = str(row['Table Name']).strip()
                
                if pd.isna(row['Table Name']) or not table_full_name:
                    continue
                
                # Split schema.table format
                if '.' in table_full_name:
                    schema_name, table_name = table_full_name.split('.', 1)
                    schema_name = schema_name.strip()
                    table_name = table_name.strip()
                    
                    # Find the page
                    page_id = self.find_page_by_schema_table(schema_name, table_name)
                    
                    # Output result
                    result_page_id = page_id if page_id else "Not Found"
                    print(f"{table_full_name} - {result_page_id}")
                else:
                    print(f"{table_full_name} - Not Found (Invalid format)")
            
        except FileNotFoundError:
            print(f"Error: Excel file not found at {excel_file_path}")
        except Exception as e:
            print(f"Error processing Excel file: {e}")


def main():
    """Main function to run the Confluence page finder"""
    
    # Configuration
    excel_file_path = "assets/EC3 - Data warehouse data dictionary.xlsx"
    
    # Get parent page ID from user input
    parent_page_id = input("Enter the parent page ID to search under: ").strip()
    
    if not parent_page_id:
        print("Error: Parent page ID is required")
        return
    
    # Check if Excel file exists
    if not os.path.exists(excel_file_path):
        print(f"Error: Excel file not found at {excel_file_path}")
        return
    
    try:
        # Initialize the page finder
        finder = ConfluencePageUpdater(parent_page_id)
        
        # Check if configuration is complete
        if not finder.config_manager.is_complete():
            print("Error: Confluence configuration is incomplete. Please check your config file or environment variables.")
            return
        
        print(f"Connecting to Confluence at: {finder.config_manager.get('confluence_url')}")
        print(f"Reading Excel file: {excel_file_path}")
        print(f"Searching under parent page ID: {parent_page_id}")
        print()
        
        # Process the Excel file
        finder.process_excel_file(excel_file_path, parent_page_id)
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
