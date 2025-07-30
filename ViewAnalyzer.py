"""
View Analyzer for ChatGPT Integration
This module connects to the database, retrieves views and their structure,
and sends them to OpenAI's ChatGPT API for analysis and documentation.
"""

import requests
import json
import logging
from typing import List, Dict, Any, Optional
from DatabaseConnectionUtility import DatabaseManager
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_chatgpt_config() -> Dict[str, Any]:
    """Load ChatGPT configuration from external file or environment variables."""
    try:
        # Try to import from a config file first
        from chatgpt_config import CHATGPT_CONFIG
        return CHATGPT_CONFIG
    except ImportError:
        # Fallback to environment variables
        logger.warning("chatgpt_config.py not found, using environment variables")
        return {
            'api_key': os.getenv('OPENAI_API_KEY', ''),
            'base_url': os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'model': os.getenv('OPENAI_MODEL', 'gpt-4'),
            'timeout': int(os.getenv('OPENAI_TIMEOUT', '60')),
            'max_retries': int(os.getenv('OPENAI_MAX_RETRIES', '3')),
            'max_tokens': int(os.getenv('OPENAI_MAX_TOKENS', '2000')),
            'temperature': float(os.getenv('OPENAI_TEMPERATURE', '0.1'))
        }

def get_available_schemas(db_manager: DatabaseManager) -> List[str]:
    """Get list of available non-empty schemas."""
    return db_manager.get_non_empty_schemas()

def select_schemas_interactive(available_schemas: List[str]) -> List[str]:
    """Interactive schema selection."""
    print("\n" + "="*80)
    print("DATABASE VIEW ANALYSIS TOOL")
    print("="*80)
    
    if not available_schemas:
        print("No schemas found in the database.")
        return []
    
    print(f"\nAvailable non-empty schemas ({len(available_schemas)}):")
    for i, schema in enumerate(available_schemas, 1):
        print(f"  {i}. {schema}")
    
    print("\nOptions:")
    print("  A. All schemas")
    print("  S. Select specific schemas")
    print("  Q. Quit")
    
    while True:
        choice = input("\nChoose an option (A/S/Q): ").strip().upper()
        
        if choice == 'Q':
            print("Exiting...")
            return []
        
        elif choice == 'A':
            print(f"Selected all {len(available_schemas)} schemas")
            return available_schemas
        
        elif choice == 'S':
            selected_schemas = []
            print("\nSelect schemas by number (comma-separated, e.g., 1,3,5):")
            
            while True:
                try:
                    selection = input("Schema numbers: ").strip()
                    if not selection:
                        break
                    
                    numbers = [int(x.strip()) for x in selection.split(',')]
                    
                    for num in numbers:
                        if 1 <= num <= len(available_schemas):
                            schema = available_schemas[num - 1]
                            if schema not in selected_schemas:
                                selected_schemas.append(schema)
                        else:
                            print(f"Invalid schema number: {num}")
                            continue
                    
                    if selected_schemas:
                        print(f"Selected schemas: {', '.join(selected_schemas)}")
                        return selected_schemas
                    else:
                        print("No valid schemas selected.")
                        continue
                        
                except ValueError:
                    print("Invalid input. Please enter numbers separated by commas.")
                    continue
        
        else:
            print("Invalid choice. Please select A, S, or Q.")

class ViewAnalyzer:
    """Class to analyze database views using ChatGPT API."""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """Initialize the analyzer with an optional API key and model."""
        self.db_manager = DatabaseManager()
        
        # Load configuration from an external file
        config = load_chatgpt_config()
        
        self.api_key = api_key or config.get('api_key', '')
        self.base_url = config.get('base_url', 'https://api.openai.com/v1')
        self.model = model or config.get('model', 'gpt-4o')
        self.timeout = config.get('timeout', 60)
        self.max_retries = config.get('max_retries', 3)
        self.max_tokens = config.get('max_tokens', 2000)
        self.temperature = config.get('temperature', 0.1)
        
        self.session = requests.Session()
        
        if self.api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            })
            logger.info("ChatGPT API key loaded successfully")
        else:
            logger.warning("No ChatGPT API key found - will run in simulation mode")
    
    def get_all_views(self, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Retrieve all views from the database, filtering by non-empty schemas."""
        
        # Get list of valid non-empty schemas
        valid_schemas = self.db_manager.get_non_empty_schemas()
        
        if not valid_schemas:
            logger.warning("No non-empty schemas found in the database")
            return []
        
        # If a specific schema is requested, check if it's in the valid schemas list
        if schema_name and schema_name not in valid_schemas:
            logger.warning(f"Schema '{schema_name}' is not in the list of non-empty schemas: {valid_schemas}")
            return []
        
        # Build the query to get view information
        if schema_name:
            # Single schema query
            query = """
            SELECT 
                s.name AS VIEW_SCHEMA,
                v.name AS VIEW_NAME,
                v.create_date AS CREATED,
                v.modify_date AS LAST_ALTERED,
                CASE 
                    WHEN v.with_check_option = 1 THEN 'WITH CHECK OPTION'
                    ELSE 'NO CHECK OPTION'
                END AS CHECK_OPTION,
                ep.value AS VIEW_COMMENT,
                m.definition AS VIEW_DEFINITION,
                v.is_replicated,
                v.is_published
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON v.object_id = ep.major_id 
                AND ep.minor_id = 0 
                AND ep.name = 'MS_Description'
            LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
            WHERE s.name = ?
            AND v.is_ms_shipped = 0
            ORDER BY s.name, v.name
            """
            query_params = (schema_name,)
        else:
            # Multiple schemas query - get views from all non-empty schemas
            placeholders = ','.join(['?'] * len(valid_schemas))
            query = f"""
            SELECT 
                s.name AS VIEW_SCHEMA,
                v.name AS VIEW_NAME,
                v.create_date AS CREATED,
                v.modify_date AS LAST_ALTERED,
                CASE 
                    WHEN v.with_check_option = 1 THEN 'WITH CHECK OPTION'
                    ELSE 'NO CHECK OPTION'
                END AS CHECK_OPTION,
                ep.value AS VIEW_COMMENT,
                m.definition AS VIEW_DEFINITION,
                v.is_replicated,
                v.is_published
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON v.object_id = ep.major_id 
                AND ep.minor_id = 0 
                AND ep.name = 'MS_Description'
            LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
            WHERE s.name IN ({placeholders})
            AND v.is_ms_shipped = 0
            ORDER BY s.name, v.name
            """
            query_params = tuple(valid_schemas)
        
        try:
            rows = self.db_manager.execute_query(query, query_params)
            views = []
            
            for row in rows:
                view = {
                    'schema': row[0],
                    'name': row[1],
                    'created': row[2],
                    'last_altered': row[3],
                    'check_option': row[4],
                    'comment': row[5],
                    'definition': row[6],
                    'is_replicated': row[7],
                    'is_published': row[8]
                }
                views.append(view)
            
            if schema_name:
                logger.info(f"Retrieved {len(views)} views from schema '{schema_name}'")
            else:
                logger.info(f"Retrieved {len(views)} views from {len(valid_schemas)} non-empty schemas")
            
            return views
            
        except Exception as e:
            logger.error(f"Error retrieving views: {e}")
            return []

    def get_views_from_multiple_schemas(self, schemas: List[str]) -> List[Dict[str, Any]]:
        """Retrieve all views from multiple schemas."""
        all_views = []
        
        for schema in schemas:
            views = self.get_all_views(schema)
            all_views.extend(views)
        
        return all_views
    
    def get_view_columns(self, view_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Get detailed column information for a specific view."""
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.ORDINAL_POSITION,
            ep.value AS COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.extended_properties ep ON ep.major_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
            AND ep.minor_id = c.ORDINAL_POSITION
            AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        try:
            rows = self.db_manager.execute_query(query, (schema_name, view_name))
            columns = []
            
            for row in rows:
                column = {
                    'name': row[0],
                    'data_type': row[1],
                    'is_nullable': row[2],
                    'column_default': row[3],
                    'max_length': row[4],
                    'precision': row[5],
                    'scale': row[6],
                    'ordinal_position': row[7],
                    'comment': row[8]
                }
                columns.append(column)
            
            return columns
            
        except Exception as e:
            logger.error(f"Error retrieving columns for view {view_name}: {e}")
            return []
    
    def get_view_dependencies(self, view_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Get dependency information for a specific view."""
        query = """
        SELECT DISTINCT
            SCHEMA_NAME(ref_obj.schema_id) AS REFERENCED_SCHEMA,
            ref_obj.name AS REFERENCED_OBJECT,
            ref_obj.type_desc AS REFERENCED_TYPE
        FROM sys.sql_expression_dependencies dep
        INNER JOIN sys.objects obj ON dep.referencing_id = obj.object_id
        INNER JOIN sys.schemas sch ON obj.schema_id = sch.schema_id
        INNER JOIN sys.objects ref_obj ON dep.referenced_id = ref_obj.object_id
        WHERE sch.name = ? 
        AND obj.name = ?
        AND obj.type = 'V'
        ORDER BY SCHEMA_NAME(ref_obj.schema_id), ref_obj.name
        """
        
        try:
            rows = self.db_manager.execute_query(query, (schema_name, view_name))
            dependencies = []
            
            for row in rows:
                dependency = {
                    'referenced_schema': row[0],
                    'referenced_object': row[1],
                    'referenced_type': row[2]
                }
                dependencies.append(dependency)
            
            return dependencies
            
        except Exception as e:
            logger.error(f"Error retrieving dependencies for view {view_name}: {e}")
            return []
    
    def send_to_chatgpt_api(self, view_info: Dict[str, Any], columns: List[Dict[str, Any]], dependencies: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Send view structure to ChatGPT API for analysis."""
        
        view_name = view_info['name']
        schema_name = view_info['schema']
        
        # Format view structure for analysis
        view_structure = f"View: {schema_name}.{view_name}\n"
        view_structure += f"Check Option: {view_info['check_option']}\n"
        if view_info.get('comment'):
            view_structure += f"Description: {view_info['comment']}\n"
        view_structure += f"Created: {view_info['created']}\n"
        view_structure += f"Last Modified: {view_info['last_altered']}\n"
        if view_info['is_replicated']:
            view_structure += "Replication: YES\n"
        if view_info['is_published']:
            view_structure += "Published: YES\n"
        view_structure += "\n"
        
        # Add columns information
        view_structure += "Columns:\n"
        for col in columns:
            col_info = f"  - {col['name']} ({col['data_type']}"
            if col['max_length']:
                col_info += f"({col['max_length']})"
            elif col['precision'] and col['scale']:
                col_info += f"({col['precision']},{col['scale']})"
            col_info += f", {'NOT NULL' if col['is_nullable'] == 'NO' else 'NULL'}"
            if col['column_default']:
                col_info += f", DEFAULT: {col['column_default']}"
            col_info += ")"
            if col.get('comment'):
                col_info += f" -- {col['comment']}"
            view_structure += col_info + "\n"
        
        # Add dependencies information
        if dependencies:
            view_structure += "\nDependencies (Referenced Objects):\n"
            for dep in dependencies:
                dep_info = f"  - {dep['referenced_schema']}.{dep['referenced_object']} ({dep['referenced_type']})"
                view_structure += dep_info + "\n"
        
        # Add view definition if available
        if view_info.get('definition'):
            view_structure += f"\nView Definition:\n{view_info['definition']}\n"
        
        # Create a comprehensive prompt for ChatGPT
        prompt = f"""
Please analyze the following Microsoft SQL Server database view and provide a detailed analysis:

{view_structure}

Please provide:
1. A clear explanation of what this view represents and its purpose
2. Analysis of its complexity level (Low/Medium/High) based on structure, dependencies, and SQL logic
3. Data model analysis including the underlying tables/views it depends on
4. Business context and likely use cases
5. Performance considerations based on the view definition and dependencies
6. Security and access control considerations
7. Potential issues or improvement recommendations
8. Do not include assumptions or phrases like "likely" unless clearly marked as such

Format your response as a structured analysis that is easy to read and understand. Format your response as follows:

#### 1. Overview
#### 2. Complexity Level: (Low/Medium/High)
#### 3. Data Model Analysis
#### 4. Business Context and Use Cases
#### 5. Performance Considerations
#### 6. Security and Access Control
#### 7. Potential Issues or Recommendations

"""
        
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert database analyst and data architect. Analyze database view structures and provide detailed, technical explanations that would be helpful for database administrators, developers, and data architects."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": self.max_tokens,
            "temperature": self.temperature
        }
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Extract the explanation from ChatGPT response
                    explanation_text = result['choices'][0]['message']['content']
                    
                    # Log message if explanation contains "Incomplete" or similar warnings
                    if "incomplete" in explanation_text.lower():
                        logger.warning(f"ChatGPT response for view '{view_name}' may contain incomplete analysis")
                    
                    # Parse the response to extract structured information
                    analysis_result = self._parse_chatgpt_response(
                        explanation_text, 
                        view_name,
                        result
                    )
                    
                    logger.info(f"Successfully got analysis for view: {view_name}")
                    return analysis_result
                else:
                    logger.error(f"ChatGPT API request failed with status {response.status_code}: {response.text}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"ChatGPT API request error for view {view_name} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return None
        
        return None
    
    def _parse_chatgpt_response(self, explanation_text: str, view_name: str, api_response: Dict) -> Dict[str, Any]:
        """Parse ChatGPT response to extract structured information."""
        
        # Extract complexity if mentioned
        complexity = "Medium"  # Default
        explanation_upper = explanation_text.upper()
        if "COMPLEXITY LEVEL: LOW" in explanation_upper:
            complexity = "Low"
        elif "COMPLEXITY LEVEL: HIGH" in explanation_upper:
            complexity = "High"
        
        return {
            "view_name": view_name,
            "explanation": explanation_text,
            "complexity": complexity,
            "model_used": api_response.get('model', self.model),
            "tokens_used": api_response.get('usage', {}).get('total_tokens', 0),
            "api_response_id": api_response.get('id', '')
        }

    def analyze_all_views(self, schema_name: str = 'dbo', output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all views in a schema."""
        views = self.get_all_views(schema_name)
        results = []
        
        if not views:
            logger.warning(f"No views found in schema '{schema_name}'")
            return results
        
        logger.info(f"Starting analysis of {len(views)} views...")
        
        for i, view in enumerate(views, 1):
            logger.info(f"Analyzing view {i}/{len(views)}: {view['name']}")
            
            # Get view columns
            columns = self.get_view_columns(view['name'], view['schema'])
            
            # Get view dependencies
            dependencies = self.get_view_dependencies(view['name'], view['schema'])
            
            # Send to ChatGPT for analysis
            analysis = self.send_to_chatgpt_api(view, columns, dependencies)
            
            analysis_result = {
                'view_info': view,
                'columns': columns,
                'dependencies': dependencies,
                'analysis': analysis,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            results.append(analysis_result)
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Save results to the file if specified
        if output_file:
            self.save_results_to_file(results, output_file)
        
        logger.info(f"Analysis completed for {len(results)} views")
        return results

    def analyze_views_from_schemas(self, schemas: List[str], output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all views from multiple schemas."""
        all_views = self.get_views_from_multiple_schemas(schemas)
        results = []
        
        if not all_views:
            logger.warning("No views found in the selected schemas")
            return results
        
        # Group views by schema for better logging
        schema_counts = {}
        for view in all_views:
            schema = view['schema']
            schema_counts[schema] = schema_counts.get(schema, 0) + 1
        
        logger.info(f"Starting analysis of {len(all_views)} views from {len(schema_counts)} schemas:")
        for schema, count in schema_counts.items():
            logger.info(f"  {schema}: {count} views")
        
        for i, view in enumerate(all_views, 1):
            logger.info(f"Analyzing view {i}/{len(all_views)}: {view['schema']}.{view['name']}")
            
            # Get view columns
            columns = self.get_view_columns(view['name'], view['schema'])
            
            # Get view dependencies
            dependencies = self.get_view_dependencies(view['name'], view['schema'])
            
            # Send to ChatGPT for analysis
            analysis = self.send_to_chatgpt_api(view, columns, dependencies)
            
            analysis_result = {
                'view_info': view,
                'columns': columns,
                'dependencies': dependencies,
                'analysis': analysis,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            results.append(analysis_result)
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Save results to the file if specified
        if output_file:
            self.save_results_to_file(results, output_file)
        
        logger.info(f"Analysis completed for {len(results)} views from selected schemas")
        return results
    
    def save_results_to_file(self, results: List[Dict[str, Any]], filename: str):
        """Save analysis results to a JSON file."""
        try:
            # Ensure the export directory exists
            os.makedirs('export', exist_ok=True)
            filepath = os.path.join('export', filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Results saved to: {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving results to file: {e}")

    def analyze_all_views_from_all_schemas(self, output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all views from all non-empty schemas."""
        # Get views from all non-empty schemas
        views = self.get_all_views(schema_name=None)  # None means all schemas
        results = []
        
        if not views:
            logger.warning("No views found in any non-empty schemas")
            return results
        
        # Group views by schema for better logging
        schema_counts = {}
        for view in views:
            schema = view['schema']
            schema_counts[schema] = schema_counts.get(schema, 0) + 1
        
        logger.info(f"Starting analysis of {len(views)} views from {len(schema_counts)} schemas:")
        for schema, count in schema_counts.items():
            logger.info(f"  {schema}: {count} views")
        
        for i, view in enumerate(views, 1):
            logger.info(f"Analyzing view {i}/{len(views)}: {view['schema']}.{view['name']}")
            
            # Get view columns
            columns = self.get_view_columns(view['name'], view['schema'])
            
            # Get view dependencies
            dependencies = self.get_view_dependencies(view['name'], view['schema'])
            
            # Send to ChatGPT for analysis
            analysis = self.send_to_chatgpt_api(view, columns, dependencies)
            
            analysis_result = {
                'view_info': view,
                'columns': columns,
                'dependencies': dependencies,
                'analysis': analysis,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            results.append(analysis_result)
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Save results to the file if specified
        if output_file:
            self.save_results_to_file(results, output_file)
        
        logger.info(f"Analysis completed for {len(results)} views from all schemas")
        return results

def parse_command_line_args():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze database views using ChatGPT')
    parser.add_argument('--schema', '-s', default=None,
                        help='Database schema to analyze (if not specified, interactive mode will be used)')
    parser.add_argument('--output', '-o', default=None,
                        help='Output JSON file name (if not specified, will be generated based on selection)')
    parser.add_argument('--all-schemas', '-a', action='store_true',
                        help='Analyze views from all non-empty schemas (bypasses interactive mode)')
    parser.add_argument('--interactive', '-i', action='store_true',
                        help='Force interactive mode even if schema is specified')
    
    return parser.parse_args()

def main():
    """Main function with interactive mode."""
    args = parse_command_line_args()
    
    # Initialize the analyzer
    analyzer = ViewAnalyzer()
    
    # Handle different execution modes
    if args.all_schemas and not args.interactive:
        # Non-interactive: Analyze all schemas
        output_filename = args.output or 'views_analysis_all_schemas.json'
        results = analyzer.analyze_all_views_from_all_schemas(output_filename)
        
        if results:
            print(f"\nAnalysis completed successfully!")
            print(f"Analyzed {len(results)} views from all non-empty schemas")
            print(f"Results saved to: export/{output_filename}")
        else:
            print("No views found to analyze")
            
    elif args.schema and not args.interactive:
        # Non-interactive: Analyze specific schema
        output_filename = args.output or f'views_analysis_{args.schema}.json'
        results = analyzer.analyze_all_views(args.schema, output_filename)
        
        if results:
            print(f"\nAnalysis completed successfully!")
            print(f"Analyzed {len(results)} views from schema '{args.schema}'")
            print(f"Results saved to: export/{output_filename}")
        else:
            print(f"No views found in schema '{args.schema}'")
            
    else:
        # Interactive mode
        available_schemas = get_available_schemas(analyzer.db_manager)
        
        if not available_schemas:
            print("No non-empty schemas found in the database.")
            return
        
        selected_schemas = select_schemas_interactive(available_schemas)
        
        if not selected_schemas:
            print("No schemas selected. Exiting...")
            return
        
        # Generate output filename based on selection
        if len(selected_schemas) == 1:
            output_filename = args.output or f'views_analysis_{selected_schemas[0]}.json'
        elif len(selected_schemas) == len(available_schemas):
            output_filename = args.output or 'views_analysis_all_schemas.json'
        else:
            schema_names = '_'.join(selected_schemas[:3])  # Limit filename length
            if len(selected_schemas) > 3:
                schema_names += f'_and_{len(selected_schemas)-3}_more'
            output_filename = args.output or f'views_analysis_{schema_names}.json'
        
        print(f"\nStarting analysis of views from selected schemas...")
        print(f"Output will be saved to: export/{output_filename}")
        
        # Analyze selected schemas
        results = analyzer.analyze_views_from_schemas(selected_schemas, output_filename)
        
        if results:
            print(f"\nAnalysis completed successfully!")
            print(f"Analyzed {len(results)} views from {len(selected_schemas)} schemas")
            print(f"Results saved to: export/{output_filename}")
            
            # Show summary by schema
            schema_counts = {}
            for result in results:
                schema = result['view_info']['schema']
                schema_counts[schema] = schema_counts.get(schema, 0) + 1
            
            print(f"\nSummary by schema:")
            for schema, count in schema_counts.items():
                print(f"  {schema}: {count} views")
        else:
            print("No views found to analyze in the selected schemas")

if __name__ == "__main__":
    main()
