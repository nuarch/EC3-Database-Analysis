"""
Table Analyzer for ChatGPT Integration
This module connects to the database, retrieves tables and their structure,
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

class TableAnalyzer:
    """Class to analyze database tables using ChatGPT API."""
    
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
    
    def get_all_tables(self, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Retrieve all tables from the database, filtering by non-empty schemas."""
        
        # Get list of valid non-empty schemas
        valid_schemas = self.db_manager.get_non_empty_schemas()
        
        if not valid_schemas:
            logger.warning("No non-empty schemas found in the database")
            return []
        
        # If a specific schema is requested, check if it's in the valid schemas list
        if schema_name and schema_name not in valid_schemas:
            logger.warning(f"Schema '{schema_name}' is not in the list of non-empty schemas: {valid_schemas}")
            return []
        
        # Build the query to get table information
        if schema_name:
            # Single schema query
            query = """
            SELECT 
                s.name AS TABLE_SCHEMA,
                t.name AS TABLE_NAME,
                t.create_date AS CREATED,
                t.modify_date AS LAST_ALTERED,
                CASE t.type 
                    WHEN 'U' THEN 'BASE TABLE'
                    WHEN 'V' THEN 'VIEW'
                    ELSE 'OTHER'
                END AS TABLE_TYPE,
                ep.value AS TABLE_COMMENT
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON t.object_id = ep.major_id 
                AND ep.minor_id = 0 
                AND ep.name = 'MS_Description'
            WHERE s.name = ?
            AND t.is_ms_shipped = 0
            ORDER BY s.name, t.name
            """
            query_params = (schema_name,)
        else:
            # Multiple schemas query - get tables from all non-empty schemas
            placeholders = ','.join(['?'] * len(valid_schemas))
            query = f"""
            SELECT 
                s.name AS TABLE_SCHEMA,
                t.name AS TABLE_NAME,
                t.create_date AS CREATED,
                t.modify_date AS LAST_ALTERED,
                CASE t.type 
                    WHEN 'U' THEN 'BASE TABLE'
                    WHEN 'V' THEN 'VIEW'
                    ELSE 'OTHER'
                END AS TABLE_TYPE,
                ep.value AS TABLE_COMMENT
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON t.object_id = ep.major_id 
                AND ep.minor_id = 0 
                AND ep.name = 'MS_Description'
            WHERE s.name IN ({placeholders})
            AND t.is_ms_shipped = 0
            ORDER BY s.name, t.name
            """
            query_params = tuple(valid_schemas)
        
        try:
            rows = self.db_manager.execute_query(query, query_params)
            tables = []
            
            for row in rows:
                table = {
                    'schema': row[0],
                    'name': row[1],
                    'created': row[2],
                    'last_altered': row[3],
                    'type': row[4],
                    'comment': row[5]
                }
                tables.append(table)
            
            if schema_name:
                logger.info(f"Retrieved {len(tables)} tables from schema '{schema_name}'")
            else:
                logger.info(f"Retrieved {len(tables)} tables from {len(valid_schemas)} non-empty schemas")
            
            return tables
            
        except Exception as e:
            logger.error(f"Error retrieving tables: {e}")
            return []
    
    def get_table_columns(self, table_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Get detailed column information for a specific table."""
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
            CASE 
                WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END AS IS_PRIMARY_KEY,
            CASE 
                WHEN fk.COLUMN_NAME IS NOT NULL THEN 'YES'
                ELSE 'NO'
            END AS IS_FOREIGN_KEY,
            fk.REFERENCED_SCHEMA,
            fk.REFERENCED_TABLE,
            fk.REFERENCED_COLUMN,
            ep.value AS COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT 
                kcu.COLUMN_NAME,
                kcu.TABLE_SCHEMA,
                kcu.TABLE_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
                ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME 
            AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
            AND c.TABLE_NAME = pk.TABLE_NAME
        LEFT JOIN (
            SELECT 
                ccu.COLUMN_NAME,
                ccu.TABLE_SCHEMA,
                ccu.TABLE_NAME,
                SCHEMA_NAME(fk_tab.schema_id) AS REFERENCED_SCHEMA,
                fk_tab.name AS REFERENCED_TABLE,
                fk_col.name AS REFERENCED_COLUMN
            FROM sys.foreign_key_columns fkc
            INNER JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
            INNER JOIN sys.tables tab ON fk.parent_object_id = tab.object_id
            INNER JOIN sys.schemas sch ON tab.schema_id = sch.schema_id
            INNER JOIN sys.columns col ON fkc.parent_object_id = col.object_id AND fkc.parent_column_id = col.column_id
            INNER JOIN sys.tables fk_tab ON fk.referenced_object_id = fk_tab.object_id
            INNER JOIN sys.columns fk_col ON fkc.referenced_object_id = fk_col.object_id AND fkc.referenced_column_id = fk_col.column_id
            INNER JOIN INFORMATION_SCHEMA.COLUMNS ccu ON ccu.COLUMN_NAME = col.name 
                AND ccu.TABLE_NAME = tab.name 
                AND ccu.TABLE_SCHEMA = sch.name
        ) fk ON c.COLUMN_NAME = fk.COLUMN_NAME 
            AND c.TABLE_SCHEMA = fk.TABLE_SCHEMA 
            AND c.TABLE_NAME = fk.TABLE_NAME
        LEFT JOIN sys.extended_properties ep ON ep.major_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
            AND ep.minor_id = c.ORDINAL_POSITION
            AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """

        try:
            rows = self.db_manager.execute_query(query, (schema_name, table_name))
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
                    'is_primary_key': row[8],
                    'is_foreign_key': row[9],
                    'referenced_schema': row[10],
                    'referenced_table': row[11],
                    'referenced_column': row[12],
                    'comment': row[13]
                }
                columns.append(column)

            return columns

        except Exception as e:
            logger.error(f"Error retrieving columns for table {table_name}: {e}")
            return []

    def get_table_indexes(self, table_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Get index information for a specific table."""
        query = """
        SELECT 
            i.name AS INDEX_NAME,
            i.type_desc AS INDEX_TYPE,
            i.is_unique,
            i.is_primary_key,
            STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMNS
        FROM sys.indexes i
        INNER JOIN sys.objects o ON i.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE s.name = ? AND o.name = ?
        AND i.type > 0  -- Exclude heap
        GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
        ORDER BY i.is_primary_key DESC, i.is_unique DESC, i.name
        """

        try:
            rows = self.db_manager.execute_query(query, (schema_name, table_name))
            indexes = []

            for row in rows:
                index = {
                    'name': row[0],
                    'type': row[1],
                    'is_unique': row[2],
                    'is_primary_key': row[3],
                    'columns': row[4]
                }
                indexes.append(index)

            return indexes

        except Exception as e:
            logger.error(f"Error retrieving indexes for table {table_name}: {e}")
            return []

    def send_to_chatgpt_api(self, table_info: Dict[str, Any], columns: List[Dict[str, Any]], indexes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Send table structure to ChatGPT API for analysis."""

        table_name = table_info['name']
        schema_name = table_info['schema']

        # Format table structure for analysis
        table_structure = f"Table: {schema_name}.{table_name}\n"
        table_structure += f"Type: {table_info['type']}\n"
        if table_info.get('comment'):
            table_structure += f"Description: {table_info['comment']}\n"
        table_structure += f"Created: {table_info['created']}\n"
        table_structure += f"Last Modified: {table_info['last_altered']}\n\n"

        # Add columns information
        table_structure += "Columns:\n"
        for col in columns:
            col_info = f"  - {col['name']} ({col['data_type']}"
            if col['max_length']:
                col_info += f"({col['max_length']})"
            elif col['precision'] and col['scale']:
                col_info += f"({col['precision']},{col['scale']})"
            col_info += f", {'NOT NULL' if col['is_nullable'] == 'NO' else 'NULL'}"
            if col['is_primary_key'] == 'YES':
                col_info += ", PRIMARY KEY"
            if col['is_foreign_key'] == 'YES':
                col_info += f", FK -> {col['referenced_schema']}.{col['referenced_table']}.{col['referenced_column']}"
            if col['column_default']:
                col_info += f", DEFAULT: {col['column_default']}"
            col_info += ")"
            if col.get('comment'):
                col_info += f" -- {col['comment']}"
            table_structure += col_info + "\n"

        # Add indexes information
        if indexes:
            table_structure += "\nIndexes:\n"
            for idx in indexes:
                idx_info = f"  - {idx['name']} ({idx['type']}"
                if idx['is_primary_key']:
                    idx_info += ", PRIMARY KEY"
                elif idx['is_unique']:
                    idx_info += ", UNIQUE"
                idx_info += f") on [{idx['columns']}]"
                table_structure += idx_info + "\n"

        # Create a comprehensive prompt for ChatGPT
        prompt = f"""
Please analyze the following Microsoft SQL Server database table and provide a detailed analysis:

{table_structure}

Please provide:
1. A clear explanation of what this table represents and its purpose
2. Analysis of its complexity level (Low/Medium/High) based on structure, relationships, and indexes
3. Data model analysis including primary keys, foreign keys, and relationships
4. Business context and likely use cases
5. Data integrity considerations (constraints, nullability, defaults)
6. Performance considerations based on indexes and structure
7. Potential issues or improvement recommendations
8. Do not include assumptions or phrases like "likely" unless clearly marked as such

Format your response as a structured analysis that is easy to read and understand. Format your response as follows:

#### 1. Overview
#### 2. Complexity Level: (Low/Medium/High)
#### 3. Data Model Analysis
#### 4. Business Context and Use Cases
#### 5. Data Integrity Considerations
#### 6. Performance Considerations
#### 7. Potential Issues or Recommendations

"""

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert database analyst and data architect. Analyze database table structures and provide detailed, technical explanations that would be helpful for database administrators, developers, and data architects."
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
                        logger.warning(f"ChatGPT response for table '{table_name}' may contain incomplete analysis")

                    # Parse the response to extract structured information
                    analysis_result = self._parse_chatgpt_response(
                        explanation_text,
                        table_name,
                        result
                    )

                    logger.info(f"Successfully got analysis for table: {table_name}")
                    return analysis_result
                else:
                    logger.error(f"ChatGPT API request failed with status {response.status_code}: {response.text}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return None

            except requests.exceptions.RequestException as e:
                logger.error(f"ChatGPT API request error for table {table_name} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return None

        return None

    def _parse_chatgpt_response(self, explanation_text: str, table_name: str, api_response: Dict) -> Dict[str, Any]:
        """Parse ChatGPT response to extract structured information."""

        # Extract complexity if mentioned
        complexity = "Medium"  # Default
        explanation_upper = explanation_text.upper()
        if "COMPLEXITY LEVEL: LOW" in explanation_upper:
            complexity = "Low"
        elif "COMPLEXITY LEVEL: HIGH" in explanation_upper:
            complexity = "High"

        return {
            "table_name": table_name,
            "explanation": explanation_text,
            "complexity": complexity,
            "model_used": api_response.get('model', self.model),
            "tokens_used": api_response.get('usage', {}).get('total_tokens', 0),
            "api_response_id": api_response.get('id', '')
        }

    def analyze_all_tables(self, schema_name: str = 'dbo', output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all tables in a schema."""
        tables = self.get_all_tables(schema_name)
        results = []

        if not tables:
            logger.warning(f"No tables found in schema '{schema_name}'")
            return results

        logger.info(f"Starting analysis of {len(tables)} tables...")

        for i, table in enumerate(tables, 1):
            logger.info(f"Analyzing table {i}/{len(tables)}: {table['name']}")

            # Get table columns
            columns = self.get_table_columns(table['name'], table['schema'])

            # Get table indexes
            indexes = self.get_table_indexes(table['name'], table['schema'])

            # Send to ChatGPT for analysis
            analysis = self.send_to_chatgpt_api(table, columns, indexes)

            analysis_result = {
                'table_info': table,
                'columns': columns,
                'indexes': indexes,
                'analysis': analysis,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }

            results.append(analysis_result)

            # Small delay to avoid overwhelming the API
            time.sleep(1)

        # Save results to the file if specified
        if output_file:
            self.save_results_to_file(results, output_file)

        logger.info(f"Analysis completed for {len(results)} tables")
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

    def analyze_all_tables_from_all_schemas(self, output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all tables from all non-empty schemas."""
        # Get tables from all non-empty schemas
        tables = self.get_all_tables(schema_name=None)  # None means all schemas
        results = []

        if not tables:
            logger.warning("No tables found in any non-empty schemas")
            return results

        # Group tables by schema for better logging
        schema_counts = {}
        for table in tables:
            schema = table['schema']
            schema_counts[schema] = schema_counts.get(schema, 0) + 1

        logger.info(f"Starting analysis of {len(tables)} tables from {len(schema_counts)} schemas:")
        for schema, count in schema_counts.items():
            logger.info(f"  {schema}: {count} tables")

        for i, table in enumerate(tables, 1):
            logger.info(f"Analyzing table {i}/{len(tables)}: {table['schema']}.{table['name']}")

            # Get table columns
            columns = self.get_table_columns(table['name'], table['schema'])

            # Get table indexes
            indexes = self.get_table_indexes(table['name'], table['schema'])

            # Send to ChatGPT for analysis
            analysis = self.send_to_chatgpt_api(table, columns, indexes)

            analysis_result = {
                'table_info': table,
                'columns': columns,
                'indexes': indexes,
                'analysis': analysis,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }

            results.append(analysis_result)

            # Small delay to avoid overwhelming the API
            time.sleep(1)

        # Save results to the file if specified
        if output_file:
            self.save_results_to_file(results, output_file)

        logger.info(f"Analysis completed for {len(results)} tables from all schemas")
        return results

def parse_command_line_args():
    """Parse command line arguments"""
    import argparse

    parser = argparse.ArgumentParser(description='Analyze database tables using ChatGPT')
    parser.add_argument('--schema', '-s', default='dbo',
                        help='Database schema to analyze (default: dbo)')
    parser.add_argument('--output', '-o', default='tables_analysis.json',
                        help='Output JSON file name (default: tables_analysis.json)')
    parser.add_argument('--all-schemas', '-a', action='store_true',
                        help='Analyze tables from all non-empty schemas')

    return parser.parse_args()

def main():
    """Main function"""
    args = parse_command_line_args()

    # Initialize the analyzer
    analyzer = TableAnalyzer()

    if args.all_schemas:
        # Analyze all tables from all schemas
        output_filename = f'tables_analysis_all_schemas.json'
        results = analyzer.analyze_all_tables_from_all_schemas(output_filename)

        if results:
            print(f"\nAnalysis completed successfully!")
            print(f"Analyzed {len(results)} tables from all non-empty schemas")
            print(f"Results saved to: export/{output_filename}")
        else:
            print("No tables found to analyze")
    else:
        # Analyze tables from a specific schema
        output_filename = f'tables_analysis_{args.schema}.json'
        results = analyzer.analyze_all_tables(args.schema, output_filename)

        if results:
            print(f"\nAnalysis completed successfully!")
            print(f"Analyzed {len(results)} tables from schema '{args.schema}'")
            print(f"Results saved to: export/{output_filename}")
        else:
            print(f"No tables found in schema '{args.schema}'")

if __name__ == "__main__":
    main()
