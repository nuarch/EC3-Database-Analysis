"""
Stored Procedure Analyzer for ChatGPT Integration
This module connects to the database, retrieves stored procedures,
and sends them to OpenAI's ChatGPT API for code explanation.
"""

import requests
import json
import logging
from typing import List, Dict, Any, Optional
from DatabaseConnectionUtility import DatabaseManager
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(pastime)s - %(levelness)s - %(message)s')
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

class StoredProcedureAnalyzer:
    """Class to analyze stored procedures using ChatGPT API."""
    
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
    
    def get_all_stored_procedures(self, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Retrieve all stored procedures from the database, filtering by non-empty schemas."""
        
        # Get list of valid non-empty schemas
        valid_schemas = self.db_manager.get_non_empty_schemas()
        
        if not valid_schemas:
            logger.warning("No non-empty schemas found in the database")
            return []
        
        # If a specific schema is requested, check if it's in the valid schemas list
        if schema_name and schema_name not in valid_schemas:
            logger.warning(f"Schema '{schema_name}' is not in the list of non-empty schemas: {valid_schemas}")
            return []
        
        # Build the query with sys.sql_modules for complete procedure definitions
        if schema_name:
            # Single schema query
            query = """
        SELECT 
            s.name AS ROUTINE_SCHEMA,
            o.name AS ROUTINE_NAME,
            m.definition AS ROUTINE_DEFINITION,
            o.create_date AS CREATED,
            o.modify_date AS LAST_ALTERED,
            'PROCEDURE' AS ROUTINE_TYPE
        FROM sys.sql_modules m
        INNER JOIN sys.objects o ON m.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type = 'P'
        AND s.name = ?
        AND o.is_ms_shipped = 0
        ORDER BY s.name, o.name
        """
            query_params = (schema_name,)
        else:
            # Multiple schemas query - get procedures from all non-empty schemas
            placeholders = ','.join(['?'] * len(valid_schemas))
            query = f"""
        SELECT 
            s.name AS ROUTINE_SCHEMA,
            o.name AS ROUTINE_NAME,
            m.definition AS ROUTINE_DEFINITION,
            o.create_date AS CREATED,
            o.modify_date AS LAST_ALTERED,
            'PROCEDURE' AS ROUTINE_TYPE
        FROM sys.sql_modules m
        INNER JOIN sys.objects o ON m.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type = 'P'
        AND s.name IN ({placeholders})
        AND o.is_ms_shipped = 0
        ORDER BY s.name, o.name
        """
            query_params = tuple(valid_schemas)
        
        try:
            rows = self.db_manager.execute_query(query, query_params)
            procedures = []
            
            for row in rows:
                procedure = {
                    'schema': row[0],
                    'name': row[1],
                    'definition': row[2],
                    'created': row[3],
                    'last_altered': row[4],
                    'type': row[5]
                }
                procedures.append(procedure)
            
            if schema_name:
                logger.info(f"Retrieved {len(procedures)} stored procedures from schema '{schema_name}'")
            else:
                logger.info(f"Retrieved {len(procedures)} stored procedures from {len(valid_schemas)} non-empty schemas")
            
            return procedures
            
        except Exception as e:
            logger.error(f"Error retrieving stored procedures: {e}")
            return []
    
    def get_procedure_parameters(self, procedure_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
        """Get parameters for a specific stored procedure."""
        query = """
        SELECT 
            p.name AS PARAMETER_NAME,
            TYPE_NAME(p.user_type_id) AS DATA_TYPE,
            CASE 
                WHEN p.is_output = 1 THEN 'OUT'
                ELSE 'IN'
            END AS PARAMETER_MODE,
            p.max_length AS CHARACTER_MAXIMUM_LENGTH,
            p.precision AS NUMERIC_PRECISION,
            p.scale AS NUMERIC_SCALE,
            p.parameter_id AS ORDINAL_POSITION,
            p.has_default_value,
            p.default_value
        FROM sys.parameters p
        INNER JOIN sys.objects o ON p.object_id = o.object_id
        INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE s.name = ? 
        AND o.name = ?
        AND o.type = 'P'
        ORDER BY p.parameter_id
        """
        
        try:
            rows = self.db_manager.execute_query(query, (schema_name, procedure_name))
            parameters = []
            
            for row in rows:
                param = {
                    'name': row[0],
                    'data_type': row[1],
                    'mode': row[2],
                    'max_length': row[3],
                    'precision': row[4],
                    'scale': row[5],
                    'ordinal_position': row[6],
                    'has_default_value': row[7],
                    'default_value': row[8]
                }
                parameters.append(param)
            
            return parameters
            
        except Exception as e:
            logger.error(f"Error retrieving parameters for procedure {procedure_name}: {e}")
            return []
    
    def send_to_chatgpt_api(self, procedure_code: str, procedure_name: str) -> Optional[Dict[str, Any]]:
        """Send stored procedure code to ChatGPT API for explanation."""

        # Create a comprehensive prompt for ChatGPT
        prompt = f"""
Please analyze the following Microsoft SQL Server SQL stored procedure and provide a detailed explanation:

Procedure Name: {procedure_name}

SQL Code:
```sql
{procedure_code}
```

Please provide:
1. A clear explanation of what this stored procedure does
2. Analysis of its complexity level (Low/Medium/High)
3. Input parameters and their purposes
4. Business logic and workflow
5. Performance considerations
6. Potential issues or risks
7. Do not include assumptions or phrases like "likely"

Format your response as a structured analysis that is easy to read and understand.  Format your response as follows:

#### 1. Overview
#### 2. Complexity Level: (Low/Medium/High)
#### 3. Input Parameters
#### 4. Business Logic and Workflow
#### 5. Performance Considerations
#### 6. Potential Issues or Risks

"""
        
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert SQL database analyst. Analyze stored procedures and provide detailed, technical explanations that would be helpful for database administrators and developers."
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


                    # Log message if explanation contains "Incomplete code"
                    if "Incomplete Code" in explanation_text:
                        logger.warning(f"ChatGPT response for procedure '{procedure_name}' contains 'Incomplete Code'")

                    # Parse the response to extract structured information
                    analysis_result = self._parse_chatgpt_response(
                        explanation_text, 
                        procedure_name,
                        result
                    )
                    
                    logger.info(f"Successfully got explanation for procedure: {procedure_name}")
                    return analysis_result
                else:
                    logger.error(f"ChatGPT API request failed with status {response.status_code}: {response.text}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"ChatGPT API request error for procedure {procedure_name} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return None
        
        return None
    
    def _parse_chatgpt_response(self, explanation_text: str, procedure_name: str, api_response: Dict) -> Dict[str, Any]:
        """Parse ChatGPT response to extract structured information."""
        
        # Extract complexity if mentioned
        complexity = "Medium"  # Default
        explanation_upper = explanation_text.upper()
        if "COMPLEXITY LEVEL: LOW" in explanation_upper:
            complexity = "Low"
        elif "COMPLEXITY LEVEL: HIGH" in explanation_upper:
            complexity = "High"
        
        return {
            "procedure_name": procedure_name,
            "explanation": explanation_text,
            "complexity": complexity,
            "model_used": api_response.get('model', self.model),
            "tokens_used": api_response.get('usage', {}).get('total_tokens', 0),
            "api_response_id": api_response.get('id', '')
        }

    def analyze_all_procedures(self, schema_name: str = 'dbo', output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all stored procedures in a schema."""
        procedures = self.get_all_stored_procedures(schema_name)
        results = []
        
        if not procedures:
            logger.warning(f"No stored procedures found in schema '{schema_name}'")
            return results
        
        logger.info(f"Starting analysis of {len(procedures)} stored procedures...")
        
        for i, procedure in enumerate(procedures, 1):
            logger.info(f"Analyzing procedure {i}/{len(procedures)}: {procedure['name']}")
            
            # Get procedure parameters
            parameters = self.get_procedure_parameters(procedure['name'], schema_name)
            
            # Send to ChatGPT for explanation
            explanation = self.send_to_chatgpt_api(procedure['definition'], procedure['name'])
            
            analysis_result = {
                'procedure_info': procedure,
                'parameters': parameters,
                'chatgpt_explanation': explanation,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            results.append(analysis_result)
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Save results to the file if specified
        if output_file:
            self.save_results_to_file(results, output_file)
        
        logger.info(f"Analysis completed for {len(results)} stored procedures")
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

    def analyze_all_procedures_from_all_schemas(self, output_file: Optional[str] = None) -> List[Dict[str, Any]]:
        """Analyze all stored procedures from all non-empty schemas."""
        # Get procedures from all non-empty schemas
        procedures = self.get_all_stored_procedures(schema_name=None)  # None means all schemas
        results = []
        
        if not procedures:
            logger.warning("No stored procedures found in any non-empty schemas")
            return results
        
        # Group procedures by schema for better logging
        schema_counts = {}
        for proc in procedures:
            schema = proc['schema']
            schema_counts[schema] = schema_counts.get(schema, 0) + 1
        
        logger.info(f"Starting analysis of {len(procedures)} stored procedures across {len(schema_counts)} schemas:")
        for schema, count in schema_counts.items():
            logger.info(f"  - {schema}: {count} procedures")
        
        for i, procedure in enumerate(procedures, 1):
            logger.info(f"Analyzing procedure {i}/{len(procedures)}: {procedure['schema']}.{procedure['name']}")
            
            # Get procedure parameters
            parameters = self.get_procedure_parameters(procedure['name'], procedure['schema'])
            
            # Send to ChatGPT for explanation
            explanation = self.send_to_chatgpt_api(procedure['definition'], procedure['name'])
            
            analysis_result = {
                'procedure_info': procedure,
                'parameters': parameters,
                'chatgpt_explanation': explanation,
                'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            results.append(analysis_result)
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Save results to file if specified
        if output_file:
            self.save_results_to_file(results, output_file)
        
        logger.info(f"Analysis completed for {len(results)} stored procedures across {len(schema_counts)} schemas")
        return results

def main():
    """Main function to run the stored procedure analysis."""
    print("🤖 Stored Procedure Analyzer for ChatGPT Integration")
    print("=" * 60)

    # Initialize the analyzer - will load API key from chatgpt_config.py
    analyzer = StoredProcedureAnalyzer()

    # Test database connection
    if not analyzer.db_manager.test_connection():
        print("❌ Failed to connect to database. Please check your configuration.")
        return

    print("✅ Database connection successful!")

    # Check API configuration
    if analyzer.api_key:
        print(f"✅ ChatGPT API key loaded from configuration (Model: {analyzer.model})")
    else:
        print("⚠️  No ChatGPT API key found - running in simulation mode")
        return

    # Get available schemas
    schemas = analyzer.db_manager.get_non_empty_schemas()
    print(f"📊 Available non-empty schemas: {schemas}")

    if not schemas:
        print("❌ No non-empty schemas found in the database.")
        return

    # Choose analysis scope
    print("\nAnalysis Options:")
    print("1. Analyze specific schema")
    print("2. Analyze all non-empty schemas")

    choice = input("Choose option (1 or 2, default: 1): ").strip() or "1"

    if choice == "2":
        # Analyze all schemas
        print(f"\n🚀 Starting analysis of stored procedures from all non-empty schemas...")

        results = analyzer.analyze_all_procedures_from_all_schemas(
            output_file='stored_procedures_analysis_all_schemas.json'
        )

        if results:
            print(f"\n✅ Analysis complete!")
            print(f"📁 Results saved to export directory")
            print(f"📋 {len(results)} stored procedures analyzed across all schemas")

            # Display summary by schema
            schema_summary = {}
            for result in results:
                schema = result['procedure_info']['schema']
                schema_summary[schema] = schema_summary.get(schema, 0) + 1

            print("\n📊 Summary by schema:")
            for schema, count in schema_summary.items():
                print(f"   {schema}: {count} procedures")

            # Display token usage if using real API
            if analyzer.api_key:
                total_tokens = sum(r.get('chatgpt_explanation', {}).get('tokens_used', 0) for r in results)
                print(f"🔢 Total tokens used: {total_tokens}")
        else:
            print(f"\n⚠️ No stored procedures found in any schemas")

    else:
        # Analyze specific schema
        # Choose schema to analyze (default to 'dbo' if it exists, otherwise first schema)
        default_schema = 'dbo' if 'dbo' in schemas else schemas[0]
        schema_to_analyze = input(f"Enter schema name to analyze (default: {default_schema}): ").strip() or default_schema

        # Validate schema choice
        if schema_to_analyze not in schemas:
            print(f"❌ Schema '{schema_to_analyze}' is not in the list of non-empty schemas: {schemas}")
            return

        # Perform analysis
        print(f"\n🚀 Starting analysis of stored procedures in schema '{schema_to_analyze}'...")

        results = analyzer.analyze_all_procedures(
            schema_name=schema_to_analyze,
            output_file=f'stored_procedures_analysis_{schema_to_analyze}.json'
        )

        if results:
            print(f"\n✅ Analysis complete!")
            print(f"📁 Results saved to export directory")
            print(f"📋 {len(results)} stored procedures analyzed")

            # Display summary
            if analyzer.api_key:
                total_tokens = sum(r.get('chatgpt_explanation', {}).get('tokens_used', 0) for r in results)
                print(f"🔢 Total tokens used: {total_tokens}")
        else:
            print(f"\n⚠️ No stored procedures found in schema '{schema_to_analyze}'")

if __name__ == "__main__":
    main()
