"""
Function Analyzer for ChatGPT Integration
This module connects to the database, retrieves functions (Inline Table-Valued, Scalar, and Table-Valued),
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

class FunctionAnalyzer:
  """Class to analyze functions using ChatGPT API."""

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

  def get_all_functions(self, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
    """Retrieve all functions from the database, filtering by non-empty schemas."""

    # Get list of valid non-empty schemas
    valid_schemas = self.db_manager.get_non_empty_schemas()

    if not valid_schemas:
      logger.warning("No non-empty schemas found in the database")
      return []

    # If a specific schema is requested, check if it's in the valid schemas list
    if schema_name and schema_name not in valid_schemas:
      logger.warning(f"Schema '{schema_name}' is not in the list of non-empty schemas: {valid_schemas}")
      return []

    # Build the query with IN clause for multiple schemas
    if schema_name:
      # Single schema query
      query = """
              SELECT
                  ROUTINE_SCHEMA,
                  ROUTINE_NAME,
                  ROUTINE_DEFINITION,
                  CREATED,
                  LAST_ALTERED,
                  ROUTINE_TYPE,
                  DATA_TYPE,
                  IS_DETERMINISTIC
              FROM INFORMATION_SCHEMA.ROUTINES
              WHERE ROUTINE_TYPE = 'FUNCTION'
                AND ROUTINE_SCHEMA = ?
              ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME \
              """
      query_params = (schema_name,)
    else:
      # Multiple schemas query - get functions from all non-empty schemas
      placeholders = ','.join(['?'] * len(valid_schemas))
      query = f"""
            SELECT 
                ROUTINE_SCHEMA,
                ROUTINE_NAME,
                ROUTINE_DEFINITION,
                CREATED,
                LAST_ALTERED,
                ROUTINE_TYPE,
                DATA_TYPE,
                IS_DETERMINISTIC
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_TYPE = 'FUNCTION'
            AND ROUTINE_SCHEMA IN ({placeholders})
            ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
            """
      query_params = tuple(valid_schemas)

    try:
      rows = self.db_manager.execute_query(query, query_params)
      functions = []

      for row in rows:
        function = {
          'schema': row[0],
          'name': row[1],
          'definition': row[2],
          'created': row[3],
          'last_altered': row[4],
          'type': row[5],
          'data_type': row[6],
          'is_deterministic': row[7]
        }
        # Determine function subtype based on definition and return type
        function['function_subtype'] = self._determine_function_subtype(function)
        functions.append(function)

      if schema_name:
        logger.info(f"Retrieved {len(functions)} functions from schema '{schema_name}'")
      else:
        logger.info(f"Retrieved {len(functions)} functions from {len(valid_schemas)} non-empty schemas")

      return functions

    except Exception as e:
      logger.error(f"Error retrieving functions: {e}")
      return []

  def _determine_function_subtype(self, function: Dict[str, Any]) -> str:
    """Determine the function subtype based on its definition and return type."""
    definition = function.get('definition', '').upper()
    data_type = function.get('data_type', '').upper()

    # Check for inline table-valued function (returns TABLE and contains RETURN with table expression)
    if 'TABLE' in data_type and 'RETURN' in definition:
      if definition.count('RETURN') == 1 and ('SELECT' in definition.split('RETURN')[1][:200]):
        return 'Inline Table-Valued Function'
      else:
        return 'Multi-Statement Table-Valued Function'
    elif 'TABLE' in data_type:
      return 'Table-Valued Function'
    else:
      return 'Scalar Function'

  def get_function_parameters(self, function_name: str, schema_name: str = 'dbo') -> List[Dict[str, Any]]:
    """Get parameters for a specific function."""
    query = """
            SELECT
                PARAMETER_NAME,
                DATA_TYPE,
                PARAMETER_MODE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.PARAMETERS
            WHERE SPECIFIC_SCHEMA = ? AND SPECIFIC_NAME = ?
            ORDER BY ORDINAL_POSITION \
            """

    try:
      rows = self.db_manager.execute_query(query, (schema_name, function_name))
      parameters = []

      for row in rows:
        param = {
          'name': row[0],
          'data_type': row[1],
          'mode': row[2],
          'max_length': row[3],
          'precision': row[4],
          'scale': row[5]
        }
        parameters.append(param)

      return parameters

    except Exception as e:
      logger.error(f"Error retrieving parameters for function {function_name}: {e}")
      return []

  def send_to_chatgpt_api(self, function_code: str, function_name: str, function_subtype: str) -> Optional[Dict[str, Any]]:
    """Send function code to ChatGPT API for explanation."""

    # Create a comprehensive prompt for ChatGPT
    prompt = f"""
Please analyze the following Microsoft SQL Server SQL function and provide a detailed explanation:

Function Name: {function_name}
Function Type: {function_subtype}

SQL Code:
    
Please provide:
1. Assumptions made about the function
2. A clear explanation of what this function does
3. Analysis of its complexity level (Low/Medium/High)
4. Input parameters and their purposes
5. Return type and structure
6. Business logic and workflow
7. Performance considerations
8. Potential issues or risks


Answer 2 - 9 using assumptions made in 1.

Format your response as a structured analysis that is easy to read and understand.  Format your response as follows.  Do not include any additional text outside of the structured analysis.

#### 1. Overview & Assumptions
#### 2. Complexity Level: (Low/Medium/High)
#### 3. Input Parameters
#### 4. Return Type
#### 5. Business Logic and Workflow
#### 6. Performance Considerations
#### 7. Potential Issues or Risks
  
"""

    payload = {
      "model": self.model,
      "messages": [
        {
          "role": "system",
          "content": "You are an expert SQL database analyst. Analyze SQL functions including scalar functions, inline table-valued functions, and multi-statement table-valued functions. Provide detailed, technical explanations that would be helpful for database administrators and developers."
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

          # Parse the response to extract structured information
          analysis_result = self._parse_chatgpt_response(
            explanation_text,
            function_name,
            result
          )

          logger.info(f"Successfully got explanation for function: {function_name}")
          return analysis_result
        else:
          logger.error(f"ChatGPT API request failed with status {response.status_code}: {response.text}")
          if attempt < self.max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff
            continue
          return None

      except requests.exceptions.RequestException as e:
        logger.error(f"ChatGPT API request error for function {function_name} (attempt {attempt + 1}): {e}")
        if attempt < self.max_retries - 1:
          time.sleep(2 ** attempt)  # Exponential backoff
          continue
        return None

    return None

  def _parse_chatgpt_response(self, explanation_text: str, function_name: str, api_response: Dict) -> Dict[str, Any]:
    """Parse ChatGPT response to extract structured information."""

    # Extract complexity if mentioned
    complexity = "Medium"  # Default
    explanation_upper = explanation_text.upper()
    if "COMPLEXITY LEVEL: LOW" in explanation_upper:
      complexity = "Low"
    elif "COMPLEXITY LEVEL: HIGH" in explanation_upper:
      complexity = "High"

    return {
      "function_name": function_name,
      "explanation": explanation_text,
      "complexity": complexity,
      "model_used": api_response.get('model', self.model),
      "tokens_used": api_response.get('usage', {}).get('total_tokens', 0),
      "api_response_id": api_response.get('id', '')
    }

  def analyze_all_functions(self, schema_name: str = 'dbo', output_file: Optional[str] = None) -> List[Dict[str, Any]]:
    """Analyze all functions in a schema."""
    functions = self.get_all_functions(schema_name)
    results = []

    if not functions:
      logger.warning(f"No functions found in schema '{schema_name}'")
      return results

    logger.info(f"Starting analysis of {len(functions)} functions...")

    for i, function in enumerate(functions, 1):
      logger.info(f"Analyzing function {i}/{len(functions)}: {function['name']} ({function['function_subtype']})")

      # Get function parameters
      parameters = self.get_function_parameters(function['name'], schema_name)

      # Send to ChatGPT for explanation
      explanation = self.send_to_chatgpt_api(
        function['definition'],
        function['name'],
        function['function_subtype']
      )

      analysis_result = {
        'function_info': function,
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

    logger.info(f"Analysis completed for {len(results)} functions")
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

  def analyze_all_functions_from_all_schemas(self, output_file: Optional[str] = None) -> List[Dict[str, Any]]:
    """Analyze all functions from all non-empty schemas."""
    # Get functions from all non-empty schemas
    functions = self.get_all_functions(schema_name=None)  # None means all schemas
    results = []

    if not functions:
      logger.warning("No functions found in any non-empty schemas")
      return results

    # Group functions by schema and subtype for better logging
    schema_counts = {}
    subtype_counts = {}
    for func in functions:
      schema = func['schema']
      subtype = func['function_subtype']
      schema_counts[schema] = schema_counts.get(schema, 0) + 1
      subtype_counts[subtype] = subtype_counts.get(subtype, 0) + 1

    logger.info(f"Starting analysis of {len(functions)} functions across {len(schema_counts)} schemas:")
    for schema, count in schema_counts.items():
      logger.info(f"  - {schema}: {count} functions")

    logger.info("Function types breakdown:")
    for subtype, count in subtype_counts.items():
      logger.info(f"  - {subtype}: {count} functions")

    for i, function in enumerate(functions, 1):
      logger.info(f"Analyzing function {i}/{len(functions)}: {function['schema']}.{function['name']} ({function['function_subtype']})")

      # Get function parameters
      parameters = self.get_function_parameters(function['name'], function['schema'])

      # Send to ChatGPT for explanation
      explanation = self.send_to_chatgpt_api(
        function['definition'],
        function['name'],
        function['function_subtype']
      )

      analysis_result = {
        'function_info': function,
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

    logger.info(f"Analysis completed for {len(results)} functions across {len(schema_counts)} schemas")
    return results

def main():
  """Main function to run the function analysis."""
  print("🔧 Function Analyzer for ChatGPT Integration")
  print("=" * 60)

  # Initialize the analyzer - will load API key from chatgpt_config.py
  analyzer = FunctionAnalyzer()

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
    print(f"\n🚀 Starting analysis of functions from all non-empty schemas...")

    results = analyzer.analyze_all_functions_from_all_schemas(
      output_file='functions_analysis_all_schemas.json'
    )

    if results:
      print(f"\n✅ Analysis complete!")
      print(f"📁 Results saved to export directory")
      print(f"📋 {len(results)} functions analyzed across all schemas")

      # Display summary by schema and function type
      schema_summary = {}
      subtype_summary = {}
      for result in results:
        schema = result['function_info']['schema']
        subtype = result['function_info']['function_subtype']
        schema_summary[schema] = schema_summary.get(schema, 0) + 1
        subtype_summary[subtype] = subtype_summary.get(subtype, 0) + 1

      print("\n📊 Summary by schema:")
      for schema, count in schema_summary.items():
        print(f"   {schema}: {count} functions")

      print("\n🔧 Summary by function type:")
      for subtype, count in subtype_summary.items():
        print(f"   {subtype}: {count} functions")

      # Display token usage if using real API
      if analyzer.api_key:
        total_tokens = sum(r.get('chatgpt_explanation', {}).get('tokens_used', 0) for r in results)
        print(f"🔢 Total tokens used: {total_tokens}")
    else:
      print(f"\n⚠️ No functions found in any schemas")

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
    print(f"\n🚀 Starting analysis of functions in schema '{schema_to_analyze}'...")

    results = analyzer.analyze_all_functions(
      schema_name=schema_to_analyze,
      output_file=f'functions_analysis_{schema_to_analyze}.json'
    )

    if results:
      print(f"\n✅ Analysis complete!")
      print(f"📁 Results saved to export directory")
      print(f"📋 {len(results)} functions analyzed")

      # Display summary by function type
      subtype_summary = {}
      for result in results:
        subtype = result['function_info']['function_subtype']
        subtype_summary[subtype] = subtype_summary.get(subtype, 0) + 1

      print("\n🔧 Summary by function type:")
      for subtype, count in subtype_summary.items():
        print(f"   {subtype}: {count} functions")

      # Display token usage if using real API
      if analyzer.api_key:
        total_tokens = sum(r.get('chatgpt_explanation', {}).get('tokens_used', 0) for r in results)
        print(f"🔢 Total tokens used: {total_tokens}")
    else:
      print(f"\n⚠️ No functions found in schema '{schema_to_analyze}'")

if __name__ == "__main__":
  main()
