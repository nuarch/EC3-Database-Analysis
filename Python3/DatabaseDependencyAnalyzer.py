import re
import logging
import time
from contextlib import contextmanager
from typing import List, Tuple, Dict, Set

import pyodbc
from pyodbc import Row

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler('database_analysis.log', mode='w')  # Log file
    ]
)
logger = logging.getLogger(__name__)

# Configuration constants
DB_CONFIG = {
  'driver': 'ODBC Driver 18 for SQL Server',
  'server': 'localhost',
  'database': 'EC3Database_Analysis',
  'uid': 'sa',
  'pwd': 'Passw0rd*',
  'TrustServerCertificate': 'yes'
}

OBJECT_TYPES = ('U', 'P', 'FN', 'TF', 'IF', 'TT')

SQL_QUERY = """
            SELECT s.name AS SchemaName, o.name AS ObjectName, o.type_desc AS ObjectType
            FROM EC3Database_Analysis.sys.objects o
                     JOIN EC3Database_Analysis.sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF')
            ORDER BY s.name, o.type_desc, o.name; \
            """

# Query to get stored procedure definitions
SP_DEFINITION_QUERY = """
                      SELECT s.name       AS SchemaName,
                             o.name       AS ProcedureName,
                             m.definition AS Definition,
                             'PROCEDURE'  AS ObjectType
                      FROM sys.objects o
                               JOIN sys.schemas s ON o.schema_id = s.schema_id
                               JOIN sys.sql_modules m ON o.object_id = m.object_id
                      WHERE o.type = 'P'
                      UNION ALL
                      SELECT s.name       AS SchemaName,
                             o.name       AS FunctionName,
                             m.definition AS Definition,
                             CASE
                                 WHEN o.type = 'FN' THEN 'SCALAR_FUNCTION'
                                 WHEN o.type = 'TF' THEN 'TABLE_FUNCTION'
                                 WHEN o.type = 'IF' THEN 'INLINE_FUNCTION'
                                 ELSE 'FUNCTION'
                                 END      AS ObjectType
                      FROM sys.objects o
                               JOIN sys.schemas s ON o.schema_id = s.schema_id
                               JOIN sys.sql_modules m ON o.object_id = m.object_id
                      WHERE o.type IN ('FN', 'TF', 'IF')
                      ORDER BY SchemaName, ObjectType, ProcedureName \
                      """

# Query to get all tables
TABLES_QUERY = """
               SELECT s.name  AS SchemaName,
                      t.name  AS TableName,
                      'TABLE' AS ObjectType
               FROM sys.tables t
                        JOIN sys.schemas s ON t.schema_id = s.schema_id
               ORDER BY s.name, t.name \
               """

# Query to get user-defined types
TYPES_QUERY = """
              SELECT s.name                                              AS SchemaName,
                     t.name                                              AS TypeName,
                     IIF(t.is_table_type = 1, 'TABLE_TYPE', 'USER_TYPE') AS ObjectType
              FROM sys.types t
                       JOIN sys.schemas s ON t.schema_id = s.schema_id
              WHERE t.is_user_defined = 1
              ORDER BY s.name, t.name \
              """


@contextmanager
def create_db_connection():
  """Creates and manages database connection using context manager."""
  logger.info("Attempting to connect to database...")
  logger.info(f"Server: {DB_CONFIG['server']}, Database: {DB_CONFIG['database']}")
  
  conn_string = 'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};TrustServerCertificate={TrustServerCertificate}'.format(
    **DB_CONFIG)
  
  try:
    connection = pyodbc.connect(conn_string)
    logger.info("✓ Database connection established successfully")
    yield connection
  except Exception as e:
    logger.error(f"✗ Failed to connect to database: {str(e)}")
    raise
  finally:
    logger.info("Closing database connection")
    connection.close()


def fetch_code_objects() -> list[Row]:
  """Fetches stored procedures and functions with their definitions.
  
  Returns:
      List of tuples containing (schema_name, object_name, definition, object_type)
  """
  logger.info("Fetching code objects (procedures and functions)...")
  start_time = time.time()
  
  with create_db_connection() as conn:
    cursor = conn.cursor()
    logger.info("Executing SQL query for code objects with definitions")
    cursor.execute(SP_DEFINITION_QUERY)
    results = cursor.fetchall()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✓ Fetched {len(results)} code objects in {elapsed_time:.2f} seconds")
    
    # Log code object type counts
    code_counts = {}
    for row in results:
      obj_type = row.ObjectType
      code_counts[obj_type] = code_counts.get(obj_type, 0) + 1
    
    logger.info("Code object type breakdown:")
    for obj_type, count in sorted(code_counts.items()):
      logger.info(f"  - {obj_type}: {count}")
    
    return results


def fetch_tables() -> List[Tuple[str, str, str]]:
  """Fetches all tables.
  
  Returns:
      List of tuples containing (schema_name, table_name, object_type)
  """
  logger.info("Fetching tables...")
  start_time = time.time()
  
  with create_db_connection() as conn:
    cursor = conn.cursor()
    logger.info("Executing SQL query for tables")
    cursor.execute(TABLES_QUERY)
    results = cursor.fetchall()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✓ Fetched {len(results)} tables in {elapsed_time:.2f} seconds")
    
    return results


def fetch_user_types() -> List[Tuple[str, str, str]]:
  """Fetches user-defined types.
  
  Returns:
      List of tuples containing (schema_name, type_name, object_type)
  """
  logger.info("Fetching user-defined types...")
  start_time = time.time()
  
  with create_db_connection() as conn:
    cursor = conn.cursor()
    logger.info("Executing SQL query for user-defined types")
    cursor.execute(TYPES_QUERY)
    results = cursor.fetchall()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✓ Fetched {len(results)} user-defined types in {elapsed_time:.2f} seconds")
    
    # Log type breakdown
    type_counts = {}
    for row in results:
      obj_type = row.ObjectType
      type_counts[obj_type] = type_counts.get(obj_type, 0) + 1
    
    if type_counts:
      logger.info("User-defined type breakdown:")
      for obj_type, count in sorted(type_counts.items()):
        logger.info(f"  - {obj_type}: {count}")
    
    return results


def extract_object_references(definition: str,
    available_objects: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
  """Extract object references from the stored procedures / functions definition.
  
  Args:
      definition: The SQL definition
      available_objects: Dictionary mapping object types to sets of object names (schema.object format)
  
  Returns:
      Dictionary mapping object types to sets of referenced object names
  """
  if not definition:
    return {obj_type: set() for obj_type in available_objects.keys()}

  # Convert to uppercase for case-insensitive matching
  definition_upper = definition.upper()
  referenced_objects = {obj_type: set() for obj_type in
                        available_objects.keys()}

  # Table reference keywords
  table_keywords = [
    r'\bFROM\s+',
    r'\bJOIN\s+',
    r'\bINTO\s+',
    r'\bUPDATE\s+',
    r'\bDELETE\s+FROM\s+',
    r'\bINSERT\s+INTO\s+'
  ]

  # Function reference keywords
  [
    r'\b\w*\s*=\s*',  # Assignment
    r'\bSELECT\s+.*',  # In SELECT clause
    r'\bWHERE\s+.*',  # In the WHERE clause
    r'\bHAVING\s+.*',  # In HAVING clause
    r'\bORDER\s+BY\s+.*',  # In ORDER BY clause
  ]

  # Type reference keywords (for user-defined types)
  type_keywords = [
    r'\bAS\s+',
    r'\bDECLARE\s+@\w+\s+',
    r'\b@\w+\s+',
    r'\bCAST\s*\(\s*.*\s+AS\s+',
    r'\bCONVERT\s*\(\s*'
  ]

  # Check for table references
  for table_full_name in available_objects.get('TABLE', set()):
    schema_name, table_name = table_full_name.split('.')

    patterns = [
      rf'\b{re.escape(schema_name.upper())}\.{re.escape(table_name.upper())}\b',
      rf'\b{re.escape(table_name.upper())}\b'
    ]

    for pattern in patterns:
      if re.search(pattern, definition_upper):
        for keyword_pattern in table_keywords:
          full_pattern = keyword_pattern + r'[^\s]*\s*' + pattern
          if re.search(full_pattern, definition_upper):
            referenced_objects['TABLE'].add(table_full_name)
            break

  # Check for function references
  for func_type in ['SCALAR_FUNCTION', 'TABLE_FUNCTION', 'INLINE_FUNCTION']:
    for func_full_name in available_objects.get(func_type, set()):
      schema_name, func_name = func_full_name.split('.')

      # Functions are typically called with parentheses
      patterns = [
        rf'\b{re.escape(schema_name.upper())}\.{re.escape(func_name.upper())}\s*\(',
        rf'\b{re.escape(func_name.upper())}\s*\('
      ]

      for pattern in patterns:
        if re.search(pattern, definition_upper):
          referenced_objects[func_type].add(func_full_name)
          break

  # Check for procedure references (EXEC calls)
  for proc_full_name in available_objects.get('PROCEDURE', set()):
    schema_name, proc_name = proc_full_name.split('.')

    patterns = [
      rf'\bEXEC\s+{re.escape(schema_name.upper())}\.{re.escape(proc_name.upper())}\b',
      rf'\bEXEC\s+{re.escape(proc_name.upper())}\b',
      rf'\bEXECUTE\s+{re.escape(schema_name.upper())}\.{re.escape(proc_name.upper())}\b',
      rf'\bEXECUTE\s+{re.escape(proc_name.upper())}\b'
    ]

    for pattern in patterns:
      if re.search(pattern, definition_upper):
        referenced_objects['PROCEDURE'].add(proc_full_name)
        break

  # Check for user-defined type references
  for type_category in ['USER_TYPE', 'TABLE_TYPE']:
    for type_full_name in available_objects.get(type_category, set()):
      schema_name, type_name = type_full_name.split('.')

      patterns = [
        rf'\b{re.escape(schema_name.upper())}\.{re.escape(type_name.upper())}\b',
        rf'\b{re.escape(type_name.upper())}\b'
      ]

      for pattern in patterns:
        if re.search(pattern, definition_upper):
          for keyword_pattern in type_keywords:
            full_pattern = keyword_pattern + r'[^\s]*\s*' + pattern
            if re.search(full_pattern, definition_upper):
              referenced_objects[type_category].add(type_full_name)
              break

  return referenced_objects


def build_dependency_graph() -> Dict[str, Dict[str, Set[str]]]:
  """Build a comprehensive dependency graph between all database objects.
  
  Returns:
      Dictionary mapping object names to dictionaries of referenced object types and names
  """
  logger.info("Building dependency graph...")
  start_time = time.time()
  
  logger.info("Step 1/4: Fetching code objects...")
  code_objects = fetch_code_objects()
  
  logger.info("Step 2/4: Fetching tables...")
  tables = fetch_tables()
  
  logger.info("Step 3/4: Fetching user-defined types...")
  user_types = fetch_user_types()

  logger.info("Step 4/4: Building available objects index...")
  # Create dictionaries of all available objects by type
  available_objects = {
    'TABLE': {f"{schema}.{name}" for schema, name, _ in tables},
    'PROCEDURE': {f"{schema}.{name}" for schema, name, _, obj_type in
                  code_objects if obj_type == 'PROCEDURE'},
    'SCALAR_FUNCTION': {f"{schema}.{name}" for schema, name, _, obj_type in
                        code_objects if obj_type == 'SCALAR_FUNCTION'},
    'TABLE_FUNCTION': {f"{schema}.{name}" for schema, name, _, obj_type in
                       code_objects if obj_type == 'TABLE_FUNCTION'},
    'INLINE_FUNCTION': {f"{schema}.{name}" for schema, name, _, obj_type in
                        code_objects if obj_type == 'INLINE_FUNCTION'},
    'USER_TYPE': {f"{schema}.{name}" for schema, name, obj_type in user_types if
                  obj_type == 'USER_TYPE'},
    'TABLE_TYPE': {f"{schema}.{name}" for schema, name, obj_type in user_types
                   if obj_type == 'TABLE_TYPE'}
  }

  logger.info("Available objects summary:")
  for obj_type, objects in available_objects.items():
    logger.info(f"  - {obj_type}: {len(objects)}")

  logger.info("Analyzing dependencies for each code object...")
  dependency_graph = {}
  processed_count = 0

  for obj_schema, obj_name, obj_definition, obj_type in code_objects:
    obj_full_name = f"{obj_schema}.{obj_name}"
    processed_count += 1
    
    if processed_count % 10 == 0:
      logger.info(f"  Processing object {processed_count}/{len(code_objects)}: {obj_full_name}")
    
    referenced_objects = extract_object_references(obj_definition,
                                                   available_objects)
    dependency_graph[obj_full_name] = {
      'type': obj_type,
      'references': referenced_objects
    }

  elapsed_time = time.time() - start_time
  logger.info(f"✓ Dependency graph built successfully in {elapsed_time:.2f} seconds")
  
  # Log dependency statistics
  total_deps = 0
  objects_with_deps = 0
  
  for obj_name, obj_info in dependency_graph.items():
    obj_deps = sum(len(refs) for refs in obj_info['references'].values())
    total_deps += obj_deps
    if obj_deps > 0:
      objects_with_deps += 1
  
  logger.info(f"Dependency statistics:")
  logger.info(f"  - Total objects analyzed: {len(dependency_graph)}")
  logger.info(f"  - Objects with dependencies: {objects_with_deps}")
  logger.info(f"  - Total dependencies found: {total_deps}")

  return dependency_graph


def generate_mermaid_diagram(
    dependency_graph: Dict[str, Dict[str, Set[str]]]) -> str:
  """Generate a Mermaid diagram showing the relationships.
  
  Args:
      dependency_graph: Dictionary mapping objects to their references
  
  Returns:
      Mermaid diagram as string
  """
  logger.info("Generating Mermaid diagram...")
  start_time = time.time()
  
  mermaid_lines = ["graph TD"]

  # Add nodes and relationships
  relationship_count = 0
  for obj_name, obj_info in dependency_graph.items():
    obj_clean = obj_name.replace('.', '_').replace(' ', '_')
    obj_type = obj_info['type']

    for ref_type, ref_objects in obj_info['references'].items():
      for ref_obj in ref_objects:
        ref_clean = ref_obj.replace('.', '_').replace(' ', '_')
        mermaid_lines.append(
          f"    {obj_clean}[\"{obj_name}<br/>({obj_type})\"] --> {ref_clean}"
          f"[\"{ref_obj}<br/>({ref_type})\"]")
        relationship_count += 1

  # Style different object types
  mermaid_lines.extend([
    "",
    "    classDef procedureClass fill:#e3f2fd,stroke:#1976d2,stroke-width:2px",
    "    classDef scalarFuncClass fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px",
    "    classDef tableFuncClass fill:#fce4ec,stroke:#c2185b,stroke-width:2px",
    "    classDef inlineFuncClass fill:#fff8e1,stroke:#f57f17,stroke-width:2px",
    "    classDef tableClass fill:#fff3e0,stroke:#e65100,stroke-width:2px",
    "    classDef userTypeClass fill:#e8f5e8,stroke:#388e3c,stroke-width:2px",
    "    classDef tableTypeClass fill:#f1f8e9,stroke:#689f38,stroke-width:2px"
  ])

  # Apply styles to nodes
  all_objects = set(dependency_graph.keys())
  for obj_info in dependency_graph.values():
    for ref_objects in obj_info['references'].values():
      all_objects.update(ref_objects)

  for obj_name in all_objects:
    obj_clean = obj_name.replace('.', '_').replace(' ', '_')

    # Determine object type for styling
    obj_type = None
    if obj_name in dependency_graph:
      obj_type = dependency_graph[obj_name]['type']
    else:
      # This is a referenced object, determine its type
      # We need to look it up in our available objects
      pass  # For now, we'll style based on common patterns

    if obj_type == 'PROCEDURE':
      mermaid_lines.append(f"    class {obj_clean} procedureClass")
    elif obj_type == 'SCALAR_FUNCTION':
      mermaid_lines.append(f"    class {obj_clean} scalarFuncClass")
    elif obj_type == 'TABLE_FUNCTION':
      mermaid_lines.append(f"    class {obj_clean} tableFuncClass")
    elif obj_type == 'INLINE_FUNCTION':
      mermaid_lines.append(f"    class {obj_clean} inlineFuncClass")
    elif 'TABLE' in obj_name.upper() or obj_type == 'TABLE':
      mermaid_lines.append(f"    class {obj_clean} tableClass")
    elif 'TYPE' in obj_name.upper() or obj_type in ['USER_TYPE', 'TABLE_TYPE']:
      if obj_type == 'TABLE_TYPE':
        mermaid_lines.append(f"    class {obj_clean} tableTypeClass")
      else:
        mermaid_lines.append(f"    class {obj_clean} userTypeClass")

  elapsed_time = time.time() - start_time
  logger.info(f"✓ Mermaid diagram generated with {relationship_count} relationships in {elapsed_time:.2f} seconds")
  
  return "\n".join(mermaid_lines)


def save_mermaid_diagram(mermaid_content: str, filename: str = "database_dependencies.mmd") -> str:
  """Save Mermaid diagram to a file.
  
  Args:
      mermaid_content: The Mermaid diagram content
      filename: Output filename (default: database_dependencies.mmd)
  
  Returns:
      The full path to the saved file
  """
  logger.info(f"Saving Mermaid diagram to file: {filename}")
  start_time = time.time()
  
  try:
    with open(filename, 'w', encoding='utf-8') as f:
      # Add header comment
      f.write("%% Database Dependencies Mermaid Diagram\n")
      f.write("%% Copy this content into a Mermaid diagram viewer\n")
      f.write("%% Online viewers: https://mermaid.live/ or "
              "https://mermaid-js.github.io/mermaid-live-editor/\n\n")
      f.write(mermaid_content)
    
    elapsed_time = time.time() - start_time
    logger.info(f"✓ Mermaid diagram saved successfully in {elapsed_time:.2f} seconds")
    
    return filename
    
  except Exception as e:
    logger.error(f"✗ Failed to save Mermaid diagram: {str(e)}")
    raise


def generate_text_report(
    dependency_graph: Dict[str, Dict[str, Set[str]]]) -> str:
  """Generate a comprehensive text-based dependency report.
  
  Args:
      dependency_graph: Dictionary mapping objects to their references
  
  Returns:
      Formatted text report
  """
  logger.info("Generating text report...")
  start_time = time.time()
  
  report_lines = ["# Database Dependencies Report", ""]

  # Summary statistics
  total_objects = len(dependency_graph)
  objects_with_deps = sum(1 for obj_info in dependency_graph.values()
                          if
                          any(refs for refs in obj_info['references'].values()))

  all_referenced_objects = set()
  object_type_counts = {}

  for obj_info in dependency_graph.values():
    obj_type = obj_info['type']
    object_type_counts[obj_type] = object_type_counts.get(obj_type, 0) + 1

    for ref_objects in obj_info['references'].values():
      all_referenced_objects.update(ref_objects)

  report_lines.extend([
    "## Summary",
    f"- Total Code Objects: {total_objects}",
    f"- Objects with Dependencies: {objects_with_deps}",
    f"- Unique Objects Referenced: {len(all_referenced_objects)}",
    ""
  ])

  # Object type breakdown
  report_lines.append("### Object Type Breakdown")
  for obj_type, count in sorted(object_type_counts.items()):
    report_lines.append(f"- {obj_type}: {count}")
  report_lines.append("")

  # Detailed dependencies
  report_lines.append("## Object Dependencies")
  report_lines.append("")

  for obj_name in sorted(dependency_graph.keys()):
    obj_info = dependency_graph[obj_name]
    obj_type = obj_info['type']
    references = obj_info['references']

    report_lines.append(f"### {obj_name} ({obj_type})")

    has_refs = False
    for ref_type, ref_objects in references.items():
      if ref_objects:
        has_refs = True
        report_lines.append(f"**References {ref_type}s:**")
        for ref_obj in sorted(ref_objects):
          report_lines.append(f"- {ref_obj}")

    if not has_refs:
      report_lines.append("*No dependencies found*")

    report_lines.append("")

  # Reverse dependencies - what references each object
  report_lines.append("## Object Usage")
  report_lines.append("")

  object_to_dependents = {}
  for obj_name, obj_info in dependency_graph.items():
    for ref_type, ref_objects in obj_info['references'].items():
      for ref_obj in ref_objects:
        if ref_obj not in object_to_dependents:
          object_to_dependents[ref_obj] = []
        object_to_dependents[ref_obj].append((obj_name, obj_info['type']))

  for obj_name in sorted(object_to_dependents.keys()):
    dependents = object_to_dependents[obj_name]
    report_lines.append(f"### {obj_name}")
    report_lines.append("**Used by:**")
    for dependent_name, dependent_type in sorted(dependents):
      report_lines.append(f"- {dependent_name} ({dependent_type})")
    report_lines.append("")

  elapsed_time = time.time() - start_time
  logger.info(f"✓ Text report generated in {elapsed_time:.2f} seconds")
  
  return "\n".join(report_lines)


def save_text_report(report_content: str, filename: str = "database_dependencies_report.md") -> str:
  """Save text report to a file.
  
  Args:
      report_content: The report content
      filename: Output filename (default: database_dependencies_report.md)
  
  Returns:
      The full path to the saved file
  """
  logger.info(f"Saving text report to file: {filename}")
  start_time = time.time()
  
  try:
    with open(filename, 'w', encoding='utf-8') as f:
      f.write(report_content)
    
    elapsed_time = time.time() - start_time
    logger.info(f"✓ Text report saved successfully in {elapsed_time:.2f} seconds")
    
    return filename
    
  except Exception as e:
    logger.error(f"✗ Failed to save text report: {str(e)}")
    raise


def main():
  """Main function to execute the database object listing process."""
  logger.info("="*80)
  logger.info("STARTING DATABASE DEPENDENCY ANALYSIS")
  logger.info("="*80)
  
  overall_start_time = time.time()
  
  try:
    print("Generating comprehensive database dependency analysis...")
    print("=" * 60)

    # Build dependency graph
    logger.info("Phase 1: Building dependency graph")
    dependency_graph = build_dependency_graph()

    # Generate reports
    logger.info("Phase 2: Generating reports")
    
    logger.info("Generating text report...")
    text_report = generate_text_report(dependency_graph)
    
    logger.info("Generating Mermaid diagram...")
    mermaid_diagram = generate_mermaid_diagram(dependency_graph)

    # Save reports to files
    logger.info("Phase 3: Saving reports to files")
    
    # Save text report
    report_file = save_text_report(text_report)
    print(f"✓ Dependencies report saved to: {report_file}")
    
    # Save Mermaid diagram
    mermaid_file = save_mermaid_diagram(mermaid_diagram)
    print(f"✓ Mermaid diagram saved to: {mermaid_file}")
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("# Analysis Summary")
    print("=" * 60)
    
    # Extract key statistics for console display
    total_objects = len(dependency_graph)
    objects_with_deps = sum(1 for obj_info in dependency_graph.values()
                           if any(refs for refs in obj_info['references'].values()))
    
    total_deps = 0
    for obj_info in dependency_graph.values():
      total_deps += sum(len(refs) for refs in obj_info['references'].values())
    
    print(f"Total Code Objects Analyzed: {total_objects}")
    print(f"Objects with Dependencies: {objects_with_deps}")
    print(f"Total Dependencies Found: {total_deps}")
    
    print("\n" + "=" * 60)
    print("# Output Files")
    print("=" * 60)
    print(f"1. Dependencies Report: {report_file}")
    print(f"2. Mermaid Diagram: {mermaid_file}")
    print(f"3. Analysis Log: database_analysis.log")
    
    print("\n" + "=" * 60)
    print("# How to View the Mermaid Diagram")
    print("=" * 60)
    print(f"1. Open {mermaid_file} in a text editor")
    print("2. Copy the diagram code (skip the comment lines)")
    print("3. Paste into a Mermaid viewer:")
    print("   - https://mermaid.live/")
    print("   - https://mermaid-js.github.io/mermaid-live-editor/")
    print("   - VS Code with Mermaid extension")
    
    overall_elapsed_time = time.time() - overall_start_time
    logger.info("="*80)
    logger.info(f"ANALYSIS COMPLETED SUCCESSFULLY in {overall_elapsed_time:.2f} seconds")
    logger.info("="*80)
    
    print(f"\n✓ Analysis completed in {overall_elapsed_time:.2f} seconds")

  except Exception as e:
    logger.error(f"FATAL ERROR: {str(e)}", exc_info=True)
    print(f"✗ Analysis failed: {str(e)}")
    print("✗ Check database_analysis.log for detailed error information")
    raise


if __name__ == "__main__":
  main()
