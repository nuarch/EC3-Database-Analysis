from typing import List, Tuple, Dict
from collections import Counter
from database_connection_utility import DatabaseManager

# Object types to include
OBJECT_TYPES = ('U', 'P', 'FN', 'TF', 'IF', 'TT')

# Mapping of SQL Server object type descriptions to friendly names
OBJECT_TYPE_MAPPING = {
    'USER_TABLE': 'Tables',
    'SQL_STORED_PROCEDURE': 'Stored Procedures',
    'SQL_SCALAR_FUNCTION': 'Scalar Functions',
    'SQL_TABLE_VALUED_FUNCTION': 'Table-Valued Functions',
    'SQL_INLINE_TABLE_VALUED_FUNCTION': 'Inline Table-Valued Functions',
    'TABLE_TYPE': 'Table Types',
    'VIEW': 'Views',
    'SYNONYM': 'Synonyms',
    'AGGREGATE_FUNCTION': 'Aggregate Functions',
    'CLR_STORED_PROCEDURE': 'CLR Stored Procedures',
    'CLR_SCALAR_FUNCTION': 'CLR Scalar Functions',
    'CLR_TABLE_VALUED_FUNCTION': 'CLR Table-Valued Functions'
}

def get_friendly_object_type(object_type: str) -> str:
    """Convert SQL Server object type description to friendly name.
    
    Args:
        object_type: SQL Server object type description
        
    Returns:
        Human-readable object type name
    """
    return OBJECT_TYPE_MAPPING.get(object_type, object_type)

def fetch_database_objects(db_manager: DatabaseManager, schemas: List[str]) -> List[Tuple[str, str, str]]:
    """Fetches database objects from the database, filtered by non-empty schemas.

    Args:
        db_manager: DatabaseManager instance
        schemas: List of schema names to include

    Returns:
        List of tuples containing (schema_name, object_name, object_type)
    """
    if not schemas:
        return []
    
    # Create placeholders for schema names in the IN clause
    schema_placeholders = ','.join(['?' for _ in schemas])
    
    sql_query = f"""
    SELECT s.name      AS SchemaName,
           o.name      AS ObjectName,
           o.type_desc AS ObjectType
    FROM sys.objects o
             JOIN
         sys.schemas s
         ON
             o.schema_id = s.schema_id
    WHERE o.type IN ({','.join(['?' for _ in OBJECT_TYPES])})
      AND s.name IN ({schema_placeholders})
    ORDER BY
        s.name, o.type_desc, o.name
    """
    
    # Combine parameters: object types + schema names
    params = OBJECT_TYPES + tuple(schemas)
    
    return db_manager.execute_query(sql_query, params)


def generate_markdown(db_objects: List[Tuple[str, str, str]], schemas: List[str]) -> str:
    """Generates hierarchical Markdown from database objects.

    Args:
        db_objects: List of tuples containing (schema_name, object_name, object_type)
        schemas: List of schema names that were queried

    Returns:
        Formatted markdown string
    """
    markdown_output = []
    
    # Count objects by type for the summary section
    object_type_counts = Counter()
    for _, _, object_type in db_objects:
        friendly_object_type = get_friendly_object_type(object_type)
        object_type_counts[friendly_object_type] += 1
    
    # Add summary section at the top (always show)
    markdown_output.append("# Database Object Summary")
    markdown_output.append("")
    
    total_schemas = len(schemas)
    total_objects = sum(object_type_counts.values())
    
    markdown_output.append(f"**Total Schemas:** {total_schemas}")
    markdown_output.append(f"**Total Objects:** {total_objects}")
    markdown_output.append("")
    
    # Only show object type breakdown if there are objects
    if object_type_counts:
        # Sort object types alphabetically for consistent output
        for object_type in sorted(object_type_counts.keys()):
            count = object_type_counts[object_type]
            markdown_output.append(f"- **{object_type}:** {count}")
        
        markdown_output.append("")
    
    markdown_output.append("---")
    markdown_output.append("")
    
    # Generate the detailed hierarchical listing
    current_schema = None
    current_type = None

    for schema_name, object_name, object_type in db_objects:
        # Convert to friendly object type name
        friendly_object_type = get_friendly_object_type(object_type)
        
        if schema_name != current_schema:
            if current_schema is not None:
                markdown_output.append("")
            markdown_output.append(f"## {schema_name}")
            current_schema = schema_name
            current_type = None

        if friendly_object_type != current_type:
            markdown_output.append(f"### {friendly_object_type}")
            current_type = friendly_object_type

        markdown_output.append(f"  - {object_name}")

    return "\n".join(markdown_output)


def main():
    """Main function to execute the database object listing process."""
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Get non-empty schemas
    non_empty_schemas = db_manager.get_non_empty_schemas()
    
    # Fetch database objects only from non-empty schemas
    db_objects = fetch_database_objects(db_manager, non_empty_schemas)
    
    # Generate and print markdown output
    markdown_output = generate_markdown(db_objects, non_empty_schemas)
    print(markdown_output)


if __name__ == "__main__":
    main()
