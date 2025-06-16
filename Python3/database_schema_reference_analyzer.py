import os
import csv
import logging
from datetime import datetime
from typing import Dict, List, Set, Tuple, NamedTuple
import sys
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import numpy as np
import seaborn as sns
import pandas as pd

# Import the database connection utility
try:
    from database_connection_utility import DatabaseManager
except ImportError:
    print("Error: Could not import database_connection_utility.py")
    print("Please ensure the file exists in the same directory or Python path")
    sys.exit(1)


class SchemaReference(NamedTuple):
    """Structure to hold schema reference information"""
    source_schema: str
    target_schema: str
    reference_type: str
    count: int = 1


class DatabaseSchemaReferenceAnalyzer:
    """
    Database analyzer that creates a cross-reference matrix showing
    the number and types of references from one schema to another.
    """

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.setup_logging()
        self.ensure_export_directory()
        self.reference_types = [
            'table_references',
            'view_dependencies',
            'procedure_references',
            'function_references',
            'type_references',
            'table_type_references'
        ]

    def setup_logging(self):
        """Setup logging configuration for progress tracking"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def ensure_export_directory(self):
        """Ensure the export directory exists"""
        self.export_dir = "export"
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)
            self.logger.info(f"Created export directory: {self.export_dir}")

    def get_non_empty_schemas(self) -> List[str]:
        """
        Get a list of non-empty schemas using the database connection utility
        """
        self.logger.info("Fetching non-empty schemas...")

        try:
            # Use the existing method from DatabaseManager
            schemas = self.db_manager.get_non_empty_schemas()
            self.logger.info(f"Found {len(schemas)} schemas: {', '.join(schemas)}")
            return schemas

        except Exception as e:
            self.logger.error(f"Error fetching schemas: {str(e)}")
            return []

    def get_table_references(self) -> List[SchemaReference]:
        """
        Get direct table references between schemas
        """
        self.logger.info("Analyzing direct table references...")

        try:
            # SQL Server specific query for direct table references across schemas
            # This captures table usage that isn't covered by foreign keys
            table_query = """
                          SELECT
                              s1.name as source_schema,
                              s2.name as target_schema,
                              COUNT(DISTINCT CONCAT(s1.name, '.', o1.name, '->', s2.name, '.', t.name)) as reference_count
                          FROM sys.sql_dependencies d
                                   JOIN sys.objects o1 ON d.object_id = o1.object_id
                                   JOIN sys.schemas s1 ON o1.schema_id = s1.schema_id
                                   JOIN sys.objects t ON d.referenced_major_id = t.object_id
                                   JOIN sys.schemas s2 ON t.schema_id = s2.schema_id
                          WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND t.type IN ('U', 'IT')  -- User tables and internal tables
                          GROUP BY s1.name, s2.name"""

            result = self.db_manager.execute_query(table_query)
            references = [
                SchemaReference(row[0], row[1], 'table_references', row[2])
                for row in result
            ]

            self.logger.info(f"Found {len(references)} direct table cross-schema reference groups")
            return references

        except Exception as e:
            self.logger.error(f"Error getting table references: {str(e)}")
            return []

    def get_view_references(self) -> List[SchemaReference]:
        """
        Get view references between schemas
        """
        self.logger.info("Analyzing view references...")

        try:
            # SQL Server specific query for views referencing tables in other schemas
            view_query = """
                         SELECT
                             s1.name as source_schema,
                             s2.name as target_schema,
                             COUNT(DISTINCT CONCAT(s1.name, '.', o1.name, '->', s2.name, '.', t.name)) as reference_count
                         FROM sys.sql_dependencies d
                                  JOIN sys.objects o1 ON d.object_id = o1.object_id
                                  JOIN sys.schemas s1 ON o1.schema_id = s1.schema_id
                                  JOIN sys.objects t ON d.referenced_major_id = t.object_id
                                  JOIN sys.schemas s2 ON t.schema_id = s2.schema_id
                         WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND t.type IN ('V')  -- Views
                         GROUP BY s1.name, s2.name"""

            result = self.db_manager.execute_query(view_query)
            references = [
                SchemaReference(row[0], row[1], 'view_dependencies', row[2])
                for row in result
            ]

            self.logger.info(f"Found {len(references)} view cross-schema reference groups")
            return references

        except Exception as e:
            self.logger.error(f"Error getting view references: {str(e)}")
            return []

    def get_stored_procedure_references(self) -> List[SchemaReference]:
        """
        Get stored procedure references between schemas
        """
        self.logger.info("Analyzing stored procedure references...")

        try:
            # SQL Server specific query for stored procedures referencing other schemas
            procedure_query = """
                              SELECT
                                  s1.name as source_schema,
                                  s2.name as target_schema,
                                  COUNT(DISTINCT CONCAT(s1.name, '.', o1.name, '->', s2.name, '.', t.name)) as reference_count
                              FROM sys.sql_dependencies d
                                       JOIN sys.objects o1 ON d.object_id = o1.object_id
                                       JOIN sys.schemas s1 ON o1.schema_id = s1.schema_id
                                       JOIN sys.objects t ON d.referenced_major_id = t.object_id
                                       JOIN sys.schemas s2 ON t.schema_id = s2.schema_id
                              WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND t.type IN ('P', 'PC')  -- Procedures, CLR procedures
                              GROUP BY s1.name, s2.name"""

            result = self.db_manager.execute_query(procedure_query)
            references = [
                SchemaReference(row[0], row[1], 'procedure_references', row[2])
                for row in result
            ]

            self.logger.info(f"Found {len(references)} stored procedure cross-schema references")
            return references

        except Exception as e:
            self.logger.error(f"Error getting stored procedure references: {str(e)}")
            return []

    def get_function_references(self) -> List[SchemaReference]:
        """
        Get function references between schemas
        """
        self.logger.info("Analyzing function references...")

        try:
            # SQL Server specific query for functions referencing other schemas
            function_query = """
                             SELECT
                                 s1.name as source_schema,
                                 s2.name as target_schema,
                                 COUNT(DISTINCT CONCAT(s1.name, '.', o1.name, '->', s2.name, '.', t.name)) as reference_count
                             FROM sys.sql_dependencies d
                                      JOIN sys.objects o1 ON d.object_id = o1.object_id
                                      JOIN sys.schemas s1 ON o1.schema_id = s1.schema_id
                                      JOIN sys.objects t ON d.referenced_major_id = t.object_id
                                      JOIN sys.schemas s2 ON t.schema_id = s2.schema_id
                             WHERE s1.name != s2.name  
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND t.type IN ('AF', 'FN', 'FS', 'FT', 'IF')  -- Functions, inline table-valued functions, scalar functions,
                             GROUP BY s1.name, s2.name"""

            result = self.db_manager.execute_query(function_query)
            references = [
                SchemaReference(row[0], row[1], 'function_references', row[2])
                for row in result
            ]

            self.logger.info(f"Found {len(references)} function cross-schema references")
            return references

        except Exception as e:
            self.logger.error(f"Error getting function references: {str(e)}")
            return []

    def get_type_references(self) -> List[SchemaReference]:
        """
        Get alias type (user-defined type) references between schemas
        """
        self.logger.info("Analyzing alias type references...")

        try:
            # SQL Server specific query to find cross-schema references to user-defined alias types
            # This looks for columns, parameters, and variables that use alias types from other schemas
            type_query = """
                         WITH TypeUsage AS (
                             -- Table columns using alias types from other schemas
                             SELECT
                                 SCHEMA_NAME(t.schema_id) as source_schema,
                                 SCHEMA_NAME(ut.schema_id) as target_schema,
                                 'column_type_usage' as usage_type,
                                 COUNT(*) as usage_count
                             FROM sys.columns c
                                      JOIN sys.tables t ON c.object_id = t.object_id
                                      JOIN sys.types ut ON c.user_type_id = ut.user_type_id
                             WHERE ut.is_user_defined = 1
                               AND ut.is_assembly_type = 0  -- Exclude CLR types, focus on alias types
                               AND SCHEMA_NAME(t.schema_id) != SCHEMA_NAME(ut.schema_id)
                             AND SCHEMA_NAME(t.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                             AND SCHEMA_NAME(ut.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                         GROUP BY SCHEMA_NAME(t.schema_id), SCHEMA_NAME(ut.schema_id)

                         UNION ALL

                         -- Stored procedure parameters using alias types from other schemas
                         SELECT
                             SCHEMA_NAME(o.schema_id) as source_schema,
                             SCHEMA_NAME(ut.schema_id) as target_schema,
                             'parameter_type_usage' as usage_type,
                             COUNT(*) as usage_count
                         FROM sys.parameters p
                                  JOIN sys.objects o ON p.object_id = o.object_id
                                  JOIN sys.types ut ON p.user_type_id = ut.user_type_id
                         WHERE ut.is_user_defined = 1
                           AND ut.is_assembly_type = 0  -- Exclude CLR types, focus on alias types
                           AND o.type IN ('P', 'FN', 'IF', 'TF', 'AF')  -- Procedures and functions
                           AND SCHEMA_NAME(o.schema_id) != SCHEMA_NAME(ut.schema_id)
                        AND SCHEMA_NAME(o.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                        AND SCHEMA_NAME(ut.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                         GROUP BY SCHEMA_NAME(o.schema_id), SCHEMA_NAME(ut.schema_id)
                             )
                         SELECT
                             source_schema,
                             target_schema,
                             SUM(usage_count) as reference_count
                         FROM TypeUsage
                         GROUP BY source_schema, target_schema
                         ORDER BY source_schema, target_schema \
                         """

            result = self.db_manager.execute_query(type_query)
            references = [
                SchemaReference(row[0], row[1], 'type_references', row[2])
                for row in result
            ]

            self.logger.info(f"Found {len(references)} alias type cross-schema reference groups")
            return references

        except Exception as e:
            self.logger.error(f"Error getting type references: {str(e)}")
            return []

    def get_table_type_references(self) -> List[SchemaReference]:
        """
        Get table type (user-defined table type) references between schemas
        """
        self.logger.info("Analyzing table type references...")

        try:
            # SQL Server specific query to find cross-schema references to user-defined table types
            # This looks for parameters and variables that use table types from other schemas
            table_type_query = """
                               WITH TableTypeUsage AS (
                                   -- Stored procedure parameters using table types from other schemas
                                   SELECT
                                       SCHEMA_NAME(o.schema_id) as source_schema,
                                       SCHEMA_NAME(tt.schema_id) as target_schema,
                                       'parameter_table_type_usage' as usage_type,
                                       COUNT(*) as usage_count
                                   FROM sys.parameters p
                                            JOIN sys.objects o ON p.object_id = o.object_id
                                            JOIN sys.table_types tt ON p.user_type_id = tt.user_type_id
                                   WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'AF')  -- Procedures and functions
                                     AND SCHEMA_NAME(o.schema_id) != SCHEMA_NAME(tt.schema_id)
                                   AND SCHEMA_NAME(o.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                                   AND SCHEMA_NAME(tt.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                               GROUP BY SCHEMA_NAME(o.schema_id), SCHEMA_NAME(tt.schema_id)

                               UNION ALL

                               -- Function return types using table types from other schemas
                               SELECT
                                   SCHEMA_NAME(o.schema_id) as source_schema,
                                   SCHEMA_NAME(tt.schema_id) as target_schema,
                                   'return_table_type_usage' as usage_type,
                                   COUNT(*) as usage_count
                               FROM sys.objects o
                                        JOIN sys.table_types tt ON EXISTS (
                                   SELECT 1 FROM sys.columns c
                                   WHERE c.object_id = o.object_id
                                     AND c.user_type_id = tt.user_type_id
                               )
                               WHERE o.type IN ('IF', 'TF')  -- Table-valued functions
                                 AND SCHEMA_NAME(o.schema_id) != SCHEMA_NAME(tt.schema_id)
                        AND SCHEMA_NAME(o.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                        AND SCHEMA_NAME(tt.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                               GROUP BY SCHEMA_NAME(o.schema_id), SCHEMA_NAME(tt.schema_id)

                               UNION ALL

                               -- Direct references via sys.sql_dependencies for table types
                               SELECT
                                   SCHEMA_NAME(o1.schema_id) as source_schema,
                                   SCHEMA_NAME(tt.schema_id) as target_schema,
                                   'dependency_table_type_usage' as usage_type,
                                   COUNT(*) as usage_count
                               FROM sys.sql_dependencies d
                                        JOIN sys.objects o1 ON d.object_id = o1.object_id
                                        JOIN sys.table_types tt ON d.referenced_major_id = tt.type_table_object_id
                               WHERE SCHEMA_NAME(o1.schema_id) != SCHEMA_NAME(tt.schema_id)
                        AND SCHEMA_NAME(o1.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                        AND SCHEMA_NAME(tt.schema_id) NOT IN ('sys', 'INFORMATION_SCHEMA')
                        AND o1.type IN ('P', 'FN', 'IF', 'TF', 'AF', 'V')  -- Procedures, functions, views
                               GROUP BY SCHEMA_NAME(o1.schema_id), SCHEMA_NAME(tt.schema_id)
                                   )
                               SELECT
                                   source_schema,
                                   target_schema,
                                   SUM(usage_count) as reference_count
                               FROM TableTypeUsage
                               GROUP BY source_schema, target_schema
                               ORDER BY source_schema, target_schema \
                               """

            result = self.db_manager.execute_query(table_type_query)
            references = [
                SchemaReference(row[0], row[1], 'table_type_references', row[2])
                for row in result
            ]

            self.logger.info(f"Found {len(references)} table type cross-schema reference groups")
            return references

        except Exception as e:
            self.logger.error(f"Error getting table type references: {str(e)}")
            return []

    def build_detailed_reference_matrix(self, schemas: List[str]) -> Tuple[
        Dict[str, Dict[str, Dict[str, int]]], Dict[str, Dict[str, int]]]:
        """
        Build a detailed cross-reference matrix with reference types
        Returns: (detailed_matrix, summary_matrix)
        """
        self.logger.info("Building detailed schema reference matrix...")

        # Initialize a detailed matrix: source -> target -> reference_type -> count
        detailed_matrix = {}
        for source_schema in schemas:
            detailed_matrix[source_schema] = {}
            for target_schema in schemas:
                detailed_matrix[source_schema][target_schema] = {}
                for ref_type in self.reference_types:
                    detailed_matrix[source_schema][target_schema][ref_type] = 0

        # Initialize summary matrix: source -> target -> total_count
        summary_matrix = {}
        for source_schema in schemas:
            summary_matrix[source_schema] = {}
            for target_schema in schemas:
                summary_matrix[source_schema][target_schema] = 0

        # Get all types of references
        all_references = []
        all_references.extend(self.get_table_references())
        all_references.extend(self.get_view_references())
        all_references.extend(self.get_stored_procedure_references())
        all_references.extend(self.get_function_references())
        all_references.extend(self.get_type_references())
        all_references.extend(self.get_table_type_references())

        # Populate matrices
        for ref in all_references:
            if (ref.source_schema in detailed_matrix and
                ref.target_schema in detailed_matrix[ref.source_schema]):

                detailed_matrix[ref.source_schema][ref.target_schema][ref.reference_type] += ref.count
                summary_matrix[ref.source_schema][ref.target_schema] += ref.count

        # Log summary
        total_references = sum(
            sum(target_counts.values())
            for target_counts in summary_matrix.values()
        )
        self.logger.info(f"Total cross-schema references: {total_references}")

        return detailed_matrix, summary_matrix

    def generate_summary_csv(self, schemas: List[str], summary_matrix: Dict[str, Dict[str, int]], timestamp: str) -> str:
        """
        Generate summary CSV with total references between schemas
        """
        filename = f"{timestamp}--schema_references_summary.csv"
        filepath = os.path.join(self.export_dir, filename)

        self.logger.info(f"Generating summary CSV: {filepath}")

        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['source_schema'] + schemas
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header
                writer.writeheader()

                # Write data rows
                for source_schema in schemas:
                    row = {'source_schema': source_schema}
                    for target_schema in schemas:
                        row[target_schema] = summary_matrix[source_schema][target_schema]
                    writer.writerow(row)

                # Write summary rows
                summary_row = {'source_schema': 'TOTAL_INCOMING'}
                for target_schema in schemas:
                    total_incoming = sum(
                        summary_matrix[source][target_schema]
                        for source in schemas
                    )
                    summary_row[target_schema] = total_incoming
                writer.writerow(summary_row)

                summary_row = {'source_schema': 'TOTAL_OUTGOING'}
                for source_schema in schemas:
                    total_outgoing = sum(summary_matrix[source_schema].values())
                    summary_row[source_schema] = total_outgoing
                writer.writerow(summary_row)

            self.logger.info(f"Summary CSV generated: {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error generating summary CSV: {str(e)}")
            return ""

    def generate_detailed_csv(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]], timestamp: str) -> str:
        """
        Generate detailed CSV with reference types breakdown
        """
        filename = f"{timestamp}--schema_references_detailed.csv"
        filepath = os.path.join(self.export_dir, filename)

        self.logger.info(f"Generating detailed CSV: {filepath}")

        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                # Create fieldnames: source_schema, target_schema, then each reference type
                fieldnames = ['source_schema', 'target_schema'] + self.reference_types + ['total_references']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header
                writer.writeheader()

                # Write a detailed breakdown for each schema pair
                for source_schema in schemas:
                    for target_schema in schemas:
                        # Only write rows with actual references
                        total_refs = sum(detailed_matrix[source_schema][target_schema].values())
                        if total_refs > 0:
                            row = {
                                'source_schema': source_schema,
                                'target_schema': target_schema,
                                'total_references': total_refs
                            }
                            for ref_type in self.reference_types:
                                row[ref_type] = detailed_matrix[source_schema][target_schema][ref_type]
                            writer.writerow(row)

            self.logger.info(f"Detailed CSV generated: {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error generating detailed CSV: {str(e)}")
            return ""

    def generate_reference_type_summary_csv(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]], timestamp: str) -> str:
        """
        Generate CSV summarizing references by type across all schemas
        """
        filename = f"{timestamp}--schema_references_by_type.csv"
        filepath = os.path.join(self.export_dir, filename)

        self.logger.info(f"Generating reference type summary CSV: {filepath}")

        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['reference_type', 'total_count', 'schema_pairs_affected']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header
                writer.writeheader()

                # Calculate totals for each reference type
                for ref_type in self.reference_types:
                    total_count = 0
                    schema_pairs_affected = 0

                    for source_schema in schemas:
                        for target_schema in schemas:
                            count = detailed_matrix[source_schema][target_schema][ref_type]
                            total_count += count
                            if count > 0:
                                schema_pairs_affected += 1

                    writer.writerow({
                        'reference_type': ref_type,
                        'total_count': total_count,
                        'schema_pairs_affected': schema_pairs_affected
                    })

            self.logger.info(f"Reference type summary CSV generated: {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error generating reference type summary CSV: {str(e)}")
            return ""


    def plot_reference_matrix(self, schemas: List[str], summary_matrix: Dict[str, Dict[str, int]], timestamp: str) -> str:
        """
        Generate a heatmap visualization of the schema reference matrix with exponential coloring
        """
        self.logger.info("Generating reference matrix heatmap with exponential coloring...")

        try:
            # Sort schemas alphabetically for X-axis (target schemas)
            sorted_schemas_x = sorted(schemas)
            # Reverse alphabetical order for Y-axis (source schemas)
            sorted_schemas_y = sorted(schemas, reverse=True)

            # Convert matrix to a numpy array for plotting using different orders for X and Y
            matrix_data = []
            for source_schema in sorted_schemas_y:  # Y-axis in reverse order
                row = []
                for target_schema in sorted_schemas_x:  # X-axis in normal order
                    row.append(summary_matrix[source_schema][target_schema])
                matrix_data.append(row)

            matrix_array = np.array(matrix_data, dtype=float)

            # Determine figure size based on the number of schemas
            n_schemas = len(schemas)
            fig_width = max(10, n_schemas * 0.8)
            fig_height = max(8, n_schemas * 0.8)

            # Create the plot
            plt.figure(figsize=(fig_width, fig_height))

            max_val = matrix_array.max()

            if max_val > 0:
                # Apply exponential transformation to enhance visibility of lower values
                # Use log1p (log(1+x)) to handle zeros gracefully and compress the range
                transformed_array = np.log1p(matrix_array)

                # Create a custom colormap for better visual distinction
                from matplotlib.colors import LinearSegmentedColormap

                # Enhanced color scheme: white -> light blue -> medium blue -> dark blue -> navy
                colors_list = [
                    '#FFFFFF',  # White for 0
                    '#E3F2FD',  # Very light blue for 1
                    '#90CAF9',  # Light blue for small values
                    '#42A5F5',  # Medium blue for medium values
                    '#1976D2',  # Dark blue for high values
                    '#0D47A1'   # Navy for very high values
                ]

                cmap = LinearSegmentedColormap.from_list('exp_blue', colors_list, N=256)

                # Create heatmap with transformed data
                im = plt.imshow(transformed_array, cmap=cmap, aspect='equal')

                # Create custom colorbar with original values
                cbar = plt.colorbar(im, shrink=0.8)

                # Set colorbar ticks to show meaningful original values
                if max_val <= 10:
                    # For small ranges, show every integer
                    tick_values = list(range(int(max_val) + 1))
                elif max_val <= 50:
                    # For medium ranges, show multiples of 5
                    tick_values = list(range(0, int(max_val) + 1, 5))
                    if int(max_val) % 5 != 0:
                        tick_values.append(int(max_val))
                else:
                    # For large ranges, use exponential spacing
                    tick_values = [0, 1, 2, 5, 10, 20, 50]
                    # Add values that make sense for the data range
                    if max_val > 50:
                        tick_values.extend([100, 200, 500])
                    if max_val > 500:
                        tick_values.extend([1000, 2000, 5000])
                    if max_val > 5000:
                        tick_values.extend([10000])

                    # Keep only values <= max_val and add max_val if not present
                    tick_values = [v for v in tick_values if v <= max_val]
                    if int(max_val) not in tick_values:
                        tick_values.append(int(max_val))

                # Convert tick values to transformed space for colorbar positioning
                tick_positions = [np.log1p(val) for val in tick_values]

                cbar.set_ticks(tick_positions)
                cbar.set_ticklabels(tick_values)
                cbar.set_label('Number of References (Exponential Scale)', rotation=270, labelpad=20)

                # Add text annotations for non-zero values to show exact counts
                for i in range(len(sorted_schemas_y)):
                    for j in range(len(sorted_schemas_x)):
                        value = int(matrix_array[i, j])
                        if value > 0:
                            # Choose text color based on background intensity
                            text_color = 'white' if transformed_array[i, j] > np.percentile(transformed_array[transformed_array > 0], 60) else 'black'
                            plt.text(j, i, str(value), ha='center', va='center',
                                     fontsize=8, fontweight='bold', color=text_color)

            else:
                # All zeros case
                from matplotlib.colors import LinearSegmentedColormap
                colors_gradient = ['#F7FBFF', '#E3F2FD']
                cmap = LinearSegmentedColormap.from_list('zero_blue', colors_gradient, N=256)

                im = plt.imshow(matrix_array, cmap=cmap, aspect='equal')
                cbar = plt.colorbar(im, shrink=0.8)
                cbar.set_label('Number of References', rotation=270, labelpad=20)

            # Set labels
            plt.xlabel('Target Schema (Referenced)', fontsize=12, fontweight='bold')
            plt.ylabel('Source Schema (Referencing)', fontsize=12, fontweight='bold')
            plt.title('Schema Cross-Reference Matrix (Exponential Color Scale)\n(Values show number of references from source to target)',
                      fontsize=14, fontweight='bold', pad=20)

            # Set tick labels using different orders for X and Y axes
            plt.xticks(range(len(sorted_schemas_x)), sorted_schemas_x, rotation=45, ha='right')
            plt.yticks(range(len(sorted_schemas_y)), sorted_schemas_y)

            # Add grid for better readability
            plt.grid(True, alpha=0.3, linewidth=0.5)

            # Adjust the layout to prevent label cutoff
            plt.tight_layout()

            # Save the plot
            filename = f"{timestamp}--schema_references_matrix.png"
            filepath = os.path.join(self.export_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()  # Close the figure to free memory

            self.logger.info(f"Reference matrix heatmap with exponential coloring saved: {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error generating heatmap: {str(e)}")
            return ""

    def plot_reference_matrix_no_types(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]], timestamp: str) -> str:
        """
        Generate a heatmap visualization of the schema reference matrix excluding type references
        """
        self.logger.info("Generating reference matrix heatmap without type references...")

        try:
            # Sort schemas alphabetically for X-axis (target schemas)
            sorted_schemas_x = sorted(schemas)
            # Reverse alphabetical order for Y-axis (source schemas)
            sorted_schemas_y = sorted(schemas, reverse=True)

            # Create a filtered matrix excluding type references
            filtered_matrix = {}
            for source_schema in schemas:
                filtered_matrix[source_schema] = {}
                for target_schema in schemas:
                    # Sum all reference types except type_references and table_type_references
                    total_count = 0
                    for ref_type in self.reference_types:
                        if ref_type not in ['type_references', 'table_type_references']:
                            total_count += detailed_matrix[source_schema][target_schema][ref_type]
                    filtered_matrix[source_schema][target_schema] = total_count

            # Convert matrix to a numpy array for plotting using different orders for X and Y
            matrix_data = []
            for source_schema in sorted_schemas_y:  # Y-axis in reverse order
                row = []
                for target_schema in sorted_schemas_x:  # X-axis in normal order
                    row.append(filtered_matrix[source_schema][target_schema])
                matrix_data.append(row)

            matrix_array = np.array(matrix_data, dtype=float)

            # Determine figure size based on the number of schemas
            n_schemas = len(schemas)
            fig_width = max(10, n_schemas * 0.8)
            fig_height = max(8, n_schemas * 0.8)

            # Create the plot
            plt.figure(figsize=(fig_width, fig_height))

            max_val = matrix_array.max()

            if max_val > 0:
                # Apply exponential transformation to enhance visibility of lower values
                # Use log1p (log(1+x)) to handle zeros gracefully and compress the range
                transformed_array = np.log1p(matrix_array)

                # Create a custom colormap for better visual distinction
                from matplotlib.colors import LinearSegmentedColormap

                # Enhanced color scheme: white -> light green -> medium green -> dark green -> forest green
                colors_list = [
                    '#FFFFFF',  # White for 0
                    '#E8F5E8',  # Very light green for 1
                    '#81C784',  # Light green for small values
                    '#4CAF50',  # Medium green for medium values
                    '#388E3C',  # Dark green for high values
                    '#1B5E20'   # Forest green for very high values
                ]

                cmap = LinearSegmentedColormap.from_list('exp_green', colors_list, N=256)

                # Create heatmap with transformed data
                im = plt.imshow(transformed_array, cmap=cmap, aspect='equal')

                # Create custom colorbar with original values
                cbar = plt.colorbar(im, shrink=0.8)

                # Set colorbar ticks to show meaningful original values
                if max_val <= 10:
                    # For small ranges, show every integer
                    tick_values = list(range(int(max_val) + 1))
                elif max_val <= 50:
                    # For medium ranges, show multiples of 5
                    tick_values = list(range(0, int(max_val) + 1, 5))
                    if int(max_val) % 5 != 0:
                        tick_values.append(int(max_val))
                else:
                    # For large ranges, use exponential spacing
                    tick_values = [0, 1, 2, 5, 10, 20, 50]
                    # Add values that make sense for the data range
                    if max_val > 50:
                        tick_values.extend([100, 200, 500])
                    if max_val > 500:
                        tick_values.extend([1000, 2000, 5000])
                    if max_val > 5000:
                        tick_values.extend([10000])

                    # Keep only values <= max_val and add max_val if not present
                    tick_values = [v for v in tick_values if v <= max_val]
                    if int(max_val) not in tick_values:
                        tick_values.append(int(max_val))

                # Convert tick values to transformed space for colorbar positioning
                tick_positions = [np.log1p(val) for val in tick_values]

                cbar.set_ticks(tick_positions)
                cbar.set_ticklabels(tick_values)
                cbar.set_label('Number of References (Exponential Scale)', rotation=270, labelpad=20)

                # Add text annotations for non-zero values to show exact counts
                for i in range(len(sorted_schemas_y)):
                    for j in range(len(sorted_schemas_x)):
                        value = int(matrix_array[i, j])
                        if value > 0:
                            # Choose text color based on background intensity
                            text_color = 'white' if transformed_array[i, j] > np.percentile(transformed_array[transformed_array > 0], 60) else 'black'
                            plt.text(j, i, str(value), ha='center', va='center',
                                     fontsize=8, fontweight='bold', color=text_color)

            else:
                # All zeros case
                from matplotlib.colors import LinearSegmentedColormap
                colors_gradient = ['#F7FFF7', '#E8F5E8']
                cmap = LinearSegmentedColormap.from_list('zero_green', colors_gradient, N=256)

                im = plt.imshow(matrix_array, cmap=cmap, aspect='equal')
                cbar = plt.colorbar(im, shrink=0.8)
                cbar.set_label('Number of References', rotation=270, labelpad=20)

            # Set labels
            plt.xlabel('Target Schema (Referenced)', fontsize=12, fontweight='bold')
            plt.ylabel('Source Schema (Referencing)', fontsize=12, fontweight='bold')
            plt.title('Schema Cross-Reference Matrix (Excluding Type References)\n(Values show number of references from source to target)',
                      fontsize=14, fontweight='bold', pad=20)

            # Set tick labels using different orders for X and Y axes
            plt.xticks(range(len(sorted_schemas_x)), sorted_schemas_x, rotation=45, ha='right')
            plt.yticks(range(len(sorted_schemas_y)), sorted_schemas_y)

            # Add grid for better readability
            plt.grid(True, alpha=0.3, linewidth=0.5)

            # Adjust the layout to prevent label cutoff
            plt.tight_layout()

            # Save the plot
            filename = f"{timestamp}--schema_references_matrix_no_types.png"
            filepath = os.path.join(self.export_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()  # Close the figure to free memory

            self.logger.info(f"Reference matrix heatmap without type references saved: {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error generating heatmap without type references: {str(e)}")
            return ""

    def plot_reference_types_breakdown(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]], timestamp: str) -> str:
        """
        Generate a stacked bar chart showing reference types breakdown by schema
        """
        self.logger.info("Generating reference types breakdown chart...")

        try:
            # Calculate totals by schema and reference type
            schema_totals = {}
            for source_schema in schemas:
                schema_totals[source_schema] = {}
                for ref_type in self.reference_types:
                    total = sum(
                        detailed_matrix[source_schema][target_schema][ref_type]
                        for target_schema in schemas
                    )
                    schema_totals[source_schema][ref_type] = total

            # Convert to DataFrame for easier plotting
            df_data = []
            for schema in schemas:
                for ref_type in self.reference_types:
                    count = schema_totals[schema][ref_type]
                    if count > 0:  # Only include non-zero values
                        df_data.append({
                            'schema': schema,
                            'reference_type': ref_type.replace('_', ' ').title(),
                            'count': count
                        })

            if not df_data:
                self.logger.warning("No reference type data to plot")
                return ""

            df = pd.DataFrame(df_data)

            # Create pivot table for stacked bar chart
            pivot_df = df.pivot(index='schema', columns='reference_type', values='count').fillna(0)

            # Create the plot
            fig, ax = plt.subplots(figsize=(12, 8))

            # Create a stacked bar chart
            pivot_df.plot(kind='bar', stacked=True, ax=ax,
                          colormap='Set3', alpha=0.8)

            # Customize the plot
            plt.title('Cross-Schema References by Type', fontsize=14, fontweight='bold', pad=20)
            plt.xlabel('Source Schema', fontsize=12, fontweight='bold')
            plt.ylabel('Number of References', fontsize=12, fontweight='bold')
            plt.xticks(rotation=45, ha='right')
            plt.legend(title='Reference Type', bbox_to_anchor=(1.05, 1), loc='upper left')

            # Add value labels on bars
            for container in ax.containers:
                ax.bar_label(container, label_type='center', fontsize=8,
                             fmt='%g', fontweight='bold')

            plt.tight_layout()

            # Save the plot
            filename = f"{timestamp}--schema_references_types.png"
            filepath = os.path.join(self.export_dir, filename)
            plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            self.logger.info(f"Reference types breakdown chart saved: {filepath}")
            return filepath

        except Exception as e:
            self.logger.error(f"Error generating reference types chart: {str(e)}")
            return ""

    def print_detailed_summary(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]], summary_matrix: Dict[str, Dict[str, int]]):
        """
        Print a detailed summary of references to the console
        """
        self.logger.info("\nDetailed Schema Cross-Reference Summary:")
        self.logger.info("=" * 60)

        # Summary by reference type
        type_totals = {}
        for ref_type in self.reference_types:
            total = sum(
                detailed_matrix[source][target][ref_type]
                for source in schemas
                for target in schemas
            )
            type_totals[ref_type] = total
            if total > 0:
                self.logger.info(f"{ref_type.replace('_', ' ').title()}: {total} references")

        self.logger.info("\nTop Schema Dependencies:")
        self.logger.info("-" * 40)

        # Find top dependencies
        dependencies = []
        for source in schemas:
            for target in schemas:
                total_refs = summary_matrix[source][target]
                if total_refs > 0:
                    dependencies.append((source, target, total_refs))

        # Sort by reference count
        dependencies.sort(key=lambda x: x[2], reverse=True)

        for source, target, count in dependencies[:10]:  # Top 10
            self.logger.info(f"{source} -> {target}: {count} references")
            # Show breakdown by type
            for ref_type in self.reference_types:
                type_count = detailed_matrix[source][target][ref_type]
                if type_count > 0:
                    self.logger.info(f"  └─ {ref_type}: {type_count}")

    def run_analysis(self, analysis_datetime: datetime):
        """
        Main method to run the complete schema cross-reference analysis
        """
        self.logger.info("Starting detailed schema cross-reference analysis...")
        start_time = analysis_datetime
        timestamp = start_time.strftime("%Y-%m-%d--%H-%M-%S")

        try:
            # Get non-empty schemas
            schemas = self.get_non_empty_schemas()

            if not schemas:
                self.logger.warning("No schemas found or accessible")
                return

            if len(schemas) < 2:
                self.logger.warning("Need at least 2 schemas for cross-reference analysis")
                return

            # Build reference matrices
            self.logger.info(f"Analyzing cross-references between {len(schemas)} schemas...")
            detailed_matrix, summary_matrix = self.build_detailed_reference_matrix(schemas)

            # Generate all CSV reports
            summary_csv = self.generate_summary_csv(schemas, summary_matrix, timestamp)
            detailed_csv = self.generate_detailed_csv(schemas, detailed_matrix, timestamp)
            type_summary_csv = self.generate_reference_type_summary_csv(schemas, detailed_matrix, timestamp)

            # Generate visualizations
            matrix_plot = self.plot_reference_matrix(schemas, summary_matrix, timestamp)
            matrix_plot_no_types = self.plot_reference_matrix_no_types(schemas, detailed_matrix, timestamp)
            types_plot = self.plot_reference_types_breakdown(schemas, detailed_matrix, timestamp)

            # Print detailed summary
            self.print_detailed_summary(schemas, detailed_matrix, summary_matrix)

            # Final summary
            end_time = datetime.now()
            duration = end_time - start_time

            self.logger.info("\n" + "="*60)
            self.logger.info("DETAILED CROSS-REFERENCE ANALYSIS COMPLETE")
            self.logger.info("="*60)
            self.logger.info(f"Schemas analyzed: {len(schemas)}")
            self.logger.info(f"Total duration: {duration}")
            self.logger.info("\nGenerated reports:")
            self.logger.info(f"1. Summary matrix: {summary_csv}")
            self.logger.info(f"2. Detailed breakdown: {detailed_csv}")
            self.logger.info(f"3. Type summary: {type_summary_csv}")

            if matrix_plot:
                self.logger.info(f"4. Reference matrix heatmap: {matrix_plot}")
            if matrix_plot_no_types:
                self.logger.info(f"5. Reference matrix heatmap (no types): {matrix_plot_no_types}")
            if types_plot:
                self.logger.info(f"6. Reference types breakdown: {types_plot}")

            total_references = sum(
                sum(target_counts.values())
                for target_counts in summary_matrix.values()
            )
            self.logger.info(f"\nTotal cross-schema references: {total_references}")

        except Exception as e:
            self.logger.error(f"Analysis failed: {str(e)}")


def main():
    """
    Main entry point for the database schema cross-reference analyzer
    """
    print("Database Schema Cross-Reference Analyzer with Reference Types & Visualization")
    print("=" * 80)
    print("This tool analyzes references between database schemas including:")
    print("- Table references")
    print("- View dependencies")
    print("- Stored procedure references")
    print("- Function references")
    print("- Alias type references")
    print("- Table type references")
    print("\nGenerates:")
    print("• 3 CSV reports (summary matrix, detailed breakdown, type summary)")
    print("• 2 PNG visualizations (reference matrix heatmap, types breakdown)")
    print()

    # Set analysis datetime once at the start
    analysis_datetime = datetime.now()

    analyzer = DatabaseSchemaReferenceAnalyzer()
    analyzer.run_analysis(analysis_datetime)


if __name__ == "__main__":
    main()
