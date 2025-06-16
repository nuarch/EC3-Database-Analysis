import os
import csv
import logging
from datetime import datetime
from typing import Dict, List, Set, Tuple, NamedTuple
import sys
from collections import defaultdict

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
            'foreign_keys',
            'view_dependencies', 
            'procedure_references',
            'function_references',
            'trigger_references'
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
        Get list of non-empty schemas using the database connection utility
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
    
    def get_foreign_key_references(self) -> List[SchemaReference]:
        """
        Get foreign key references between schemas with detailed information
        """
        self.logger.info("Analyzing foreign key references...")
        
        try:
            # SQL Server specific query for foreign key references across schemas
            fk_query = """
            SELECT 
                s1.name as source_schema,
                s2.name as target_schema,
                COUNT(*) as reference_count
            FROM sys.foreign_keys fk
            JOIN sys.tables t1 ON fk.parent_object_id = t1.object_id
            JOIN sys.schemas s1 ON t1.schema_id = s1.schema_id
            JOIN sys.tables t2 ON fk.referenced_object_id = t2.object_id
            JOIN sys.schemas s2 ON t2.schema_id = s2.schema_id
            WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            GROUP BY s1.name, s2.name
            """
            
            result = self.db_manager.execute_query(fk_query)
            references = [
                SchemaReference(row[0], row[1], 'foreign_keys', row[2]) 
                for row in result
            ]
            
            self.logger.info(f"Found {len(references)} foreign key cross-schema reference groups")
            return references
            
        except Exception as e:
            self.logger.error(f"Error getting foreign key references: {str(e)}")
            return []
    
    def get_view_references(self) -> List[SchemaReference]:
        """
        Get view references to tables in other schemas
        """
        self.logger.info("Analyzing view references...")
        
        try:
            # SQL Server specific query for views referencing tables in other schemas
            view_query = """
            SELECT 
                s1.name as view_schema,
                s2.name as referenced_schema,
                COUNT(DISTINCT v.object_id) as view_count
            FROM sys.views v
            JOIN sys.schemas s1 ON v.schema_id = s1.schema_id
            JOIN sys.sql_dependencies d ON v.object_id = d.object_id
            JOIN sys.objects o ON d.referenced_major_id = o.object_id
            JOIN sys.schemas s2 ON o.schema_id = s2.schema_id
            WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND o.type IN ('U', 'V')  -- Tables and Views
            GROUP BY s1.name, s2.name
            """
            
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
        Get stored procedure references to other schemas
        """
        self.logger.info("Analyzing stored procedure references...")
        
        try:
            # SQL Server specific query for stored procedures referencing other schemas
            procedure_query = """
            SELECT 
                s1.name as procedure_schema,
                s2.name as referenced_schema,
                COUNT(DISTINCT p.object_id) as procedure_count
            FROM sys.procedures p
            JOIN sys.schemas s1 ON p.schema_id = s1.schema_id
            JOIN sys.sql_dependencies d ON p.object_id = d.object_id
            JOIN sys.objects o ON d.referenced_major_id = o.object_id
            JOIN sys.schemas s2 ON o.schema_id = s2.schema_id
            WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND o.type IN ('U', 'V', 'P', 'FN')  -- Tables, Views, Procedures, Functions
            GROUP BY s1.name, s2.name
            """
            
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
        Get function references to other schemas
        """
        self.logger.info("Analyzing function references...")
        
        try:
            # SQL Server specific query for functions referencing other schemas
            function_query = """
            SELECT 
                s1.name as function_schema,
                s2.name as referenced_schema,
                COUNT(DISTINCT f.object_id) as function_count
            FROM sys.objects f
            JOIN sys.schemas s1 ON f.schema_id = s1.schema_id
            JOIN sys.sql_dependencies d ON f.object_id = d.object_id
            JOIN sys.objects o ON d.referenced_major_id = o.object_id
            JOIN sys.schemas s2 ON o.schema_id = s2.schema_id
            WHERE f.type IN ('FN', 'TF', 'IF')  -- Scalar, Table-valued, Inline functions
            AND s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND o.type IN ('U', 'V', 'P', 'FN')  -- Tables, Views, Procedures, Functions
            GROUP BY s1.name, s2.name
            """
            
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
    
    def get_trigger_references(self) -> List[SchemaReference]:
        """
        Get trigger references to other schemas
        """
        self.logger.info("Analyzing trigger references...")
        
        try:
            # SQL Server specific query for triggers referencing objects in other schemas
            trigger_query = """
            SELECT 
                s1.name as trigger_schema,
                s2.name as referenced_schema,
                COUNT(DISTINCT tr.object_id) as trigger_count
            FROM sys.triggers tr
            JOIN sys.objects parent ON tr.parent_id = parent.object_id
            JOIN sys.schemas s1 ON parent.schema_id = s1.schema_id
            JOIN sys.sql_dependencies d ON tr.object_id = d.object_id
            JOIN sys.objects o ON d.referenced_major_id = o.object_id
            JOIN sys.schemas s2 ON o.schema_id = s2.schema_id
            WHERE s1.name != s2.name
            AND s1.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND s2.name NOT IN ('sys', 'INFORMATION_SCHEMA')
            AND o.type IN ('U', 'V', 'P', 'FN')  -- Tables, Views, Procedures, Functions
            GROUP BY s1.name, s2.name
            """
            
            result = self.db_manager.execute_query(trigger_query)
            references = [
                SchemaReference(row[0], row[1], 'trigger_references', row[2]) 
                for row in result
            ]
            
            self.logger.info(f"Found {len(references)} trigger cross-schema references")
            return references
            
        except Exception as e:
            self.logger.error(f"Error getting trigger references: {str(e)}")
            return []
    
    def build_detailed_reference_matrix(self, schemas: List[str]) -> Tuple[Dict[str, Dict[str, Dict[str, int]]], Dict[str, Dict[str, int]]]:
        """
        Build detailed cross-reference matrix with reference types
        Returns: (detailed_matrix, summary_matrix)
        """
        self.logger.info("Building detailed schema reference matrix...")
        
        # Initialize detailed matrix: source -> target -> reference_type -> count
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
        all_references.extend(self.get_foreign_key_references())
        all_references.extend(self.get_view_references())
        all_references.extend(self.get_stored_procedure_references())
        all_references.extend(self.get_function_references())
        all_references.extend(self.get_trigger_references())
        
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
    
    def generate_summary_csv(self, schemas: List[str], summary_matrix: Dict[str, Dict[str, int]]) -> str:
        """
        Generate summary CSV with total references between schemas
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"schema_references_summary_{timestamp}.csv"
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
    
    def generate_detailed_csv(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]]) -> str:
        """
        Generate detailed CSV with reference types breakdown
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"schema_references_detailed_{timestamp}.csv"
        filepath = os.path.join(self.export_dir, filename)
        
        self.logger.info(f"Generating detailed CSV: {filepath}")
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                # Create fieldnames: source_schema, target_schema, then each reference type
                fieldnames = ['source_schema', 'target_schema'] + self.reference_types + ['total_references']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write detailed breakdown for each schema pair
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
    
    def generate_reference_type_summary_csv(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]]) -> str:
        """
        Generate CSV summarizing references by type across all schemas
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"schema_references_by_type_{timestamp}.csv"
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
    
    def print_detailed_summary(self, schemas: List[str], detailed_matrix: Dict[str, Dict[str, Dict[str, int]]], summary_matrix: Dict[str, Dict[str, int]]):
        """
        Print detailed summary of references to console
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
    
    def run_analysis(self):
        """
        Main method to run the complete schema cross-reference analysis
        """
        self.logger.info("Starting detailed schema cross-reference analysis...")
        start_time = datetime.now()
        
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
            summary_csv = self.generate_summary_csv(schemas, summary_matrix)
            detailed_csv = self.generate_detailed_csv(schemas, detailed_matrix)
            type_summary_csv = self.generate_reference_type_summary_csv(schemas, detailed_matrix)
            
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
    print("Database Schema Cross-Reference Analyzer with Reference Types")
    print("=" * 70)
    print("This tool analyzes references between database schemas including:")
    print("- Foreign key constraints")
    print("- View dependencies")
    print("- Stored procedure references")
    print("- Function references")
    print("- Trigger references")
    print("\nGenerates 3 CSV reports:")
    print("1. Summary matrix (schema-to-schema totals)")
    print("2. Detailed breakdown (with reference types)")
    print("3. Reference type summary (totals by type)")
    print()
    
    analyzer = DatabaseSchemaReferenceAnalyzer()
    analyzer.run_analysis()


if __name__ == "__main__":
    main()
