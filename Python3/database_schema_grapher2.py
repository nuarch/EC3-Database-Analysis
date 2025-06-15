import os
import warnings
from datetime import datetime
from typing import Dict, List

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')

try:
    from sqlalchemy import create_engine, text, MetaData, inspect
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    print("SQLAlchemy not available. Some database features will be limited.")


class DatabaseSchemaGrapher:
    """
    A comprehensive tool for analyzing and visualizing database schemas.
    
    This class provides functionality to:
    - Connect to various database systems
    - Analyze database schema structure
    - Generate statistical reports
    - Create network visualizations
    - Export data and diagrams
    """
    
    def __init__(self):
        """Initialize the DatabaseSchemaGrapher."""
        self.connection = None
        self.engine = None
        self.inspector = None
        self.tables = []
        self.relationships = []
        self.statistics = {}
        
    def connect_to_database(self, connection_string: str) -> bool:
        """
        Connect to a database using SQLAlchemy.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            bool: True if the connection is successful, False otherwise
        """
        if not SQLALCHEMY_AVAILABLE:
            print("SQLAlchemy is required for database connections")
            return False
            
        try:
            self.engine = create_engine(connection_string)
            self.inspector = inspect(self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print("Successfully connected to database")
            return True
            
        except Exception as e:
            print(f"Database connection failed: {e}")
            return False
    
    def load_from_csv(self, csv_path: str) -> bool:
        """
        Load schema information from a CSV file.
        
        Args:
            csv_path: Path to CSV file with schema information
            
        Returns:
            bool: True if loading successful, False otherwise
        """
        try:
            df = pd.read_csv(csv_path)
            
            # Required columns for CSV
            required_columns = ['schema_name', 'table_name', 'column_name', 'data_type']
            if not all(col in df.columns for col in required_columns):
                print(f"CSV must contain columns: {required_columns}")
                return False
            
            # Process CSV data into internal format
            self.tables = []
            self.relationships = []
            
            # Group by schema and table
            for schema_name in df['schema_name'].unique():
                if pd.isna(schema_name):
                    schema_name = 'default'
                
                schema_data = df[df['schema_name'] == schema_name]
                
                for table_name in schema_data['table_name'].unique():
                    if pd.isna(table_name):
                        continue
                    
                    table_data = schema_data[schema_data['table_name'] == table_name]
                    table_info = self._process_csv_table_data(table_data, table_name, schema_name)
                    if table_info:
                        self.tables.append(table_info)
            
            # Extract relationships from foreign key information
            self._extract_relationships_from_csv()
            
            # Calculate statistics
            self._calculate_statistics()
            
            print(f"Successfully loaded {len(self.tables)} tables from CSV")
            return True
            
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return False
    
    def _process_csv_table_data(self, table_data: pd.DataFrame, table_name: str, schema_name: str) -> Dict:
        """Process CSV data for a single table."""
        try:
            columns = []
            primary_keys = []
            foreign_keys = []
            
            for _, row in table_data.iterrows():
                column_info = {
                    'name': row['column_name'],
                    'type': row['data_type'],
                    'nullable': row.get('is_nullable', True),
                    'default': row.get('default_value', None),
                    'is_primary_key': row.get('is_primary_key', False),
                    'is_foreign_key': row.get('is_foreign_key', False)
                }
                columns.append(column_info)
                
                if column_info['is_primary_key']:
                    primary_keys.append(column_info['name'])
                
                if column_info['is_foreign_key']:
                    foreign_keys.append({
                        'constrained_columns': [column_info['name']],
                        'referred_table': row.get('referenced_table', ''),
                        'referred_columns': [row.get('referenced_column', '')],
                        'referred_schema': row.get('referenced_schema', schema_name)
                    })
            
            return {
                'name': table_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{table_name}",
                'type': 'table',
                'columns': columns,
                'column_count': len(columns),
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'row_count': table_data.iloc[0].get('row_count', 0) if len(table_data) > 0 else 0,
                'size_category': self._categorize_table_size(table_data.iloc[0].get('row_count', 0) if len(table_data) > 0 else 0)
            }
            
        except Exception as e:
            print(f"Error processing table {table_name}: {e}")
            return None
    
    def _extract_relationships_from_csv(self):
        """Extract relationships from loaded table data."""
        for table in self.tables:
            for fk in table['foreign_keys']:
                relationship = {
                    'from_table': table['full_name'],
                    'from_columns': fk['constrained_columns'],
                    'to_table': f"{fk['referred_schema']}.{fk['referred_table']}",
                    'to_columns': fk['referred_columns']
                }
                self.relationships.append(relationship)
    
    def analyze_database_schema(self) -> bool:
        """
        Analyze the connected database schema.
        
        Returns:
            bool: True if analysis is successful, False otherwise
        """
        if not self.inspector:
            print("No database connection available")
            return False
        
        try:
            self.tables = []
            self.relationships = []
            
            # Get all schemas
            schema_names = self.inspector.get_schema_names()
            
            for schema_name in schema_names:
                # Skip system schemas
                if schema_name.lower() in ['information_schema', 'pg_catalog', 'pg_toast', 'sys', 'mysql']:
                    continue
                
                # Analyze tables
                table_names = self.inspector.get_table_names(schema=schema_name)
                for table_name in table_names:
                    table_info = self._analyze_table(table_name, schema_name)
                    if table_info:
                        self.tables.append(table_info)
                
                # Analyze views
                try:
                    view_names = self.inspector.get_view_names(schema=schema_name)
                    for view_name in view_names:
                        view_info = self._analyze_view(view_name, schema_name)
                        if view_info:
                            self.tables.append(view_info)
                except:
                    pass  # Some databases don't support views
            
            # Analyze relationships
            self._analyze_relationships()
            
            # Calculate statistics
            self._calculate_statistics()
            
            print(f"Schema analysis complete. Found {len(self.tables)} objects")
            return True
            
        except Exception as e:
            print(f"Error during schema analysis: {e}")
            return False
    
    def _analyze_table(self, table_name: str, schema_name: str) -> Dict:
        """Analyze a single table."""
        try:
            columns_data = self.inspector.get_columns(table_name, schema=schema_name)
            primary_keys = self.inspector.get_pk_constraint(table_name, schema=schema_name)
            foreign_keys = self.inspector.get_foreign_keys(table_name, schema=schema_name)
            indexes = self.inspector.get_indexes(table_name, schema=schema_name)
            
            # Process columns
            columns = []
            pk_list = primary_keys.get('constrained_columns', []) if primary_keys else []
            
            for col_data in columns_data:
                column = {
                    'name': col_data['name'],
                    'type': str(col_data.get('type', '')),
                    'nullable': col_data.get('nullable', True),
                    'default': col_data.get('default', None),
                    'is_primary_key': col_data['name'] in pk_list,
                    'is_foreign_key': False  # Will be set when processing foreign keys
                }
                columns.append(column)
            
            # Mark foreign key columns
            for fk in foreign_keys:
                for fk_col in fk['constrained_columns']:
                    for column in columns:
                        if column['name'] == fk_col:
                            column['is_foreign_key'] = True
            
            # Get row count
            row_count = self._get_table_row_count(table_name, schema_name)
            
            return {
                'name': table_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{table_name}",
                'type': 'table',
                'columns': columns,
                'column_count': len(columns),
                'primary_keys': pk_list,
                'foreign_keys': foreign_keys,
                'indexes': indexes,
                'row_count': row_count,
                'size_category': self._categorize_table_size(row_count)
            }
            
        except Exception as e:
            print(f"Error analyzing table {table_name}: {e}")
            return None
    
    def _analyze_view(self, view_name: str, schema_name: str) -> Dict:
        """Analyze a single view."""
        try:
            columns_data = self.inspector.get_columns(view_name, schema=schema_name)
            
            columns = []
            for col_data in columns_data:
                column = {
                    'name': col_data['name'],
                    'type': str(col_data.get('type', '')),
                    'nullable': col_data.get('nullable', True),
                    'default': col_data.get('default', None),
                    'is_primary_key': False,
                    'is_foreign_key': False
                }
                columns.append(column)
            
            return {
                'name': view_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{view_name}",
                'type': 'view',
                'columns': columns,
                'column_count': len(columns),
                'primary_keys': [],
                'foreign_keys': [],
                'indexes': [],
                'row_count': 0,
                'size_category': 'unknown'
            }
            
        except Exception as e:
            print(f"Error analyzing view {view_name}: {e}")
            return None
    
    def _get_table_row_count(self, table_name: str, schema_name: str) -> int:
        """Get the row count for a table."""
        try:
            full_table_name = f'"{schema_name}"."{table_name}"'
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name}"))
                return result.scalar()
        except:
            return 0
    
    def _categorize_table_size(self, row_count: int) -> str:
        """Categorize table size based on row count."""
        if row_count == 0:
            return 'empty'
        elif row_count < 1000:
            return 'small'
        elif row_count < 100000:
            return 'medium'
        elif row_count < 1000000:
            return 'large'
        else:
            return 'very_large'
    
    def _analyze_relationships(self):
        """Analyze relationships between tables."""
        self.relationships = []
        
        for table in self.tables:
            for fk in table['foreign_keys']:
                relationship = {
                    'from_table': table['full_name'],
                    'from_columns': fk['constrained_columns'],
                    'to_table': f"{fk.get('referred_schema', table['schema'])}.{fk['referred_table']}",
                    'to_columns': fk['referred_columns']
                }
                self.relationships.append(relationship)
    
    def _calculate_statistics(self):
        """Calculate database statistics."""
        total_tables = len([t for t in self.tables if t['type'] == 'table'])
        total_views = len([t for t in self.tables if t['type'] == 'view'])
        total_columns = sum(t['column_count'] for t in self.tables)
        total_relationships = len(self.relationships)
        
        # Schema statistics
        schema_stats = {}
        for table in self.tables:
            schema = table['schema']
            if schema not in schema_stats:
                schema_stats[schema] = {
                    'table_count': 0,
                    'view_count': 0,
                    'total_columns': 0
                }
            
            if table['type'] == 'table':
                schema_stats[schema]['table_count'] += 1
            else:
                schema_stats[schema]['view_count'] += 1
            schema_stats[schema]['total_columns'] += table['column_count']
        
        # Table size distribution
        size_distribution = {}
        for table in self.tables:
            if table['type'] == 'table':  # Only count tables, not views
                size_cat = table['size_category']
                size_distribution[size_cat] = size_distribution.get(size_cat, 0) + 1
        
        # Most connected tables
        table_connections = {}
        for rel in self.relationships:
            from_table = rel['from_table']
            to_table = rel['to_table']
            table_connections[from_table] = table_connections.get(from_table, 0) + 1
            table_connections[to_table] = table_connections.get(to_table, 0) + 1
        
        most_connected = sorted(table_connections.items(), key=lambda x: x[1], reverse=True)[:10]
        
        self.statistics = {
            'total_schemas': len(schema_stats),
            'total_tables': total_tables,
            'total_views': total_views,
            'total_columns': total_columns,
            'total_relationships': total_relationships,
            'avg_columns_per_table': total_columns / total_tables if total_tables > 0 else 0,
            'schema_statistics': schema_stats,
            'table_size_distribution': size_distribution,
            'most_connected_tables': most_connected
        }
    
    def get_statistics(self) -> Dict:
        """Get database statistics."""
        return self.statistics
    
    def get_table_info(self, table_name: str, schema_name: str = None) -> Dict:
        """Get detailed information about a specific table."""
        for table in self.tables:
            if table['name'] == table_name and (schema_name is None or table['schema'] == schema_name):
                return table
        return None
    
    def get_relationships(self) -> List[Dict]:
        """Get all relationships."""
        return self.relationships
    
    def create_schema_network_plot(self, schema_name: str = None) -> go.Figure:
        """Create a network plot of the database schema using Plotly."""
        # Filter tables by schema if specified
        filtered_tables = self.tables
        filtered_relationships = self.relationships
        
        if schema_name:
            filtered_tables = [t for t in self.tables if t['schema'] == schema_name]
            filtered_relationships = [r for r in self.relationships 
                                   if r['from_table'].startswith(f"{schema_name}.") and 
                                      r['to_table'].startswith(f"{schema_name}.")]
        
        if not filtered_tables:
            fig = go.Figure()
            fig.add_annotation(text=f"No tables found for schema: {schema_name}", 
                             x=0.5, y=0.5, showarrow=False)
            return fig
        
        # Create a network graph
        G = nx.Graph()
        
        # Add nodes (tables)
        for table in filtered_tables:
            G.add_node(table['full_name'], **table)
        
        # Add edges (relationships)
        for rel in filtered_relationships:
            if rel['from_table'] in G.nodes and rel['to_table'] in G.nodes:
                G.add_edge(rel['from_table'], rel['to_table'])
        
        # Create layout
        pos = nx.spring_layout(G, k=3, iterations=50) if len(G.nodes) > 0 else {}
        
        # Create edge traces
        edge_x, edge_y = [], []
        for edge in G.edges():
            if edge[0] in pos and edge[1] in pos:
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
        
        edge_trace = go.Scatter(x=edge_x, y=edge_y,
                              line=dict(width=0.5, color='#888'),
                              hoverinfo='none',
                              mode='lines')
        
        # Create node traces
        node_x, node_y, node_text, node_color, node_size = [], [], [], [], []
        
        size_colors = {
            'empty': 'lightgray',
            'small': 'lightblue',
            'medium': 'blue',
            'large': 'darkblue',
            'very_large': 'red',
            'unknown': 'gray'
        }
        
        for node in G.nodes():
            if node in pos:
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                
                table_info = G.nodes[node]
                node_text.append(f"{table_info['name']}<br>Columns: {table_info['column_count']}<br>Rows: {table_info['row_count']}")
                node_color.append(size_colors.get(table_info['size_category'], '#gray'))
                node_size.append(max(10, min(50, table_info['column_count'] * 2)))
        
        node_trace = go.Scatter(x=node_x, y=node_y,
                              mode='markers+text',
                              hoverinfo='text',
                              text=[t.split('<br>')[0] for t in node_text],
                              hovertext=node_text,
                              textposition="middle center",
                              marker=dict(showscale=False,
                                        color=node_color,
                                        size=node_size,
                                        line=dict(width=2, color='black')))
        
        # Create a figure
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                            title=f'Database Schema Network - {schema_name or "All Schemas"}',
                            titlefont_size=16,
                            showlegend=False,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            annotations=[dict(
                                text="Node size = column count, Color = table size",
                                showarrow=False,
                                xref="paper", yref="paper",
                                x=0.005, y=-0.002,
                                xanchor="left", yanchor="bottom",
                                font=dict(color="gray", size=12)
                            )],
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
        
        return fig

def export_schema_plot(self, schema_name: str = None, output_path: str = None) -> str:
    """Export schema plot as PNG using matplotlib."""
    # Filter tables by schema if specified
    filtered_tables = self.tables
    filtered_relationships = self.relationships

    if schema_name:
        filtered_tables = [t for t in self.tables if t['schema'] == schema_name]
        filtered_relationships = [r for r in self.relationships
                                  if r['from_table'].startswith(f"{schema_name}.") and
                                  r['to_table'].startswith(f"{schema_name}.")]

    # Generate filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        schema_suffix = f"_{schema_name}" if schema_name else "_all_schemas"
        output_path = f"schema_diagram{schema_suffix}_{timestamp}.png"

    # Ensure directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    def escape_mathtext(text):
        """Escape special characters that could be interpreted as mathtext."""
        if not isinstance(text, str):
            return str(text)
        # Escape characters that have special meaning in matplotlib mathtext
        special_chars = ['$', '_', '^', '{', '}', '\\']
        escaped_text = text
        for char in special_chars:
            escaped_text = escaped_text.replace(char, f'\\{char}')
        return escaped_text

    if not filtered_tables:
        # Create an empty plot
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.text(0.5, 0.5, f'No tables found for schema: {schema_name}',
                ha='center', va='center', transform=ax.transAxes, fontsize=16)
        ax.set_title(f'Database Schema Network - {schema_name or "All Schemas"}')
        ax.axis('off')
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()

        abs_path = os.path.abspath(output_path)
        print(f"Schema diagram exported to: {abs_path}")
        return abs_path

    # Create a network graph
    G = nx.Graph()

    # Add nodes and edges
    for table in filtered_tables:
        G.add_node(table['full_name'], **table)

    for rel in filtered_relationships:
        if rel['from_table'] in G.nodes and rel['to_table'] in G.nodes:
            G.add_edge(rel['from_table'], rel['to_table'])

    # Create the plot
    fig, ax = plt.subplots(figsize=(15, 10))

    # Create layout
    pos = nx.spring_layout(G, k=3, iterations=50)

    # Draw edges
    nx.draw_networkx_edges(G, pos, ax=ax, edge_color='gray', alpha=0.6)

    # Prepare node colors and sizes based on table properties
    size_color_map = {
        'empty': 'lightgray',
        'small': '#87CEEB',
        'medium': '#4169E1',
        'large': '#191970',
        'very_large': '#FF0000',
        'unknown': '#808080'
    }

    node_colors = []
    node_sizes = []

    for node in G.nodes():
        table_info = G.nodes[node]
        size_category = table_info['size_category']
        node_colors.append(size_color_map.get(size_category, '#808080'))
        node_sizes.append(max(300, min(2000, table_info['column_count'] * 50)))

    # Draw nodes
    nx.draw_networkx_nodes(G, pos, ax=ax,
                           node_color=node_colors,
                           node_size=node_sizes,
                           alpha=0.8,
                           edgecolors='black',
                           linewidths=1)

    # Draw labels (use just table name, not full qualified name) with escaped characters
    labels = {node: escape_mathtext(node.split('.')[-1]) for node in G.nodes()}
    nx.draw_networkx_labels(G, pos, labels, ax=ax, font_size=8, font_weight='bold')

    # Set title and formatting - also escape the schema name
    title = f'Database Schema Network - {escape_mathtext(schema_name) if schema_name else "All Schemas"}'
    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.axis('off')

    # Add legend
    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w',
                   markerfacecolor=color, markersize=10,
                   label=size.replace('_', ' ').title())
        for size, color in size_color_map.items()
    ]
    ax.legend(handles=legend_elements, loc='upper right', title='Table Size')

    # Add a note about node size
    fig.text(0.02, 0.02, 'Node size represents column count',
             fontsize=10, style='italic', color='gray')

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    abs_path = os.path.abspath(output_path)
    print(f"Schema diagram exported to: {abs_path}")
    return abs_path
    
    def export_database_statistics_csv(self, output_path: str = None) -> str:
        """Export database statistics to CSV."""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"database_statistics_{timestamp}.csv"
        
        # Ensure directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        stats_data = []
        
        # Overall statistics
        overall_stats = [
            ('Overall', 'Total Schemas', self.statistics.get('total_schemas', 0), 'Total number of database schemas'),
            ('Overall', 'Total Tables', self.statistics.get('total_tables', 0), 'Total number of tables'),
            ('Overall', 'Total Views', self.statistics.get('total_views', 0), 'Total number of views'),
            ('Overall', 'Total Columns', self.statistics.get('total_columns', 0), 'Total number of columns'),
            ('Overall', 'Total Relationships', self.statistics.get('total_relationships', 0), 'Total foreign key relationships'),
            ('Overall', 'Average Columns per Table', round(self.statistics.get('avg_columns_per_table', 0), 2), 'Average columns per table')
        ]
        
        for category, metric, value, description in overall_stats:
            stats_data.append({
                'Category': category,
                'Metric': metric,
                'Value': value,
                'Description': description
            })
        
        # Schema breakdown
        schema_stats = self.statistics.get('schema_statistics', {})
        for schema_name, stats in schema_stats.items():
            for metric, value in stats.items():
                stats_data.append({
                    'Category': f'Schema: {schema_name}',
                    'Metric': metric.replace('_', ' ').title(),
                    'Value': value,
                    'Description': f'{metric.replace("_", " ").title()} in schema {schema_name}'
                })
        
        # Size distribution
        size_distribution = self.statistics.get('table_size_distribution', {})
        for size_category, count in size_distribution.items():
            stats_data.append({
                'Category': 'Table Size Distribution',
                'Metric': f'{size_category.replace("_", " ").title()} Tables',
                'Value': count,
                'Description': f'Number of {size_category.replace("_", " ")} tables'
            })
        
        # Export to CSV
        df = pd.DataFrame(stats_data)
        df.to_csv(output_path, index=False)
        
        abs_path = os.path.abspath(output_path)
        print(f"Statistics exported to: {abs_path}")
        return abs_path
    
    def export_object_details_csv(self, output_path: str = None) -> str:
        """Export detailed object information to CSV."""
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"database_object_details_{timestamp}.csv"
        
        # Ensure directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # Initialize the details_data list here - THIS WAS THE MISSING LINE
        details_data = []
        
        for table in self.tables:
            # Table-level information
            details_data.append({
                'Object_Type': table['type'].title(),
                'Schema_Name': table['schema'],
                'Object_Name': table['name'],
                'Full_Name': table['full_name'],
                'Column_Name': '',
                'Data_Type': '',
                'Is_Primary_Key': '',
                'Is_Foreign_Key': '',
                'Column_Count': table['column_count'],
                'Row_Count': table['row_count'],
                'Size_Category': table['size_category'],
                'Referenced_Table': '',
                'Referenced_Column': ''
            })
            
            # Column-level information
            for column in table['columns']:
                # Find foreign key info for this column
                referenced_table = ''
                referenced_column = ''
                
                for fk in table['foreign_keys']:
                    if column['name'] in fk['constrained_columns']:
                        referenced_table = fk['referred_table']
                        col_index = fk['constrained_columns'].index(column['name'])
                        if col_index < len(fk['referred_columns']):
                            referenced_column = fk['referred_columns'][col_index]
                        break
                
                details_data.append({
                    'Object_Type': f'{table["type"].title()} Column',
                    'Schema_Name': table['schema'],
                    'Object_Name': table['name'],
                    'Full_Name': table['full_name'],
                    'Column_Name': column['name'],
                    'Data_Type': column['type'],
                    'Is_Primary_Key': column['is_primary_key'],
                    'Is_Foreign_Key': column['is_foreign_key'],
                    'Column_Count': '',
                    'Row_Count': '',
                    'Size_Category': '',
                    'Referenced_Table': referenced_table,
                    'Referenced_Column': referenced_column
                })
        
        # Export to CSV
        df = pd.DataFrame(details_data)
        df.to_csv(output_path, index=False)
        
        abs_path = os.path.abspath(output_path)
        print(f"Object details exported to: {abs_path}")
        return abs_path
    
    def export_all_statistics(self, base_path: str = None) -> Dict:
        """Export all statistics and diagrams."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if base_path is None:
            base_path = f"database_export_{timestamp}"
        
        # Ensure directory exists
        if not os.path.exists(base_path):
            os.makedirs(base_path, exist_ok=True)
        
        # Export files
        stats_path = self.export_database_statistics_csv(
            os.path.join(base_path, f"statistics_{timestamp}.csv")
        )
        details_path = self.export_object_details_csv(
            os.path.join(base_path, f"object_details_{timestamp}.csv")
        )
        diagram_path = self.export_schema_plot(
            output_path=os.path.join(base_path, f"schema_diagram_{timestamp}.png")
        )
        
        export_info = {
            'statistics_csv': stats_path,
            'object_details_csv': details_path,
            'schema_diagram_png': diagram_path,
            'export_directory': os.path.abspath(base_path)
        }
        
        print(f"\n=== Database Export Complete ===")
        print(f"Export directory: {export_info['export_directory']}")
        print(f"Statistics CSV: {os.path.basename(stats_path)}")
        print(f"Object Details CSV: {os.path.basename(details_path)}")
        print(f"Schema Diagram PNG: {os.path.basename(diagram_path)}")
        
        return export_info
    
    def create_statistics_dashboard(self) -> go.Figure:
        """Create a comprehensive statistics dashboard."""
        if not self.statistics:
            fig = go.Figure()
            fig.add_annotation(text="No statistics available", x=0.5, y=0.5, showarrow=False)
            return fig
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Table Size Distribution', 'Columns per Schema', 
                          'Most Connected Tables', 'Schema Overview'),
            specs=[[{"type": "pie"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "table"}]]
        )
        
        # 1. Table Size Distribution (Pie Chart)
        size_dist = self.statistics.get('table_size_distribution', {})
        if size_dist:
            fig.add_trace(
                go.Pie(labels=list(size_dist.keys()), 
                      values=list(size_dist.values()),
                      name="Size Distribution"),
                row=1, col=1
            )
        
        # 2. Columns per Schema (Bar Chart)
        schema_stats = self.statistics.get('schema_statistics', {})
        if schema_stats:
            schemas = list(schema_stats.keys())
            columns = [stats['total_columns'] for stats in schema_stats.values()]
            
            fig.add_trace(
                go.Bar(x=schemas, y=columns, name="Columns per Schema"),
                row=1, col=2
            )
        
        # 3. Most Connected Tables (Bar Chart) - THIS WAS THE PROBLEMATIC LINE
        most_connected = self.statistics.get('most_connected_tables', [])
        if most_connected and len(most_connected) > 0:
            # Take only the first 5 items
            top_connected = most_connected[:5]
            tables = [item[0].split('.')[-1] for item in top_connected]  # Just a table name
            connections = [item[1] for item in top_connected]
            
            fig.add_trace(
                go.Bar(x=tables, y=connections, name="Connections"),
                row=2, col=1
            )
        
        # 4. Schema Overview (Table)
        if schema_stats:
            schema_names = list(schema_stats.keys())
            table_counts = [stats['table_count'] for stats in schema_stats.values()]
            view_counts = [stats['view_count'] for stats in schema_stats.values()]
            column_counts = [stats['total_columns'] for stats in schema_stats.values()]
            
            fig.add_trace(
                go.Table(
                    header=dict(values=['Schema', 'Tables', 'Views', 'Columns']),
                    cells=dict(values=[schema_names, table_counts, view_counts, column_counts])
                ),
                row=2, col=2
            )
        
        fig.update_layout(height=800, showlegend=False, title_text="Database Schema Statistics Dashboard")
        return fig
    
    def close_connection(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            print("Database connection closed")


def create_sql_server_connection_string():
    """Create a connection string for SQL Server using your existing config."""
    # Based on your DB_CONFIG from the other files
    config = {
        'driver': 'ODBC Driver 18 for SQL Server',
        'server': 'localhost',
        'database': 'EC3Database_Analysis',
        'uid': 'sa',
        'pwd': 'Passw0rd*',
        'TrustServerCertificate': 'yes'
    }

    # Convert to SQLAlchemy connection string
    connection_string = (
        f"mssql+pyodbc://{config['uid']}:{config['pwd']}@{config['server']}/"
        f"{config['database']}?driver={config['driver'].replace(' ', '+')}"
        f"&TrustServerCertificate={config['TrustServerCertificate']}"
    )

    return connection_string


# Main
if __name__ == "__main__":
    grapher = DatabaseSchemaGrapher()
    connection_string = create_sql_server_connection_string()

    if grapher.connect_to_database(connection_string):
        # Analyze the database schema
        if grapher.analyze_database_schema():
            # Get and display statistics
            stats = grapher.get_statistics()
            print("Database Statistics:")
            for key, value in stats.items():
                print(f"  {key}: {value}")

            # Export statistics to a file (simple implementation)
            try:
                with open("database_statistics.txt", "w") as f:
                    f.write("Database Statistics:\n")
                    for key, value in stats.items():
                        f.write(f"  {key}: {value}\n")
                print(f"\nStatistics exported to database_statistics.txt")
            except Exception as e:
                print(f"Error exporting statistics: {e}")
        else:
            print("Failed to analyze database schema")
    else:
        print("Failed to connect to database")

    # Clean up - handle missing close_connection method gracefully
    try:
        if hasattr(grapher, 'close_connection'):
            grapher.close_connection()
        elif hasattr(grapher, 'disconnect'):
            grapher.disconnect()
        elif hasattr(grapher, 'close'):
            grapher.close()
        else:
            print("No explicit connection cleanup method available")
    except Exception as e:
        print(f"Error during connection cleanup: {e}")
