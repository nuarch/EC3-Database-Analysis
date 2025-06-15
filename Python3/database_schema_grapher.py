import os
import sqlite3
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import json
from sqlalchemy import create_engine, text, MetaData, inspect
from sqlalchemy.exc import SQLAlchemyError
import warnings
warnings.filterwarnings('ignore')


class DatabaseSchemaGrapher:
    def __init__(self):
        """Initialize the DatabaseSchemaGrapher."""
        self.connection = None
        self.engine = None
        self.metadata = None
        self.inspector = None
        self.tables = []
        self.relationships = []
        self.schemas = {}
        self.statistics = {}
        
    def connect_to_database(self, connection_string: str) -> bool:
        """Connect to database using SQLAlchemy."""
        try:
            self.engine = create_engine(connection_string)
            self.metadata = MetaData()
            self.inspector = inspect(self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print(f"Successfully connected to database")
            return True
            
        except SQLAlchemyError as e:
            print(f"Database connection failed: {e}")
            return False
    
    def analyze_database_schema(self) -> None:
        """Analyze the database schema and extract information."""
        if not self.inspector:
            print("No database connection available")
            return
        
        try:
            # Get all schemas
            schema_names = self.inspector.get_schema_names()
            
            for schema_name in schema_names:
                if schema_name in ['information_schema', 'pg_catalog', 'pg_toast']:
                    continue  # Skip system schemas in PostgreSQL
                
                self.schemas[schema_name] = {
                    'tables': [],
                    'views': [],
                    'relationships': []
                }
                
                # Get tables for this schema
                table_names = self.inspector.get_table_names(schema=schema_name)
                
                for table_name in table_names:
                    table_info = self._analyze_table(table_name, schema_name)
                    self.tables.append(table_info)
                    self.schemas[schema_name]['tables'].append(table_info)
                
                # Get views for this schema
                try:
                    view_names = self.inspector.get_view_names(schema=schema_name)
                    for view_name in view_names:
                        view_info = self._analyze_view(view_name, schema_name)
                        self.schemas[schema_name]['views'].append(view_info)
                except:
                    pass  # Some databases don't support views or method doesn't exist
            
            # Analyze relationships
            self._analyze_relationships()
            
            # Generate statistics
            self._generate_statistics()
            
            print(f"Schema analysis complete. Found {len(self.tables)} tables across {len(self.schemas)} schemas")
            
        except Exception as e:
            print(f"Error during schema analysis: {e}")
    
    def _analyze_table(self, table_name: str, schema_name: str) -> Dict:
        """Analyze a single table and return its information."""
        try:
            columns = self.inspector.get_columns(table_name, schema=schema_name)
            primary_keys = self.inspector.get_pk_constraint(table_name, schema=schema_name)
            foreign_keys = self.inspector.get_foreign_keys(table_name, schema=schema_name)
            indexes = self.inspector.get_indexes(table_name, schema=schema_name)
            
            # Get row count if possible
            row_count = self._get_row_count(table_name, schema_name)
            
            table_info = {
                'name': table_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{table_name}" if schema_name else table_name,
                'type': 'table',
                'columns': columns,
                'column_count': len(columns),
                'primary_keys': primary_keys.get('constrained_columns', []) if primary_keys else [],
                'foreign_keys': foreign_keys,
                'indexes': indexes,
                'row_count': row_count,
                'size_category': self._categorize_table_size(row_count)
            }
            
            return table_info
            
        except Exception as e:
            print(f"Error analyzing table {table_name}: {e}")
            return {
                'name': table_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{table_name}" if schema_name else table_name,
                'type': 'table',
                'columns': [],
                'column_count': 0,
                'primary_keys': [],
                'foreign_keys': [],
                'indexes': [],
                'row_count': 0,
                'size_category': 'unknown'
            }
    
    def _analyze_view(self, view_name: str, schema_name: str) -> Dict:
        """Analyze a single view and return its information."""
        try:
            columns = self.inspector.get_columns(view_name, schema=schema_name)
            
            view_info = {
                'name': view_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{view_name}" if schema_name else view_name,
                'type': 'view',
                'columns': columns,
                'column_count': len(columns)
            }
            
            return view_info
            
        except Exception as e:
            print(f"Error analyzing view {view_name}: {e}")
            return {
                'name': view_name,
                'schema': schema_name,
                'full_name': f"{schema_name}.{view_name}" if schema_name else view_name,
                'type': 'view',
                'columns': [],
                'column_count': 0
            }
    
    def _get_row_count(self, table_name: str, schema_name: str) -> int:
        """Get row count for a table."""
        try:
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
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
    
    def _analyze_relationships(self) -> None:
        """Analyze relationships between tables."""
        self.relationships = []
        
        for table in self.tables:
            for fk in table['foreign_keys']:
                relationship = {
                    'from_table': table['full_name'],
                    'from_columns': fk['constrained_columns'],
                    'to_table': f"{fk.get('referred_schema', table['schema'])}.{fk['referred_table']}",
                    'to_columns': fk['referred_columns'],
                    'constraint_name': fk.get('name', 'unnamed')
                }
                self.relationships.append(relationship)
    
    def _generate_statistics(self) -> None:
        """Generate comprehensive statistics about the database schema."""
        total_tables = len(self.tables)
        total_columns = sum(table['column_count'] for table in self.tables)
        total_relationships = len(self.relationships)
        
        # Schema-level statistics
        schema_stats = {}
        for schema_name, schema_info in self.schemas.items():
            schema_stats[schema_name] = {
                'table_count': len(schema_info['tables']),
                'view_count': len(schema_info['views']),
                'total_columns': sum(table['column_count'] for table in schema_info['tables'])
            }
        
        # Table size distribution
        size_distribution = {}
        for table in self.tables:
            size_cat = table['size_category']
            size_distribution[size_cat] = size_distribution.get(size_cat, 0) + 1
        
        # Most connected tables (by foreign keys)
        table_connections = {}
        for rel in self.relationships:
            from_table = rel['from_table']
            to_table = rel['to_table']
            table_connections[from_table] = table_connections.get(from_table, 0) + 1
            table_connections[to_table] = table_connections.get(to_table, 0) + 1
        
        most_connected = sorted(table_connections.items(), key=lambda x: x[1], reverse=True)[:10]
        
        self.statistics = {
            'total_schemas': len(self.schemas),
            'total_tables': total_tables,
            'total_views': sum(len(schema['views']) for schema in self.schemas.values()),
            'total_columns': total_columns,
            'total_relationships': total_relationships,
            'avg_columns_per_table': total_columns / total_tables if total_tables > 0 else 0,
            'schema_statistics': schema_stats,
            'table_size_distribution': size_distribution,
            'most_connected_tables': most_connected
        }
    
    def load_from_csv(self, csv_path: str) -> bool:
        """Load schema information from a CSV file."""
        try:
            df = pd.read_csv(csv_path)
            
            # Expected columns: schema_name, table_name, column_name, data_type, is_primary_key, is_foreign_key
            required_columns = ['schema_name', 'table_name', 'column_name', 'data_type']
            
            if not all(col in df.columns for col in required_columns):
                print(f"CSV file must contain columns: {required_columns}")
                return False
            
            # Process the data
            self.schemas = {}
            self.tables = []
            
            # Group by schema and table
            for schema_name in df['schema_name'].unique():
                if pd.isna(schema_name):
                    schema_name = 'default'
                
                self.schemas[schema_name] = {'tables': [], 'views': [], 'relationships': []}
                
                schema_data = df[df['schema_name'] == schema_name]
                
                for table_name in schema_data['table_name'].unique():
                    if pd.isna(table_name):
                        continue
                    
                    table_data = schema_data[schema_data['table_name'] == table_name]
                    
                    columns = []
                    primary_keys = []
                    foreign_keys = []
                    
                    for _, row in table_data.iterrows():
                        column_info = {
                            'name': row['column_name'],
                            'type': row['data_type'],
                            'nullable': row.get('is_nullable', True),
                            'default': row.get('default_value', None)
                        }
                        columns.append(column_info)
                        
                        if row.get('is_primary_key', False):
                            primary_keys.append(row['column_name'])
                        
                        if row.get('is_foreign_key', False):
                            foreign_keys.append({
                                'constrained_columns': [row['column_name']],
                                'referred_table': row.get('referenced_table', ''),
                                'referred_columns': [row.get('referenced_column', '')],
                                'referred_schema': row.get('referenced_schema', schema_name)
                            })
                    
                    table_info = {
                        'name': table_name,
                        'schema': schema_name,
                        'full_name': f"{schema_name}.{table_name}",
                        'type': 'table',
                        'columns': columns,
                        'column_count': len(columns),
                        'primary_keys': primary_keys,
                        'foreign_keys': foreign_keys,
                        'indexes': [],
                        'row_count': row.get('row_count', 0),
                        'size_category': 'unknown'
                    }
                    
                    self.tables.append(table_info)
                    self.schemas[schema_name]['tables'].append(table_info)
            
            # Analyze relationships and generate statistics
            self._analyze_relationships()
            self._generate_statistics()
            
            print(f"Successfully loaded schema from CSV: {len(self.tables)} tables")
            return True
            
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return False
    
    def get_statistics(self) -> Dict:
        """Get database statistics."""
        return self.statistics
    
    def get_table_info(self, table_name: str, schema_name: str = None) -> Optional[Dict]:
        """Get detailed information about a specific table."""
        for table in self.tables:
            if table['name'] == table_name and (schema_name is None or table['schema'] == schema_name):
                return table
        return None
    
    def get_relationships(self) -> List[Dict]:
        """Get all relationships in the database."""
        return self.relationships
    
    def create_schema_network_plot(self, schema_name: str = None) -> go.Figure:
        """Create a network plot of the database schema using Plotly."""
        # Filter tables by schema if specified
        if schema_name:
            tables_to_plot = [t for t in self.tables if t['schema'] == schema_name]
            relationships_to_plot = [r for r in self.relationships 
                                   if r['from_table'].startswith(f"{schema_name}.") and 
                                      r['to_table'].startswith(f"{schema_name}.")]
        else:
            tables_to_plot = self.tables
            relationships_to_plot = self.relationships
        
        if not tables_to_plot:
            # Return empty plot if no tables
            fig = go.Figure()
            fig.add_annotation(text="No tables found", x=0.5, y=0.5, showarrow=False)
            return fig
        
        # Create network graph
        G = nx.Graph()
        
        # Add nodes (tables)
        for table in tables_to_plot:
            G.add_node(table['full_name'], 
                      table_info=table,
                      size=table['column_count'])
        
        # Add edges (relationships)
        for rel in relationships_to_plot:
            if rel['from_table'] in G.nodes and rel['to_table'] in G.nodes:
                G.add_edge(rel['from_table'], rel['to_table'])
        
        # Create layout
        if len(G.nodes) > 0:
            pos = nx.spring_layout(G, k=3, iterations=50)
        else:
            pos = {}
        
        # Create traces for edges
        edge_x = []
        edge_y = []
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
        
        # Create traces for nodes
        node_x = []
        node_y = []
        node_text = []
        node_color = []
        node_size = []
        
        for node in G.nodes():
            if node in pos:
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                
                table_info = G.nodes[node]['table_info']
                node_text.append(f"{table_info['name']}<br>Columns: {table_info['column_count']}<br>Rows: {table_info.get('row_count', 'N/A')}")
                
                # Color by size category
                size_colors = {
                    'empty': '#lightgray',
                    'small': '#lightblue',
                    'medium': '#blue',
                    'large': '#darkblue',
                    'very_large': '#red',
                    'unknown': '#gray'
                }
                node_color.append(size_colors.get(table_info.get('size_category', 'unknown'), '#gray'))
                
                # Size by column count
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
        
        # Create figure
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title=f'Database Schema Network - {schema_name or "All Schemas"}',
                           titlefont_size=16,
                           showlegend=False,
                           hovermode='closest',
                           margin=dict(b=20,l=5,r=5,t=40),
                           annotations=[ dict(
                               text="Node size = column count, Color = table size",
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.005, y=-0.002
