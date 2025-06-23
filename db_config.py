# db_config.py
"""
Database configuration file containing sensitive credentials.
This file should be added to .gitignore to prevent checking credentials into git.
"""

# Database connection configuration
DATABASE_CONFIG = {
    'driver': 'ODBC Driver 18 for SQL Server',
    'server': 'localhost',
    'database': 'EC3Database_Analysis',
    'uid': 'sa',
    'pwd': 'Passw0rd*',
    'trust_server_certificate': 'yes'
}