# Import the module
from database import DatabaseManager, DatabaseConfig

# Use with default configuration
db = DatabaseManager()

# Or with custom configuration
config = DatabaseConfig(server='myserver', database='mydatabase')
db = DatabaseManager(config)

# Execute queries
results = db.execute_query("SELECT * FROM Users WHERE age > ?", (18,))
affected_rows = db.execute_non_query("UPDATE Users SET status = ? WHERE id = ?", ('active', 123))

# Test connection
if db.test_connection():
    print("Connected successfully!")