"""
Database Configuration
Connection parameters for PostgreSQL
"""

import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    """Database connection configuration"""
    
    # PostgreSQL connection parameters
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'privacy_analytics')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    @classmethod
    def get_connection_string(cls):
        """Get PostgreSQL connection string"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def get_jdbc_url(cls):
        """Get JDBC URL for Spark"""
        return f"jdbc:postgresql://{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def get_connection_properties(cls):
        """Get connection properties for Spark"""
        return {
            'user': cls.DB_USER,
            'password': cls.DB_PASSWORD,
            'driver': 'org.postgresql.Driver'
        }


if __name__ == "__main__":
    print("Database Configuration")
    print("=" * 60)
    print(f"Host: {DatabaseConfig.DB_HOST}")
    print(f"Port: {DatabaseConfig.DB_PORT}")
    print(f"Database: {DatabaseConfig.DB_NAME}")
    print(f"User: {DatabaseConfig.DB_USER}")
    print(f"\nConnection String:")
    print(DatabaseConfig.get_connection_string())
    print(f"\nJDBC URL (for Spark):")
    print(DatabaseConfig.get_jdbc_url())

