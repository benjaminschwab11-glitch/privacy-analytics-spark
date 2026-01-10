"""
Load Sample Anonymized Data
Loads a small sample of anonymized users into PostgreSQL for testing
"""

import sys
import os
import json
import psycopg2
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from privacy.pii_detector import PIIDetector, PIIType
from privacy.anonymizer import Anonymizer
from privacy.privacy_config import PrivacyConfig
from database.db_config import DatabaseConfig


class DataLoader:
    """Load anonymized data into PostgreSQL"""
    
    def __init__(self):
        self.anonymizer = Anonymizer(secret_key=os.getenv('SECRET_KEY', 'default-key'))
        self.data_dir = Path(__file__).parent.parent / 'data'
        
    def get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=DatabaseConfig.DB_HOST,
            port=DatabaseConfig.DB_PORT,
            database=DatabaseConfig.DB_NAME,
            user=DatabaseConfig.DB_USER,
            password=DatabaseConfig.DB_PASSWORD
        )
    
    def anonymize_user_record(self, user):
        """
        Anonymize a user record
        
        Args:
            user: User dict from JSON
            
        Returns:
            Anonymized user dict
        """
        return {
            'user_token': self.anonymizer.tokenize(user['user_id'], 'user_id'),
            'email_hash': self.anonymizer.hash_value(user['email']),
            'first_name_hash': self.anonymizer.hash_value(user['first_name']),
            'last_name_hash': self.anonymizer.hash_value(user['last_name']),
            'age_bucket': self.anonymizer.generalize_age(int(user['age'])),
            'gender': user['gender'],
            'region': self.anonymizer.generalize_city(user['city']),
            'state': user['state'],
            'zip_prefix': self.anonymizer.generalize_zip(user['zip_code']),
            'ip_subnet': self.anonymizer.mask_ip(user['ip_address']),
            'phone_suffix': self.anonymizer.mask_phone(user['phone']),
            'created_at': user['created_at'],
            'last_login': user['last_login'],
            'account_status': user['account_status'],
            'subscription_tier': user['subscription_tier'],
            'lifetime_value': float(user['lifetime_value']),
            'total_purchases': int(user['total_purchases'])
        }
    
    def load_sample_users(self, num_records=1000):
        """
        Load sample anonymized users
        
        Args:
            num_records: Number of records to load
        """
        users_file = self.data_dir / 'users.jsonl'
        
        if not users_file.exists():
            print(f"Error: {users_file} not found")
            return
        
        print(f"Loading {num_records:,} sample users...")
        print(f"Source: {users_file}")
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            loaded = 0
            
            with open(users_file, 'r') as f:
                for i, line in enumerate(f):
                    if i >= num_records:
                        break
                    
                    # Parse user
                    user = json.loads(line)
                    
                    # Anonymize
                    anon_user = self.anonymize_user_record(user)
                    
                    # Insert into anonymized_users table
                    cursor.execute('''
                        INSERT INTO anonymized_users (
                            user_token, email_hash, first_name_hash, last_name_hash,
                            age_bucket, gender, region, state, zip_prefix,
                            ip_subnet, phone_suffix, created_at, last_login,
                            account_status, subscription_tier, lifetime_value, total_purchases
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (user_token) DO NOTHING
                    ''', (
                        anon_user['user_token'],
                        anon_user['email_hash'],
                        anon_user['first_name_hash'],
                        anon_user['last_name_hash'],
                        anon_user['age_bucket'],
                        anon_user['gender'],
                        anon_user['region'],
                        anon_user['state'],
                        anon_user['zip_prefix'],
                        anon_user['ip_subnet'],
                        anon_user['phone_suffix'],
                        anon_user['created_at'],
                        anon_user['last_login'],
                        anon_user['account_status'],
                        anon_user['subscription_tier'],
                        anon_user['lifetime_value'],
                        anon_user['total_purchases']
                    ))
                    
                    loaded += 1
                    
                    if loaded % 100 == 0:
                        print(f"  Loaded {loaded:,} users...")
            
            conn.commit()
            
            # Verify
            cursor.execute("SELECT COUNT(*) FROM anonymized_users")
            total = cursor.fetchone()[0]
            
            print(f"\n✓ Successfully loaded {loaded:,} anonymized users")
            print(f"  Total users in database: {total:,}")
            
        except Exception as e:
            conn.rollback()
            print(f"Error loading data: {e}")
            raise
            
        finally:
            cursor.close()
            conn.close()
    
    def show_sample_data(self, limit=5):
        """Show sample anonymized data"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            print(f"\nSample Anonymized Users (first {limit}):")
            print("=" * 80)
            
            cursor.execute(f'''
                SELECT 
                    user_token,
                    age_bucket,
                    gender,
                    region,
                    state,
                    subscription_tier,
                    lifetime_value,
                    total_purchases
                FROM anonymized_users
                LIMIT {limit}
            ''')
            
            for row in cursor.fetchall():
                print(f"\nUser Token: {row[0]}")
                print(f"  Demographics: {row[1]}, {row[2]}, {row[3]}, {row[4]}")
                print(f"  Subscription: {row[5]}")
                print(f"  Lifetime Value: ${row[6]}, Purchases: {row[7]}")
            
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Load sample anonymized data')
    parser.add_argument('--records', type=int, default=1000, help='Number of records to load')
    parser.add_argument('--show', action='store_true', help='Show sample data only')
    
    args = parser.parse_args()
    
    loader = DataLoader()
    
    if args.show:
        loader.show_sample_data(10)
    else:
        print("Privacy-First User Analytics - Data Loader")
        print("=" * 60)
        print()
        
        loader.load_sample_users(args.records)
        loader.show_sample_data(5)
        
        print()
        print("=" * 60)
        print("✓ Data loading complete!")

