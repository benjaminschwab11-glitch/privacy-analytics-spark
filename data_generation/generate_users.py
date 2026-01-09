"""
Synthetic User Data Generator
Creates realistic user profiles with PII for privacy framework testing
"""

from faker import Faker
import json
import csv
from datetime import datetime, timedelta
import random
from pathlib import Path
from tqdm import tqdm

fake = Faker()
Faker.seed(42)  # Reproducible data
random.seed(42)


class UserDataGenerator:
    """Generate synthetic user data with realistic PII"""
    
    def __init__(self, num_users=100000):
        """
        Initialize generator
        
        Args:
            num_users: Number of user records to generate
        """
        self.num_users = num_users
        self.output_dir = Path(__file__).parent.parent / 'data'
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_user(self, user_id):
        """
        Generate single user profile
        
        Args:
            user_id: Unique user identifier
            
        Returns:
            User profile dict
        """
        # Demographics
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # Age distribution (realistic)
        age = random.choices(
            population=range(18, 80),
            weights=[2 if 25 <= a <= 54 else 1 for a in range(18, 80)]
        )[0]
        
        # Location (weighted toward major cities)
        cities = [
            'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
            'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
            'Austin', 'Seattle', 'Denver', 'Portland', 'Boston'
        ]
        city = random.choice(cities)
        
        # Generate user profile
        user = {
            'user_id': f'user_{user_id:06d}',
            'email': f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{fake.free_email_domain()}",
            'first_name': first_name,
            'last_name': last_name,
            'age': age,
            'gender': random.choice(['M', 'F', 'O', 'N']),
            'city': city,
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'ip_address': fake.ipv4(),
            'phone': fake.phone_number(),
            'created_at': fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
            'last_login': fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
            'account_status': random.choices(['active', 'inactive', 'suspended'], weights=[85, 10, 5])[0],
            'subscription_tier': random.choices(['free', 'basic', 'premium'], weights=[50, 30, 20])[0],
            'lifetime_value': round(random.uniform(0, 1000), 2),
            'total_purchases': random.randint(0, 50)
        }
        
        return user
    
    def generate_users_csv(self):
        """Generate users and save to CSV"""
        output_file = self.output_dir / 'users.csv'
        
        print(f"Generating {self.num_users:,} user records...")
        
        # Write CSV
        with open(output_file, 'w', newline='') as f:
            fieldnames = [
                'user_id', 'email', 'first_name', 'last_name', 'age', 'gender',
                'city', 'state', 'zip_code', 'ip_address', 'phone',
                'created_at', 'last_login', 'account_status', 
                'subscription_tier', 'lifetime_value', 'total_purchases'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for i in tqdm(range(1, self.num_users + 1)):
                user = self.generate_user(i)
                writer.writerow(user)
        
        print(f"✓ Users saved to {output_file}")
        print(f"  File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        
        return output_file
    
    def generate_users_json(self, chunk_size=10000):
        """Generate users and save to JSON Lines format (for Spark)"""
        output_file = self.output_dir / 'users.jsonl'
        
        print(f"Generating {self.num_users:,} user records (JSON Lines)...")
        
        with open(output_file, 'w') as f:
            for i in tqdm(range(1, self.num_users + 1)):
                user = self.generate_user(i)
                f.write(json.dumps(user) + '\n')
        
        print(f"✓ Users saved to {output_file}")
        print(f"  File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        
        return output_file
    
    def generate_sample_preview(self, n=10):
        """Generate sample users for preview"""
        print(f"\nSample Users (first {n}):")
        print("=" * 80)
        
        for i in range(1, n + 1):
            user = self.generate_user(i)
            print(f"\nUser {i}:")
            print(f"  ID: {user['user_id']}")
            print(f"  Name: {user['first_name']} {user['last_name']}")
            print(f"  Email: {user['email']}")
            print(f"  Age: {user['age']}, Gender: {user['gender']}")
            print(f"  Location: {user['city']}, {user['state']}")
            print(f"  IP: {user['ip_address']}")
            print(f"  Phone: {user['phone']}")
            print(f"  Status: {user['account_status']}, Tier: {user['subscription_tier']}")
            print(f"  LTV: ${user['lifetime_value']}, Purchases: {user['total_purchases']}")


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate synthetic user data')
    parser.add_argument('--users', type=int, default=100000, help='Number of users to generate')
    parser.add_argument('--preview', action='store_true', help='Show sample preview only')
    parser.add_argument('--format', choices=['csv', 'json', 'both'], default='both', help='Output format')
    
    args = parser.parse_args()
    
    generator = UserDataGenerator(num_users=args.users)
    
    if args.preview:
        generator.generate_sample_preview(10)
    else:
        print(f"Privacy-First User Analytics - Data Generation")
        print(f"=" * 60)
        print(f"Generating {args.users:,} synthetic user records...")
        print()
        
        if args.format in ['csv', 'both']:
            generator.generate_users_csv()
        
        if args.format in ['json', 'both']:
            generator.generate_users_json()
        
        print()
        print("=" * 60)
        print("✓ Data generation complete!")
        print()
        print("Next steps:")
        print("  1. Review data in data/ directory")
        print("  2. Run Spark job to process users")
        print("  3. Apply privacy transformations")

