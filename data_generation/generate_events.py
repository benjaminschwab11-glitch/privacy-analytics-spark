"""
Clickstream Event Generator
Creates realistic user behavior events for analytics
"""

from faker import Faker
import json
import csv
from datetime import datetime, timedelta
import random
from pathlib import Path
from tqdm import tqdm

fake = Faker()
Faker.seed(42)
random.seed(42)


class EventGenerator:
    """Generate synthetic clickstream events"""
    
    # Event types and their relative frequencies
    EVENT_TYPES = {
        'page_view': 50,
        'click': 20,
        'search': 10,
        'add_to_cart': 8,
        'purchase': 2,
        'login': 5,
        'logout': 3,
        'signup': 1,
        'video_play': 5,
        'video_complete': 2
    }
    
    # Pages users visit
    PAGES = [
        '/home', '/products', '/category/electronics', '/category/clothing',
        '/product/item-123', '/product/item-456', '/cart', '/checkout',
        '/account', '/search', '/about', '/contact'
    ]
    
    def __init__(self, num_users=100000, events_per_user_avg=50):
        """
        Initialize event generator
        
        Args:
            num_users: Number of users
            events_per_user_avg: Average events per user
        """
        self.num_users = num_users
        self.events_per_user_avg = events_per_user_avg
        self.total_events = num_users * events_per_user_avg
        self.output_dir = Path(__file__).parent.parent / 'data'
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_event(self, event_id, user_id, timestamp):
        """
        Generate single event
        
        Args:
            event_id: Unique event identifier
            user_id: User who triggered event
            timestamp: Event timestamp
            
        Returns:
            Event dict
        """
        # Pick event type (weighted)
        event_type = random.choices(
            population=list(self.EVENT_TYPES.keys()),
            weights=list(self.EVENT_TYPES.values())
        )[0]
        
        # Generate event
        event = {
            'event_id': f'evt_{event_id:09d}',
            'user_id': f'user_{user_id:06d}',
            'event_type': event_type,
            'timestamp': timestamp.isoformat(),
            'page_url': random.choice(self.PAGES),
            'referrer': random.choice([None, 'google.com', 'facebook.com', 'twitter.com', None]),
            'device_type': random.choice(['desktop', 'mobile', 'tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
            'session_id': fake.uuid4(),
            'ip_address': fake.ipv4(),
        }
        
        # Event-specific fields
        if event_type == 'search':
            event['search_query'] = fake.word()
        
        elif event_type in ['add_to_cart', 'purchase']:
            event['product_id'] = f'prod_{random.randint(1, 10000):05d}'
            event['quantity'] = random.randint(1, 5)
            event['price'] = round(random.uniform(10, 500), 2)
        
        elif event_type in ['video_play', 'video_complete']:
            event['video_id'] = f'vid_{random.randint(1, 1000):04d}'
            event['duration_seconds'] = random.randint(30, 3600)
        
        return event
    
    def generate_user_session(self, user_id, session_start):
        """
        Generate realistic session of events for a user
        
        Args:
            user_id: User identifier
            session_start: Session start time
            
        Returns:
            List of events
        """
        # Session length (2-30 events)
        num_events = random.randint(2, 30)
        
        events = []
        current_time = session_start
        
        for i in range(num_events):
            event_id = len(events) + 1
            event = self.generate_event(event_id, user_id, current_time)
            events.append(event)
            
            # Time between events (5 seconds to 5 minutes)
            current_time += timedelta(seconds=random.randint(5, 300))
        
        return events
    
    def generate_events_jsonl(self):
        """Generate events and save to JSON Lines"""
        output_file = self.output_dir / 'events.jsonl'
        
        print(f"Generating ~{self.total_events:,} events for {self.num_users:,} users...")
        
        event_count = 0
        
        with open(output_file, 'w') as f:
            # Generate sessions for each user
            for user_id in tqdm(range(1, self.num_users + 1)):
                # Users have 1-5 sessions over past 30 days
                num_sessions = random.randint(1, 5)
                
                for session in range(num_sessions):
                    session_start = fake.date_time_between(start_date='-30d', end_date='now')
                    events = self.generate_user_session(user_id, session_start)
                    
                    for event in events:
                        f.write(json.dumps(event) + '\n')
                        event_count += 1
        
        print(f"✓ Events saved to {output_file}")
        print(f"  Total events: {event_count:,}")
        print(f"  File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        
        return output_file
    
    def generate_sample_preview(self, n=10):
        """Generate sample events for preview"""
        print(f"\nSample Events (first {n}):")
        print("=" * 80)
        
        session_start = datetime.now() - timedelta(hours=2)
        events = self.generate_user_session(1, session_start)[:n]
        
        for i, event in enumerate(events, 1):
            print(f"\nEvent {i}:")
            print(f"  ID: {event['event_id']}")
            print(f"  User: {event['user_id']}")
            print(f"  Type: {event['event_type']}")
            print(f"  Page: {event['page_url']}")
            print(f"  Time: {event['timestamp']}")
            print(f"  Device: {event['device_type']}, Browser: {event['browser']}")
            if 'search_query' in event:
                print(f"  Search: {event['search_query']}")
            if 'product_id' in event:
                print(f"  Product: {event['product_id']}, Price: ${event['price']}")


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate synthetic event data')
    parser.add_argument('--users', type=int, default=100000, help='Number of users')
    parser.add_argument('--events-per-user', type=int, default=50, help='Avg events per user')
    parser.add_argument('--preview', action='store_true', help='Show sample preview only')
    
    args = parser.parse_args()
    
    generator = EventGenerator(
        num_users=args.users,
        events_per_user_avg=args.events_per_user
    )
    
    if args.preview:
        generator.generate_sample_preview(10)
    else:
        print(f"Privacy-First User Analytics - Event Generation")
        print(f"=" * 60)
        print()
        
        generator.generate_events_jsonl()
        
        print()
        print("=" * 60)
        print("✓ Event generation complete!")

