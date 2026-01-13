"""
Kafka Event Producer
Simulates real-time user events and publishes to Kafka
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from pathlib import Path


class EventProducer:
    """Produce real-time events to Kafka"""
    
    EVENT_TYPES = ['page_view', 'click', 'search', 'add_to_cart', 'purchase', 
                   'login', 'logout', 'video_play', 'video_complete']
    
    PAGES = ['/home', '/products', '/cart', '/checkout', '/account', '/search']
    
    DEVICES = ['desktop', 'mobile', 'tablet']
    
    BROWSERS = ['Chrome', 'Firefox', 'Safari', 'Edge']
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka broker address
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print(f"Kafka producer connected to {bootstrap_servers}")
    
    def generate_event(self):
        """Generate a random event"""
        
        event_type = random.choice(self.EVENT_TYPES)
        
        event = {
            'event_id': f'evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}',
            'user_id': f'user_{random.randint(1, 100000):06d}',
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'page_url': random.choice(self.PAGES),
            'device_type': random.choice(self.DEVICES),
            'browser': random.choice(self.BROWSERS),
            'ip_address': f'{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}'
        }
        
        # Event-specific fields
        if event_type == 'search':
            event['search_query'] = random.choice(['laptop', 'phone', 'shoes', 'jacket', 'camera'])
        
        elif event_type in ['add_to_cart', 'purchase']:
            event['product_id'] = f'prod_{random.randint(1, 10000):05d}'
            event['quantity'] = random.randint(1, 5)
            event['price'] = round(random.uniform(10, 500), 2)
        
        elif event_type in ['video_play', 'video_complete']:
            event['video_id'] = f'vid_{random.randint(1, 1000):04d}'
            event['duration_seconds'] = random.randint(30, 3600)
        
        return event
    
    def send_event(self, event, topic='user-events'):
        """
        Send event to Kafka
        
        Args:
            event: Event dictionary
            topic: Kafka topic name
        """
        future = self.producer.send(topic, value=event)
        
        try:
            record_metadata = future.get(timeout=10)
            return {
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            }
        except Exception as e:
            print(f"Error sending event: {e}")
            return None
    
    def produce_events(self, num_events=100, delay=0.1):
        """
        Produce multiple events with delay
        
        Args:
            num_events: Number of events to produce
            delay: Delay between events in seconds
        """
        print(f"\nProducing {num_events} events to Kafka...")
        print(f"Delay: {delay} seconds per event")
        print("=" * 60)
        
        sent = 0
        failed = 0
        
        start_time = time.time()
        
        for i in range(num_events):
            # Generate event
            event = self.generate_event()
            
            # Send to Kafka
            result = self.send_event(event)
            
            if result:
                sent += 1
                if sent % 10 == 0:
                    print(f"✓ Sent {sent}/{num_events} events "
                          f"(partition: {result['partition']}, offset: {result['offset']})")
            else:
                failed += 1
            
            # Delay
            time.sleep(delay)
        
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 60)
        print(f"✓ Production complete!")
        print(f"  Sent: {sent}")
        print(f"  Failed: {failed}")
        print(f"  Time: {elapsed:.2f} seconds")
        print(f"  Rate: {sent/elapsed:.1f} events/second")
    
    def close(self):
        """Close producer connection"""
        self.producer.flush()
        self.producer.close()
        print("\nKafka producer closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Produce events to Kafka')
    parser.add_argument('--events', type=int, default=100, 
                       help='Number of events to produce (default: 100)')
    parser.add_argument('--delay', type=float, default=0.1,
                       help='Delay between events in seconds (default: 0.1)')
    parser.add_argument('--continuous', action='store_true',
                       help='Run continuously (infinite loop)')
    
    args = parser.parse_args()
    
    producer = EventProducer()
    
    try:
        if args.continuous:
            print("Running in continuous mode (Ctrl+C to stop)")
            while True:
                producer.produce_events(num_events=10, delay=args.delay)
                time.sleep(1)
        else:
            producer.produce_events(num_events=args.events, delay=args.delay)
    
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    
    finally:
        producer.close()

