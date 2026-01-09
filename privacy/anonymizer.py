"""
Data Anonymization Pipeline
Applies privacy-preserving transformations to PII fields
"""

import hashlib
import hmac
import secrets
from typing import Dict, Any, Optional
from enum import Enum
from pii_detector import PIIType


class AnonymizationMethod(Enum):
    """Anonymization techniques available"""
    HASH = "hash"                      # One-way hash (irreversible)
    MASK = "mask"                      # Partial masking
    GENERALIZE = "generalize"          # K-anonymity grouping
    SUPPRESS = "suppress"              # Remove entirely
    TOKENIZE = "tokenize"              # Reversible pseudonymization
    NOISE = "noise"                    # Differential privacy


class Anonymizer:
    """
    Applies privacy transformations to sensitive data
    """
    
    # Default anonymization strategy per PII type
    DEFAULT_STRATEGIES = {
        PIIType.EMAIL: AnonymizationMethod.HASH,
        PIIType.PHONE: AnonymizationMethod.MASK,
        PIIType.SSN: AnonymizationMethod.HASH,
        PIIType.CREDIT_CARD: AnonymizationMethod.MASK,
        PIIType.IP_ADDRESS: AnonymizationMethod.MASK,
        PIIType.NAME: AnonymizationMethod.HASH,
        PIIType.ADDRESS: AnonymizationMethod.GENERALIZE,
        PIIType.ZIP_CODE: AnonymizationMethod.GENERALIZE,
        PIIType.USER_ID: AnonymizationMethod.TOKENIZE
    }
    
    def __init__(self, secret_key: str = "default-secret-key"):
        """
        Initialize anonymizer
        
        Args:
            secret_key: Secret key for HMAC operations
        """
        self.secret_key = secret_key
        self.token_map = {}  # For reversible tokenization
    
    def hash_value(self, value: str) -> str:
        """
        One-way hash (irreversible)
        
        Args:
            value: Value to hash
            
        Returns:
            Hashed value (hex)
        """
        if not value:
            return None
        
        return hmac.new(
            self.secret_key.encode('utf-8'),
            str(value).encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def mask_email(self, email: str) -> str:
        """
        Mask email address
        
        Args:
            email: Email address
            
        Returns:
            Masked email (e.g., "u***@example.com")
        """
        if not email or '@' not in email:
            return email
        
        local, domain = email.split('@', 1)
        
        if len(local) <= 2:
            masked_local = local[0] + '*'
        else:
            masked_local = local[0] + '*' * (len(local) - 1)
        
        return f"{masked_local}@{domain}"
    
    def mask_phone(self, phone: str) -> str:
        """
        Mask phone number
        
        Args:
            phone: Phone number
            
        Returns:
            Masked phone (e.g., "***-***-4567")
        """
        if not phone:
            return None
        
        # Extract digits
        digits = ''.join(c for c in str(phone) if c.isdigit())
        
        if len(digits) >= 4:
            return '***-***-' + digits[-4:]
        else:
            return '***'
    
    def mask_ip(self, ip_address: str) -> str:
        """
        Mask IP address (preserve subnet)
        
        Args:
            ip_address: IP address
            
        Returns:
            Masked IP (e.g., "192.168.1.0")
        """
        if not ip_address or '.' not in ip_address:
            return ip_address
        
        parts = ip_address.split('.')
        if len(parts) == 4:
            parts[-1] = '0'
            return '.'.join(parts)
        
        return ip_address
    
    def generalize_zip(self, zip_code: str) -> str:
        """
        Generalize ZIP code to 3 digits (k-anonymity)
        
        Args:
            zip_code: ZIP code
            
        Returns:
            Generalized ZIP (e.g., "123**")
        """
        if not zip_code:
            return None
        
        digits = ''.join(c for c in str(zip_code) if c.isdigit())
        
        if len(digits) >= 3:
            return digits[:3] + '**'
        else:
            return '***'
    
    def generalize_city(self, city: str) -> str:
        """
        Generalize city to region
        
        Args:
            city: City name
            
        Returns:
            Region name
        """
        # US regions mapping
        regions = {
            'New York': 'Northeast',
            'Philadelphia': 'Northeast',
            'Boston': 'Northeast',
            'Los Angeles': 'West',
            'San Diego': 'West',
            'San Francisco': 'West',
            'San Jose': 'West',
            'Seattle': 'West',
            'Portland': 'West',
            'Phoenix': 'Southwest',
            'Dallas': 'Southwest',
            'Houston': 'Southwest',
            'Austin': 'Southwest',
            'San Antonio': 'Southwest',
            'Chicago': 'Midwest',
            'Denver': 'Midwest'
        }
        
        return regions.get(city, 'Other')
    
    def generalize_age(self, age: int) -> str:
        """
        Generalize age to bucket (k-anonymity)
        
        Args:
            age: Age in years
            
        Returns:
            Age range string
        """
        if age < 18:
            return "0-17"
        elif age < 25:
            return "18-24"
        elif age < 35:
            return "25-34"
        elif age < 45:
            return "35-44"
        elif age < 55:
            return "45-54"
        elif age < 65:
            return "55-64"
        else:
            return "65+"
    
    def tokenize(self, value: str, field_name: str) -> str:
        """
        Reversible tokenization
        
        Args:
            value: Value to tokenize
            field_name: Field name (for namespacing)
            
        Returns:
            Token
        """
        if not value:
            return None
        
        # Check if already tokenized
        key = f"{field_name}:{value}"
        if key in self.token_map:
            return self.token_map[key]
        
        # Generate new token
        token = f"tok_{secrets.token_urlsafe(16)}"
        
        # Store mapping
        self.token_map[key] = token
        self.token_map[f"reverse:{token}"] = value
        
        return token
    
    def suppress(self, value: Any) -> None:
        """
        Suppress value entirely
        
        Args:
            value: Value to suppress
            
        Returns:
            None
        """
        return None
    
    def anonymize_field(self, value: Any, pii_type: PIIType, 
                       method: Optional[AnonymizationMethod] = None,
                       field_name: str = None) -> Any:
        """
        Apply anonymization to a field value
        
        Args:
            value: Value to anonymize
            pii_type: Type of PII
            method: Anonymization method (uses default if not specified)
            field_name: Field name (for tokenization)
            
        Returns:
            Anonymized value
        """
        if value is None:
            return None
        
        # Use default method if not specified
        if method is None:
            method = self.DEFAULT_STRATEGIES.get(pii_type, AnonymizationMethod.HASH)
        
        # Apply transformation
        if method == AnonymizationMethod.HASH:
            return self.hash_value(str(value))
        
        elif method == AnonymizationMethod.MASK:
            if pii_type == PIIType.EMAIL:
                return self.mask_email(str(value))
            elif pii_type == PIIType.PHONE:
                return self.mask_phone(str(value))
            elif pii_type == PIIType.IP_ADDRESS:
                return self.mask_ip(str(value))
            else:
                return self.hash_value(str(value))
        
        elif method == AnonymizationMethod.GENERALIZE:
            if pii_type == PIIType.ZIP_CODE:
                return self.generalize_zip(str(value))
            elif pii_type == PIIType.ADDRESS:
                return self.generalize_city(str(value))
            else:
                return str(value)
        
        elif method == AnonymizationMethod.TOKENIZE:
            return self.tokenize(str(value), field_name or 'default')
        
        elif method == AnonymizationMethod.SUPPRESS:
            return self.suppress(value)
        
        else:
            # Default: hash
            return self.hash_value(str(value))
    
    def anonymize_record(self, record: Dict[str, Any], 
                        pii_fields: Dict[str, PIIType]) -> Dict[str, Any]:
        """
        Anonymize entire record
        
        Args:
            record: Record dictionary
            pii_fields: Dict of field_name -> PIIType
            
        Returns:
            Anonymized record
        """
        anonymized = record.copy()
        
        for field_name, pii_type in pii_fields.items():
            if field_name in anonymized:
                anonymized[field_name] = self.anonymize_field(
                    anonymized[field_name],
                    pii_type,
                    field_name=field_name
                )
        
        return anonymized


# Example usage
if __name__ == "__main__":
    anonymizer = Anonymizer()
    
    print("Data Anonymization Pipeline")
    print("=" * 60)
    
    # Test data
    test_record = {
        'user_id': 'user_123456',
        'email': 'john.doe@example.com',
        'first_name': 'John',
        'last_name': 'Doe',
        'age': 34,
        'phone': '555-123-4567',
        'ip_address': '192.168.1.42',
        'city': 'San Diego',
        'zip_code': '92101',
        'account_status': 'active'
    }
    
    # PII fields to anonymize
    pii_fields = {
        'user_id': PIIType.USER_ID,
        'email': PIIType.EMAIL,
        'first_name': PIIType.NAME,
        'last_name': PIIType.NAME,
        'phone': PIIType.PHONE,
        'ip_address': PIIType.IP_ADDRESS,
        'city': PIIType.ADDRESS,
        'zip_code': PIIType.ZIP_CODE
    }
    
    print("\n1. ORIGINAL RECORD:")
    print("-" * 60)
    for key, value in test_record.items():
        pii_marker = "🔒" if key in pii_fields else "  "
        print(f"{pii_marker} {key:20} = {value}")
    
    # Anonymize
    anonymized = anonymizer.anonymize_record(test_record, pii_fields)
    
    print("\n2. ANONYMIZED RECORD:")
    print("-" * 60)
    for key, value in anonymized.items():
        if key in pii_fields:
            original = test_record[key]
            print(f"✓ {key:20} = {value}")
            print(f"  {'':20}   (was: {original})")
        else:
            print(f"  {key:20} = {value}")
    
    # Test specific transformations
    print("\n3. TRANSFORMATION EXAMPLES:")
    print("-" * 60)
    
    examples = [
        ('Email masking', anonymizer.mask_email('john.doe@example.com')),
        ('Phone masking', anonymizer.mask_phone('555-123-4567')),
        ('IP masking', anonymizer.mask_ip('192.168.1.42')),
        ('ZIP generalization', anonymizer.generalize_zip('92101')),
        ('City generalization', anonymizer.generalize_city('San Diego')),
        ('Age bucketing', anonymizer.generalize_age(34)),
        ('Email hashing', anonymizer.hash_value('john.doe@example.com')[:16] + '...')
    ]
    
    for label, result in examples:
        print(f"✓ {label:25} → {result}")
    
    print("\n" + "=" * 60)
    print("✓ Anonymization pipeline ready")

