"""
Automatic PII Detection Engine
Identifies sensitive fields in datasets using pattern matching and heuristics
"""

import re
from typing import List, Dict, Set
from enum import Enum


class PIIType(Enum):
    """Types of PII that can be detected"""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"
    NAME = "name"
    ADDRESS = "address"
    ZIP_CODE = "zip_code"
    DATE_OF_BIRTH = "date_of_birth"
    USER_ID = "user_id"
    SENSITIVE_ID = "sensitive_id"


class PIIDetector:
    """
    Detects PII in column names and data values
    """
    
    # Column name patterns that indicate PII
    COLUMN_NAME_PATTERNS = {
        PIIType.EMAIL: [
            r'email', r'e_mail', r'mail', r'email_address'
        ],
        PIIType.PHONE: [
            r'phone', r'mobile', r'cell', r'telephone', r'phone_number'
        ],
        PIIType.SSN: [
            r'ssn', r'social_security', r'social_security_number'
        ],
        PIIType.CREDIT_CARD: [
            r'credit_card', r'cc_number', r'card_number', r'payment_card'
        ],
        PIIType.IP_ADDRESS: [
            r'ip_address', r'ip_addr', r'ip'
        ],
        PIIType.NAME: [
            r'first_name', r'last_name', r'full_name', r'name', 
            r'firstname', r'lastname', r'given_name', r'family_name'
        ],
        PIIType.ADDRESS: [
            r'address', r'street', r'city', r'state', r'country',
            r'street_address', r'home_address', r'mailing_address'
        ],
        PIIType.ZIP_CODE: [
            r'zip', r'zipcode', r'zip_code', r'postal_code', r'postcode'
        ],
        PIIType.DATE_OF_BIRTH: [
            r'dob', r'date_of_birth', r'birth_date', r'birthdate'
        ],
        PIIType.USER_ID: [
            r'user_id', r'userid', r'customer_id', r'account_id'
        ]
    }
    
    # Value patterns for detecting PII in actual data
    VALUE_PATTERNS = {
        PIIType.EMAIL: re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        ),
        PIIType.PHONE: re.compile(
            r'^[\d\-\(\)\+\s\.x]{10,20}$'
        ),
        PIIType.SSN: re.compile(
            r'^\d{3}-?\d{2}-?\d{4}$'
        ),
        PIIType.CREDIT_CARD: re.compile(
            r'^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$'
        ),
        PIIType.IP_ADDRESS: re.compile(
            r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'
        ),
        PIIType.ZIP_CODE: re.compile(
            r'^\d{5}(-\d{4})?$'
        )
    }
    
    def __init__(self):
        """Initialize PII detector"""
        self.detected_fields = {}
    
    def detect_by_column_name(self, column_name: str) -> Set[PIIType]:
        """
        Detect PII type from column name
        
        Args:
            column_name: Column name to check
            
        Returns:
            Set of detected PII types
        """
        detected = set()
        column_lower = column_name.lower()
        
        for pii_type, patterns in self.COLUMN_NAME_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, column_lower):
                    detected.add(pii_type)
                    break
        
        return detected
    
    def detect_by_value(self, value: str) -> Set[PIIType]:
        """
        Detect PII type from actual value
        
        Args:
            value: Value to check
            
        Returns:
            Set of detected PII types
        """
        if not isinstance(value, str) or not value:
            return set()
        
        detected = set()
        
        for pii_type, pattern in self.VALUE_PATTERNS.items():
            if pattern.match(value.strip()):
                detected.add(pii_type)
        
        return detected
    
    def scan_dataframe_schema(self, columns: List[str]) -> Dict[str, List[PIIType]]:
        """
        Scan DataFrame schema for PII fields
        
        Args:
            columns: List of column names
            
        Returns:
            Dict mapping column names to detected PII types
        """
        results = {}
        
        for column in columns:
            detected = self.detect_by_column_name(column)
            if detected:
                results[column] = list(detected)
        
        return results
    
    def scan_sample_data(self, column_name: str, 
                        sample_values: List[str]) -> Set[PIIType]:
        """
        Scan sample data values to confirm PII type
        
        Args:
            column_name: Column name
            sample_values: Sample values to check
            
        Returns:
            Set of detected PII types
        """
        # First check column name
        detected = self.detect_by_column_name(column_name)
        
        # Verify with sample values
        value_matches = set()
        for value in sample_values[:100]:  # Check first 100 samples
            if value:
                value_types = self.detect_by_value(str(value))
                value_matches.update(value_types)
        
        # Combine results
        if value_matches:
            detected.update(value_matches)
        
        return detected
    
    def classify_sensitivity(self, pii_types: Set[PIIType]) -> str:
        """
        Classify sensitivity level based on PII types detected
        
        Args:
            pii_types: Set of detected PII types
            
        Returns:
            Sensitivity level: HIGH, MEDIUM, LOW, NONE
        """
        high_sensitivity = {
            PIIType.SSN,
            PIIType.CREDIT_CARD,
            PIIType.DATE_OF_BIRTH
        }
        
        medium_sensitivity = {
            PIIType.EMAIL,
            PIIType.PHONE,
            PIIType.NAME,
            PIIType.ADDRESS,
            PIIType.IP_ADDRESS
        }
        
        if any(pii in high_sensitivity for pii in pii_types):
            return "HIGH"
        elif any(pii in medium_sensitivity for pii in pii_types):
            return "MEDIUM"
        elif pii_types:
            return "LOW"
        else:
            return "NONE"
    
    def generate_field_report(self, columns: List[str], 
                             sample_data: Dict[str, List[str]] = None) -> Dict:
        """
        Generate comprehensive PII detection report
        
        Args:
            columns: List of column names
            sample_data: Optional dict of column_name -> sample values
            
        Returns:
            Detection report
        """
        report = {
            'total_fields': len(columns),
            'pii_fields_detected': 0,
            'fields': {},
            'summary': {
                'HIGH': 0,
                'MEDIUM': 0,
                'LOW': 0,
                'NONE': 0
            }
        }
        
        for column in columns:
            # Detect from column name
            detected = self.detect_by_column_name(column)
            
            # If sample data provided, verify with values
            if sample_data and column in sample_data:
                value_types = self.scan_sample_data(column, sample_data[column])
                detected.update(value_types)
            
            # Classify sensitivity
            sensitivity = self.classify_sensitivity(detected)
            
            report['fields'][column] = {
                'pii_types': [pii.value for pii in detected],
                'sensitivity': sensitivity,
                'requires_anonymization': sensitivity in ['HIGH', 'MEDIUM']
            }
            
            report['summary'][sensitivity] += 1
            
            if detected:
                report['pii_fields_detected'] += 1
        
        return report


# Example usage
if __name__ == "__main__":
    detector = PIIDetector()
    
    print("PII Detection Engine")
    print("=" * 60)
    
    # Test column names
    test_columns = [
        'user_id', 'email', 'first_name', 'last_name', 'age',
        'phone', 'ip_address', 'city', 'state', 'zip_code',
        'created_at', 'last_login', 'account_status'
    ]
    
    print("\n1. COLUMN NAME DETECTION:")
    print("-" * 60)
    for column in test_columns:
        detected = detector.detect_by_column_name(column)
        if detected:
            print(f"✓ {column:20} → {', '.join([p.value for p in detected])}")
        else:
            print(f"  {column:20} → No PII detected")
    
    # Test values
    print("\n2. VALUE PATTERN DETECTION:")
    print("-" * 60)
    
    test_values = [
        ('test@example.com', 'email'),
        ('555-123-4567', 'phone'),
        ('192.168.1.1', 'ip_address'),
        ('12345', 'zip_code'),
        ('user_12345', 'user_id'),
        ('John Doe', 'name (no pattern)'),
    ]
    
    for value, label in test_values:
        detected = detector.detect_by_value(value)
        if detected:
            print(f"✓ {label:20} '{value}' → {', '.join([p.value for p in detected])}")
        else:
            print(f"  {label:20} '{value}' → No pattern match")
    
    # Generate report
    print("\n3. FIELD REPORT:")
    print("-" * 60)
    
    sample_data = {
        'email': ['test@example.com', 'user@gmail.com'],
        'phone': ['555-123-4567', '555-987-6543'],
        'ip_address': ['192.168.1.1', '10.0.0.1'],
        'age': ['25', '34', '42']
    }
    
    report = detector.generate_field_report(test_columns, sample_data)
    
    print(f"Total fields: {report['total_fields']}")
    print(f"PII fields detected: {report['pii_fields_detected']}")
    print(f"\nSensitivity breakdown:")
    for level, count in report['summary'].items():
        if count > 0:
            print(f"  {level}: {count} fields")
    
    print(f"\nFields requiring anonymization:")
    for field, info in report['fields'].items():
        if info['requires_anonymization']:
            print(f"  - {field} ({info['sensitivity']}): {', '.join(info['pii_types'])}")
    
    print("\n" + "=" * 60)
    print("✓ PII detection engine ready")

