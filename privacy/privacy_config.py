"""
Privacy Configuration
Defines privacy policies for user analytics dataset
"""

try:
    from privacy.pii_detector import PIIType
    from privacy.anonymizer import AnonymizationMethod
except ImportError:
    from pii_detector import PIIType
    from anonymizer import AnonymizationMethod

class PrivacyConfig:
    """
    Privacy configuration for user analytics platform
    """
    
    # Field classifications for users table
    USER_FIELD_POLICIES = {
        'user_id': {
            'pii_type': PIIType.USER_ID,
            'anonymization': AnonymizationMethod.TOKENIZE,
            'sensitivity': 'LOW',
            'required': True,
            'description': 'User identifier - tokenized for reversibility'
        },
        'email': {
            'pii_type': PIIType.EMAIL,
            'anonymization': AnonymizationMethod.HASH,
            'sensitivity': 'HIGH',
            'required': True,
            'description': 'Email address - hashed (irreversible)'
        },
        'first_name': {
            'pii_type': PIIType.NAME,
            'anonymization': AnonymizationMethod.HASH,
            'sensitivity': 'MEDIUM',
            'required': False,
            'description': 'First name - hashed for privacy'
        },
        'last_name': {
            'pii_type': PIIType.NAME,
            'anonymization': AnonymizationMethod.HASH,
            'sensitivity': 'MEDIUM',
            'required': False,
            'description': 'Last name - hashed for privacy'
        },
        'age': {
            'pii_type': None,
            'anonymization': AnonymizationMethod.GENERALIZE,
            'sensitivity': 'LOW',
            'required': False,
            'description': 'Age - generalized to buckets for k-anonymity'
        },
        'gender': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': False,
            'description': 'Gender - no anonymization needed'
        },
        'city': {
            'pii_type': PIIType.ADDRESS,
            'anonymization': AnonymizationMethod.GENERALIZE,
            'sensitivity': 'MEDIUM',
            'required': False,
            'description': 'City - generalized to region'
        },
        'state': {
            'pii_type': PIIType.ADDRESS,
            'anonymization': None,
            'sensitivity': 'LOW',
            'required': False,
            'description': 'State - kept as-is (low granularity)'
        },
        'zip_code': {
            'pii_type': PIIType.ZIP_CODE,
            'anonymization': AnonymizationMethod.GENERALIZE,
            'sensitivity': 'MEDIUM',
            'required': False,
            'description': 'ZIP code - generalized to 3 digits'
        },
        'ip_address': {
            'pii_type': PIIType.IP_ADDRESS,
            'anonymization': AnonymizationMethod.MASK,
            'sensitivity': 'HIGH',
            'required': False,
            'description': 'IP address - subnet preserved, host masked'
        },
        'phone': {
            'pii_type': PIIType.PHONE,
            'anonymization': AnonymizationMethod.MASK,
            'sensitivity': 'HIGH',
            'required': False,
            'description': 'Phone number - last 4 digits preserved'
        },
        'created_at': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': True,
            'description': 'Account creation timestamp'
        },
        'last_login': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': False,
            'description': 'Last login timestamp'
        },
        'account_status': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': True,
            'description': 'Account status'
        },
        'subscription_tier': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': True,
            'description': 'Subscription tier'
        },
        'lifetime_value': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': False,
            'description': 'Customer lifetime value'
        },
        'total_purchases': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': False,
            'description': 'Total purchases count'
        }
    }
    
    # Event field policies
    EVENT_FIELD_POLICIES = {
        'event_id': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': True,
            'description': 'Event identifier'
        },
        'user_id': {
            'pii_type': PIIType.USER_ID,
            'anonymization': AnonymizationMethod.TOKENIZE,
            'sensitivity': 'LOW',
            'required': True,
            'description': 'User identifier - must match user table token'
        },
        'event_type': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': True,
            'description': 'Event type'
        },
        'timestamp': {
            'pii_type': None,
            'anonymization': None,
            'sensitivity': 'NONE',
            'required': True,
            'description': 'Event timestamp'
        },
        'ip_address': {
            'pii_type': PIIType.IP_ADDRESS,
            'anonymization': AnonymizationMethod.MASK,
            'sensitivity': 'HIGH',
            'required': False,
            'description': 'IP address - masked'
        },
        'session_id': {
            'pii_type': PIIType.SENSITIVE_ID,
            'anonymization': AnonymizationMethod.HASH,
            'sensitivity': 'MEDIUM',
            'required': False,
            'description': 'Session identifier - hashed'
        }
    }
    
    @classmethod
    def get_pii_fields(cls, dataset='users'):
        """
        Get list of PII fields for a dataset
        
        Args:
            dataset: 'users' or 'events'
            
        Returns:
            Dict of field_name -> PIIType
        """
        policies = cls.USER_FIELD_POLICIES if dataset == 'users' else cls.EVENT_FIELD_POLICIES
        
        return {
            field: config['pii_type']
            for field, config in policies.items()
            if config['pii_type'] is not None
        }
    
    @classmethod
    def get_anonymization_methods(cls, dataset='users'):
        """
        Get anonymization methods for a dataset
        
        Args:
            dataset: 'users' or 'events'
            
        Returns:
            Dict of field_name -> AnonymizationMethod
        """
        policies = cls.USER_FIELD_POLICIES if dataset == 'users' else cls.EVENT_FIELD_POLICIES
        
        return {
            field: config['anonymization']
            for field, config in policies.items()
            if config['anonymization'] is not None
        }
    
    @classmethod
    def get_high_sensitivity_fields(cls, dataset='users'):
        """
        Get high sensitivity fields
        
        Args:
            dataset: 'users' or 'events'
            
        Returns:
            List of high sensitivity field names
        """
        policies = cls.USER_FIELD_POLICIES if dataset == 'users' else cls.EVENT_FIELD_POLICIES
        
        return [
            field for field, config in policies.items()
            if config['sensitivity'] == 'HIGH'
        ]
    
    @classmethod
    def print_policy_summary(cls):
        """Print privacy policy summary"""
        print("Privacy Policy Summary")
        print("=" * 80)
        
        for dataset_name, policies in [
            ('Users', cls.USER_FIELD_POLICIES),
            ('Events', cls.EVENT_FIELD_POLICIES)
        ]:
            print(f"\n{dataset_name} Dataset:")
            print("-" * 80)
            
            for field, config in policies.items():
                pii = config['pii_type'].value if config['pii_type'] else 'None'
                anon = config['anonymization'].value if config['anonymization'] else 'None'
                sens = config['sensitivity']
                
                print(f"  {field:25} | Sensitivity: {sens:6} | PII: {pii:15} | Method: {anon}")


# Example usage
if __name__ == "__main__":
    print("Privacy Configuration")
    print("=" * 80)
    
    # Print policy summary
    PrivacyConfig.print_policy_summary()
    
    print("\n" + "=" * 80)
    
    # Get PII fields
    print("\nPII Fields (Users):")
    for field, pii_type in PrivacyConfig.get_pii_fields('users').items():
        print(f"  - {field}: {pii_type.value}")
    
    # Get high sensitivity fields
    print("\nHigh Sensitivity Fields (Users):")
    for field in PrivacyConfig.get_high_sensitivity_fields('users'):
        print(f"  - {field}")
    
    print("\n" + "=" * 80)
    print("✓ Privacy configuration ready")

