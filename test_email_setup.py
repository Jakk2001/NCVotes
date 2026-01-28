"""
Test script to verify email notification setup.
Run this to check if all imports work before setting up Gmail.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    
    try:
        from config.settings import EMAIL_CONFIG, PROJECT_ROOT
        print("✓ Config imports successful")
    except Exception as e:
        print(f"✗ Config import failed: {e}")
        return False
    
    try:
        from src.database.connection import get_engine
        print("✓ Database connection imports successful")
    except Exception as e:
        print(f"✗ Database import failed: {e}")
        return False
    
    try:
        from src.email.notifications import send_update_email
        print("✓ Email notifications imports successful")
    except Exception as e:
        print(f"✗ Email notifications import failed: {e}")
        return False
    
    return True

def test_config():
    """Test that email configuration is set up."""
    print("\nTesting email configuration...")
    
    from config.settings import EMAIL_CONFIG
    
    if not EMAIL_CONFIG.get('enabled'):
        print("⚠ Email is disabled (EMAIL_ENABLED=false or not set)")
        print("  Set EMAIL_ENABLED=true in your .env file")
    else:
        print("✓ Email is enabled")
    
    if not EMAIL_CONFIG.get('smtp_user'):
        print("✗ EMAIL_USER not set in .env file")
        return False
    else:
        print(f"✓ Email user: {EMAIL_CONFIG['smtp_user']}")
    
    if not EMAIL_CONFIG.get('smtp_password'):
        print("✗ EMAIL_PASSWORD not set in .env file")
        return False
    else:
        print("✓ Email password is set")
    
    print(f"✓ Email recipient: {EMAIL_CONFIG['to_email']}")
    
    return True

def test_database():
    """Test database connection."""
    print("\nTesting database connection...")
    
    try:
        from src.database.connection import test_connection
        if test_connection():
            print("✓ Database connection successful")
            return True
        else:
            print("✗ Database connection failed")
            return False
    except Exception as e:
        print(f"✗ Database test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("NC Votes Email Notification Test")
    print("=" * 60)
    
    # Test imports
    if not test_imports():
        print("\n❌ Import test failed. Check your file structure.")
        return False
    
    # Test config
    config_ok = test_config()
    
    # Test database
    db_ok = test_database()
    
    print("\n" + "=" * 60)
    if config_ok and db_ok:
        print("✓ All tests passed! You're ready to send emails.")
        print("\nTo send a test email, run:")
        print("  python src/email/notifications.py")
    else:
        print("⚠ Some tests failed. Fix the issues above before sending emails.")
    print("=" * 60)
    
    return config_ok and db_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
