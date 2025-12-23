import re
from urllib.parse import urlparse

def validate_url(url):
    """Validate URL format"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def validate_required_fields(form_data, required_fields):
    """Validate that required fields are not empty"""
    errors = []
    
    for field in required_fields:
        value = form_data.get(field, '')
        if not value or (isinstance(value, str) and not value.strip()):
            # Convert field name to user-friendly format
            field_name = field.replace('_', ' ').title()
            errors.append(f"{field_name} is required")
    
    return errors

def validate_email(email):
    """Validate email format"""
    if not email:
        return True  # Email is optional
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_date_format(date_string):
    """Validate date string format (YYYY-MM-DD)"""
    try:
        from datetime import datetime
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def clean_text_input(text):
    """Clean and sanitize text input"""
    if not text:
        return ""
    
    # Remove leading/trailing whitespace
    cleaned = text.strip()
    
    # Remove any potentially harmful characters (basic sanitization)
    # This is a simple approach - in production, you might want more sophisticated sanitization
    cleaned = re.sub(r'[<>"\']', '', cleaned)
    
    return cleaned

def validate_backlinks(backlinks_text):
    """Validate backlinks - each line should be a valid URL"""
    if not backlinks_text:
        return True, []
    
    lines = backlinks_text.strip().split('\n')
    invalid_urls = []
    
    for i, line in enumerate(lines, 1):
        line = line.strip()
        if line and not validate_url(line):
            invalid_urls.append(f"Line {i}: Invalid URL format")
    
    return len(invalid_urls) == 0, invalid_urls

def validate_tags_input(tags_text):
    """Validate tags/comma-separated input"""
    if not tags_text:
        return True, []
    
    # Split by comma and clean each tag
    tags = [tag.strip() for tag in tags_text.split(',')]
    
    # Remove empty tags
    valid_tags = [tag for tag in tags if tag]
    
    return True, valid_tags
