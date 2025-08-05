import re


def validate_password(password):
    # Initialize a list to hold error messages
    errors = []

    # Check length
    if len(password) < 10:
        errors.append("Password must be at least 10 characters long.")

    # Check for uppercase letter
    if not re.search(r'[A-Z]', password):
        errors.append("Password must contain at least one uppercase letter.")

    # Check for lowercase letter
    if not re.search(r'[a-z]', password):
        errors.append("Password must contain at least one lowercase letter.")

    # Check for number
    if not re.search(r'[0-9]', password):
        errors.append("Password must contain at least one number.")

    # Check for special character
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        errors.append("Password must contain at least one special character.")

    # Check for spaces
    if re.search(r'\s', password):
        errors.append("Password must not contain spaces.")

    # If there are errors, return them
    if errors:
        return False, errors
    else:
        return True, "Success"  # 200 OK


def validate_phone(phone_number):
    errors = []

    # Remove allowed characters to check for invalid ones
    allowed_chars = re.sub(r'[0-9\s\-\.\(\)\+]', '', phone_number)
    if allowed_chars:
        errors.append("Phone number contains invalid characters.")

    # Count the number of digits
    digit_count = len(re.findall(r'\d', phone_number))
    if digit_count < 6:
        errors.append("Phone number must contain at least 6 digits.")

    # If there are errors, return them
    if errors:
        return False, errors
    else:
        return True, "Success"


def validate_user_id(user_id):
    errors = []

    # Check if user_id contains only letters and numbers
    if not re.match(r'^[A-Za-z0-9]+$', user_id):
        errors.append("User ID must contain only letters and numbers, without spaces or special characters.")

    if errors:
        return False, errors
    else:
        return True, "Success"
