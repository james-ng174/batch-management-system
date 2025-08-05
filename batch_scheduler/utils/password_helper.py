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
