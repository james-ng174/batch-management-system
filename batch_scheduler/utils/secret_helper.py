import os
from cryptography.fernet import Fernet

encryption_key_path = os.getenv('ENCRYPTION_KEY_PATH')


def load_db_encryption_key():
    with open(encryption_key_path, "rb") as key_file:
        return key_file.read()


encryption_key = load_db_encryption_key()


def encrypt_data(plain_text: str) -> str:
    if not plain_text:
        return ''
    fernet = Fernet(encryption_key)
    encrypted_data = fernet.encrypt(plain_text.encode())
    return encrypted_data.decode()


def decrypt_data(encrypted_text: str) -> str:
    if not encrypted_text:
        return ''
    fernet = Fernet(encryption_key)
    decrypted_data = fernet.decrypt(encrypted_text.encode())
    return decrypted_data.decode()
