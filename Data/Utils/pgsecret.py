import secrets


def generate_secret(secret_size):
    return secrets.token_hex(secret_size)