import logging
import os


def validate_communication_key(key):
    try:
        key = str(key)
        secretKey = os.getenv("COM_X_KEY")
        if key != secretKey:
            logging.exception(f"Invalid key")
            return "Unauthorized"
        return None
    except Exception as e:
        logging.exception(f"Error while processing authentication key or invalid key, {e}")
        return "Unauthorized"
