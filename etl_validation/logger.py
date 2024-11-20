def log_message(status, message):
    """
    Logs messages with a consistent format.
    """
    status_prefix = "[PASS]" if status else "[FAIL]"
    print(f"{status_prefix} {message}")
