import re


# Define a pattern for UUID4 to keep the route patterns concise.
UUID_PATTERN = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
WORKER_PATTERN = re.compile(rf'worker/{UUID_PATTERN}')


def filter_strings_by_regex(strings, pattern):
    compiled_pattern = re.compile(pattern)
    return [s for s in strings if compiled_pattern.fullmatch(s)]

def is_valid_worker_broadcast(key: str) -> bool:
    """
    Checks if a route is valid.

    Args:
        route: route to check

    Returns:
        True if route is valid, False otherwise.
    """


    valid_key = rf'worker/{UUID_PATTERN}'

    if re.fullmatch(valid_key, key):
        return True
    return False
