import re
    
def extract_value_from_info(info_path, key, fallback=None, is_float=True, is_string=False):
    """Extract a value from the info.txt file based on the provided key.
    Args:
        info_path (str): Path to the info.txt file.
        key (str): The key to search for in the file.
        fallback: The value to return if the key is not found. Defaults to 0 for numeric values and an empty string for string values.
        is_float (bool): If True, the extracted value will be converted to float. Defaults to True.
        is_string (bool): If True, the extracted value will be treated as a string. Defaults to False.
    Returns:
        The extracted value if found, otherwise the fallback value.
    """
    if fallback is None:
        fallback = "" if is_string else 0

    try:
        with open(info_path) as f:
            lines = f.readlines()
        for line in lines:
            if key in line:
                if is_string:
                    # Get value after the first colon, trim whitespace
                    return line.split(":", 1)[-1].strip()
                else:
                    match = re.search(r"[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?", line)
                    if match:
                        return float(match.group()) if is_float else int(float(match.group()))
    except Exception:
        pass

    return fallback
