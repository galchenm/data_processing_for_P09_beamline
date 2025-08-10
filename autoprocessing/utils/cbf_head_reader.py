import os
import shutil
import re
import fabio
from pathlib import Path
import time

def wait_until_file_is_readable(filepath, timeout=10):
    """Wait until a file is readable.
    Args:
        filepath (str): Path to the file to check.
        timeout (int): Maximum time to wait in seconds.
    """
    start_time = time.time()
    while True:
        try:
            with open(filepath, 'rb'):
                return  # File can be opened for reading
        except Exception as e:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout: File {filepath} still not readable after {timeout} seconds. Error: {e}")
            time.sleep(0.5)  # Wait and retry


def retrieving_info_from_cbf(cbf_file):
    """Retrieve pixel size and dimensions from a CBF file.
    Args:
        cbf_file (str): Path to the CBF file.
    Returns:    
        tuple: A tuple containing the number of pixels in the short edge, long edge, and pixel size.
    Raises:
        TimeoutError: If the file is not readable within the specified timeout.
    """
    wait_until_file_is_readable(cbf_file, timeout=30)
    img = fabio.open(cbf_file)
    header = img.header
    if "X-Binary-Size-Fastest-Dimension" not in header or "X-Binary-Size-Second-Dimension" not in header:
        print(f"Error: Missing X-Binary-Size headers in {cbf_file}")
        N_PIXELS_TO_THE_SHORT_EDGE = 2462  # Default value
        N_PIXELS_TO_THE_LONG_EDGE = 2526   # Default value
        pixel_size = 0.000172  # Default value
    else:
        N_PIXELS_TO_THE_SHORT_EDGE = float(header["X-Binary-Size-Fastest-Dimension"])
        N_PIXELS_TO_THE_LONG_EDGE = float(header["X-Binary-Size-Second-Dimension"])
        lines = header["_array_data.header_contents"].splitlines()
        pixel_line = next((line for line in lines if "Pixel_size" in line), None)
        match = re.search(r"Pixel_size\s+([\deE\.\-]+)\s*m\s*x\s*([\deE\.\-]+)\s*m", pixel_line)
        if match:
            pixel_size = float(match.group(1))
    img.close()    
    return N_PIXELS_TO_THE_SHORT_EDGE, N_PIXELS_TO_THE_LONG_EDGE, pixel_size
