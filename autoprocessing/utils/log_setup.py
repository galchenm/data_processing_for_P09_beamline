import logging
import os

def setup_logger(log_dir=None, log_name="Auto-processing-P09-beamline"):
    
    level = logging.INFO
    logger = logging.getLogger(log_name)
    logger.setLevel(level)

    if log_dir is None:
        log_dir = os.getcwd()  # fallback to current dir

    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'{log_name}.log')

    formatter = logging.Formatter('%(levelname)s - %(message)s')
    ch = logging.FileHandler(log_file)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    if not logger.handlers:  # avoid adding multiple handlers in re-runs
        logger.addHandler(ch)

    logger.info(f"Setup logger in PID {os.getpid()}")
    print(f"Log file is {log_file}")
    return logger

