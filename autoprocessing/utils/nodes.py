import os
import subprocess
import shlex

LIMIT_FOR_RESERVED_NODES = 25
def are_the_reserved_nodes_overloaded(node_list):
    """Check if the reserved nodes are overloaded by counting running jobs.
    Args:
        node_list (str): Comma-separated list of reserved nodes.
    Returns:
        bool: True if the number of jobs exceeds the limit, False otherwise.
    """
    try:
        jobs_cmd = f'squeue -w {node_list}'
        all_jobs = subprocess.check_output(shlex.split(jobs_cmd)).decode().splitlines()
    except subprocess.CalledProcessError:
        all_jobs = []
    return len(all_jobs) > LIMIT_FOR_RESERVED_NODES
