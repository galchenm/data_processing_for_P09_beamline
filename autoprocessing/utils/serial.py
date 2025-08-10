#!/usr/bin/env python3
# coding: utf8
# Written by Galchenkova M., Tolstikova A., Yefanov O., 2022

import os
import re
import glob
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from string import Template
from utils.nodes import are_the_reserved_nodes_overloaded
from utils.templates import filling_template_serial
from utils.log_setup import setup_logger
import time
import logging

split_lines = 250
chunk_size = 1000

def serial_data_processing(folder_with_raw_data, current_data_processing_folder,
                            cell_file, indexing_method, user, reserved_nodes, slurm_partition, 
                            sshPrivateKeyPath, sshPublicKeyPath, data_range=None, iteration=0):
    """Prepare and submit the serial data processing job."""

    logger = logging.getLogger('app')

    job_name = Path(current_data_processing_folder).name
    logger.info(f"Starting serial data processing for job: {job_name}")
    
    raw = folder_with_raw_data
    proc = current_data_processing_folder
    pdb = cell_file if cell_file else ""
    
    geom = "geometry.geom"
    
    os.chdir(proc)
    os.chmod(proc, 0o777)
    
    # Create directories
    stream_dir = Path("streams")
    error_dir = Path("error")
    joined_stream_dir = Path("j_stream")
    for d in [stream_dir, error_dir, joined_stream_dir]:
        d.mkdir(exist_ok=True)

    name1 = Path(proc).name
    
    # Load modules
    subprocess.run("source /etc/profile.d/modules.sh && module load maxwell xray crystfel", shell=True, executable='/bin/bash')

    # Find files

    cbf_files_to_process = []
    h5_files_to_process = []

    if not data_range:
        list_h5 = "list_h5.lst"
        list_cbf = "list_cbf.lst"
        with open(list_h5, "w") as f:
            subprocess.run(f"find {raw} -name '*.h5' | sort", shell=True, stdout=f)
        with open(list_cbf, "w") as f:
            subprocess.run(f"find {raw} -name '*.cbf' | sort", shell=True, stdout=f)
    else:
        list_h5 = f"list_h5_{iteration}.lst"
        list_cbf = f"list_cbf_{iteration}.lst"
        cbf_files = glob.glob(f"{raw}/*.cbf")
        h5_files = glob.glob(f"{raw}/*.h5")

        cbf_files_to_process = sorted([file for file in cbf_files if int(file.split(".")[0].split("_")[-1]) in data_range])
        h5_files_to_process = sorted([file for file in h5_files if int(file.split(".")[0].split("_")[-1]) in data_range])
        
        with open(list_cbf, "w") as f:
            f.write("\n".join(cbf_files_to_process))

        with open(list_h5, "w") as f:
            f.write("\n".join(h5_files_to_process))
    
    last_file = cbf_files_to_process[-1] if cbf_files_to_process else (h5_files_to_process[-1] if h5_files_to_process else None)

    if last_file:
        while not os.path.exists(last_file):
            time.sleep(5)

    # Determine filetype
    filetype = 0
    if os.path.getsize(list_h5) > 0:
        logger.info("Found h5 files")
        filetype = 1

    if os.path.getsize(list_cbf) > 0:
        logger.info("Found cbf files")
        filetype = 2

    if filetype == 0:
        logger.info("No .h5 or .cbf files found in the raw folder. Exiting.")
        sys.exit(0)

    # Convert list if necessary
    if filetype == 1:
        subprocess.run(f"list_events -i {list_h5} -g {geom} -o {list_cbf}", shell=True)

    # Split input file
    split_prefix = f"events-{name1}.lst"
    subprocess.run(f"split -a 3 -d -l {split_lines} {list_cbf} {split_prefix}", shell=True)
    logger.info(f"Split input file into chunks with prefix: {split_prefix}")
    # Create and submit SLURM jobs
    for split_file in sorted(Path(".").glob(f"{split_prefix}*")):
        suffix = split_file.name.replace(f"events-{name1}.lst", "")
        name = f"{name1}{suffix}"
        stream = f"{name1}.stream{suffix}"
        slurmfile = f"{name}.sh"
        err_file = Path(current_data_processing_folder) / f"{error_dir}/{name}_serial.err"
        out_file = Path(current_data_processing_folder) / f"{error_dir}/{name}_serial.out"

        logger.info(f"Processing {split_file.name} -> {stream}")
        with open(slurmfile, "w") as f:
            sbatch_command = "#!/bin/sh\n"
            sbatch_command += f"#SBATCH --job-name={name}\n"
            sbatch_command += f"#SBATCH --output={out_file}\n"
            sbatch_command += f"#SBATCH --error={err_file}\n"
            if "maxwell" not in reserved_nodes:
                login_node = reserved_nodes.split(",")[0] if "," in reserved_nodes else reserved_nodes
                reserved_nodes_overloaded = are_the_reserved_nodes_overloaded(reserved_nodes)

                ssh_command = (
                    f"/usr/bin/ssh -o BatchMode=yes -o CheckHostIP=no -o StrictHostKeyChecking=no "
                    f"-o GSSAPIAuthentication=no -o GSSAPIDelegateCredentials=no "
                    f"-o PasswordAuthentication=no -o PubkeyAuthentication=yes "
                    f"-o PreferredAuthentications=publickey -o ConnectTimeout=10 "
                    f"-l {user} -i {sshPrivateKeyPath} {login_node}"
                )
                if not reserved_nodes_overloaded:
                    sbatch_command += f"#SBATCH --partition={slurm_partition}\n"
                    sbatch_command += f"#SBATCH --reservation={reserved_nodes}\n"
                else:
                    sbatch_command += f"#SBATCH --partition=allcpu,upex,short\n"
            else:
                ssh_command = ""
                sbatch_command += "#SBATCH --partition=allcpu,upex,short\n"
                sbatch_command += "#SBATCH --time=4:00:00\n"
                sbatch_command += "#SBATCH --nodes=1\n"
                sbatch_command += "#SBATCH --nice=100\n"
                sbatch_command += "#SBATCH --mem=500000\n"
            
            sbatch_command += "module load maxwell xray crystfel\n"

            indexing_command = f"indexamajig -i {split_file.name} -o {stream_dir}/{stream} -j 80 -g {geom} --int-radius=3,6,8"
            indexing_command += " --peaks=peakfinder8 --min-snr=8 --min-res=10 --max-res=1200 --threshold=5"
            indexing_command += " --min-pix-count=1 --max-pix-count=10 --min-peaks=15 --local-bg-radius=3"
            indexing_command += f" --indexing={indexing_method} --no-check-cell --multi"
            if pdb:
                indexing_command += f" -p {pdb}"

            f.write(sbatch_command + "\n" + indexing_command + "\n")
            f.write(f"touch {name}.done\n")
        os.chmod(slurmfile, 0o755)
        # Submit the job  
        if ssh_command:
            subprocess.run(f'{ssh_command} "sbatch {slurmfile}"', shell=True, check=True)
        else:
            subprocess.run(f'sbatch {slurmfile}', shell=True, check=True)


def serial_processing(
        folder_with_raw_data,
        current_data_processing_folder,
        ORGX,
        ORGY,
        distance_offset,
        geometry_filename_template,
        cell_file,
        data_h5path,
        user,
        reserved_nodes,
        slurm_partition,
        sshPrivateKeyPath,
        sshPublicKeyPath
    ):
    """Main function to handle command line arguments and initiate data processing."""
    # Setup logger
    logger = setup_logger(log_dir=current_data_processing_folder.split('processed')[0] + 'processed', log_name="serial_processing")
    
    logger.info("Starting serial data processing...")
    logger.info(f"Processing folder: {folder_with_raw_data}")
    logger.info(f"Current data processing folder: {current_data_processing_folder}")
    logger.info(f"Geometry template: {geometry_filename_template}")
    
    ORGX = float(ORGX) if ORGX != "None" else 0
    ORGY = float(ORGY) if ORGY != "None" else 0
    distance_offset = float(distance_offset)
    if cell_file == "None":
        cell_file = None

    indexing_method, cell_file, NFRAMES = filling_template_serial(
        folder_with_raw_data,
        current_data_processing_folder,
        geometry_filename_template,
        data_h5path,
        ORGX,
        ORGY,
        distance_offset,
        cell_file
    )
    
    logger.info(f"Indexing method: {indexing_method}, Cell file: {cell_file}, NFRAMES: {NFRAMES}")
    
    if not indexing_method:
        logger.info("Indexing method could not be determined. Pure hitfinding.")
    
    iteration = 0
    for start_index in range(0, NFRAMES, chunk_size):
        end_index = min(start_index + chunk_size, NFRAMES)
        data_range = list(range(start_index, end_index))
        logger.info(f"Processing frames from {start_index} to {end_index} (data range: {data_range})")
        
        # Call the serial data processing function    
        serial_data_processing(
            folder_with_raw_data, current_data_processing_folder,
            cell_file, indexing_method, user, reserved_nodes, 
            slurm_partition, sshPrivateKeyPath, sshPublicKeyPath,
            data_range=data_range, iteration=iteration
        )
        iteration += 1

    # Create flag file
    flag_file = Path(current_data_processing_folder) / 'flag.txt'
    flag_file.touch(exist_ok=True)

