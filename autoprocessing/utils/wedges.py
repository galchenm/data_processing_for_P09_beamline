#!/usr/bin/env python3
# coding: utf8
# Written by Galchenkova M., Tolstikova A., Yefanov O., 2022 (revised)

import os
import sys
import time
import math
import fabio
import gemmi
import glob
import re
import shutil
import subprocess
import logging
from string import Template
from pathlib import Path
from collections import defaultdict
import shlex
from utils.nodes import are_the_reserved_nodes_overloaded
from utils.UC import parse_UC_file, parse_cryst1_and_spacegroup_number
from utils.extract import extract_value_from_info
from utils.templates import filling_template_wedges
from utils.log_setup import setup_logger

SLEEP_TIME = 10 

def xds_start(
    current_data_processing_folder,
    command_for_data_processing,
    user,
    reserved_nodes,
    slurm_partition,
    ssh_private_key_path,
    ssh_public_key_path,
    login_node=None
):
    """Prepare and submit the XDS job."""
    os.chdir(current_data_processing_folder)
    
    def get_slurm_header(partition, reservation=None, extras=None):
        lines = [
            "#!/bin/sh\n",
            f"#SBATCH --job-name={job_name}\n",
            f"#SBATCH --partition={partition}\n",
            "#SBATCH --nodes=1\n",
            f"#SBATCH --output={out_file}\n",
            f"#SBATCH --error={err_file}\n"
        ]
        if reservation:
            lines.append(f"#SBATCH --reservation={reservation}\n")
        if extras:
            lines.extend(extras)
        return lines

    def get_common_xds_commands():
        return [
            "source /etc/profile.d/modules.sh\n",
            "module load xray\n",
            f"{command_for_data_processing}\n",
            f"sleep {SLEEP_TIME}\n",
            f"cd {current_data_processing_folder}\n",
            "cp GXPARM.XDS XPARM.XDS\n",
            "cp XDS_ASCII.HKL XDS_ASCII.HKL_1\n",
            "mv CORRECT.LP CORRECT.LP_1\n",
            "sed -i 's/ JOB= XYCORR INIT/!JOB= XYCORR INIT/g' XDS.INP\n",
            "sed -i 's/!JOB= CORRECT/ JOB= DEFPIX INTEGRATE CORRECT/g' XDS.INP\n",
            f"{command_for_data_processing}\n"
        ]

    job_name = Path(current_data_processing_folder).name
    slurmfile = Path(current_data_processing_folder) / f"{job_name}_XDS.sh"
    err_file = Path(current_data_processing_folder) / f"{job_name}_XDS.err"
    out_file = Path(current_data_processing_folder) / f"{job_name}_XDS.out"

    sbatch_file = []
    ssh_command = ""

    is_maxwell = "maxwell" in reserved_nodes

    if is_maxwell:
        # No SSH needed for maxwell
        slurm_extras = [
            "#SBATCH --time=12:00:00\n",
            "#SBATCH --nice=100\n",
            "#SBATCH --mem=500000\n"
        ]
        sbatch_file += get_slurm_header("allcpu,upex", extras=slurm_extras)
        sbatch_file += get_common_xds_commands()
    else:
        reserved_nodes_overloaded = are_the_reserved_nodes_overloaded(reserved_nodes)
        partition = slurm_partition if not reserved_nodes_overloaded else "allcpu,upex,short"
        reservation = reserved_nodes if not reserved_nodes_overloaded else None
        sbatch_file += get_slurm_header(partition, reservation)
        sbatch_file += get_common_xds_commands()

        if login_node:
            ssh_command = (
                f"/usr/bin/ssh -o BatchMode=yes -o CheckHostIP=no "
                f"-o StrictHostKeyChecking=no -o GSSAPIAuthentication=no "
                f"-o GSSAPIDelegateCredentials=no -o PasswordAuthentication=no "
                f"-o PubkeyAuthentication=yes -o PreferredAuthentications=publickey "
                f"-o ConnectTimeout=10 -l {user} -i {ssh_private_key_path} {login_node}"
            )

    # Write SLURM file
    with open(slurmfile, 'w') as fh:
        fh.writelines(sbatch_file)
    os.chmod(slurmfile, 0o755)

    # Submit the job
    submit_command = f'{ssh_command} "sbatch {slurmfile}"' if ssh_command else f'sbatch {slurmfile}'
    subprocess.run(submit_command, shell=True, check=True)

def group_cbf_by_position(folder):
    """Groups CBF files by their position and frame numbers.
    Args:
        folder (str): Path to the folder containing CBF files.
    Returns:
        dict: A dictionary where keys are position strings and values are dictionaries with 'start', 'end', and 'template'.
    """
    
    position_frames = defaultdict(list)
    templates = {}
    for filename in os.listdir(folder):
        if not filename.endswith(".cbf"):
            continue
        # Match pattern ending in _######_######.cbf
        match = re.search(r"^(.*)_(\d{6})_(\d{5})\.cbf$", filename)
        if not match:
            continue
        prefix, position_str, frame_str = match.groups()
        try:
            position = int(position_str)
            frame = int(frame_str)
        except ValueError:
            continue
        position_frames[position].append(frame)
        if position not in templates:
            # Reconstruct template using the matched prefix and position
            template = os.path.join(folder, f"{prefix}_{position_str}_?????.cbf")
            templates[position] = template
    results = {}
    for position, frames in position_frames.items():
        pos_str = f"{position:06d}"
        results[pos_str] = {
            "start": min(frames),
            "end": max(frames),
            "template": templates[position]
        }
    return results


def wedges_processing(
    folder_with_raw_data,
    current_data_processing_folder,
    ORGX,
    ORGY,
    distance_offset,
    command_for_data_processing,
    XDS_INP_template,
    REFERENCE_DATA_SET,
    user,
    reserved_nodes,
    slurm_partition,
    sshPrivateKeyPath,
    sshPublicKeyPath
    ):
    """Main function to process command line arguments and call the filling_template_wedges function."""

    # Setup logger
    logger = setup_logger(log_dir=current_data_processing_folder.split('processed')[0] + 'processed', log_name="wedges_processing")
    
    logger.info("Starting wedges data processing...")
    logger.info(f"Processing folder: {folder_with_raw_data}")
    logger.info(f"Current data processing folder: {current_data_processing_folder}")
    logger.info(f"Geometry template: {XDS_INP_template}")
    
    os.makedirs(current_data_processing_folder, exist_ok=True)

    ORGX = float(ORGX) if ORGX != "None" else 0
    ORGY = float(ORGY) if ORGY != "None" else 0
    distance_offset = float(distance_offset)


    REFERENCE_DATA_SET = REFERENCE_DATA_SET if REFERENCE_DATA_SET != "None" else "!REFERENCE_DATA_SET"
    logger.info(f"Reference data set: {REFERENCE_DATA_SET}")
    # Group CBF files by position and frame numbers
    logger.info("Grouping CBF files by position and frame numbers...")
    grouped_cbf = group_cbf_by_position(folder_with_raw_data)

    if grouped_cbf:
        logger.info(f"Found {len(grouped_cbf)} positions with CBF files.")
        for position, data in grouped_cbf.items():
            NAME_TEMPLATE_OF_DATA_FRAMES = data['template']
            first_image_index = data['start']
            last_image_index = data['end']
            processing_folder = os.path.join(current_data_processing_folder, position)
            os.makedirs(processing_folder, exist_ok=True)
            os.makedirs(os.path.join(processing_folder, 'xds'), exist_ok=True)
            #os.makedirs(os.path.join(processing_folder, 'autoPROC'), exist_ok=True)
            os.chmod(processing_folder, 0o777)
            os.chmod(os.path.join(processing_folder, 'xds'), 0o777)
            #os.chmod(os.path.join(processing_folder, 'autoPROC'), 0o777)
            logger.info(f"Processing position {position} with frames from {first_image_index} to {last_image_index}.")
            filling_template_wedges(folder_with_raw_data, processing_folder, ORGX, ORGY, position, 
                            first_image_index, last_image_index, REFERENCE_DATA_SET, distance_offset, 
                            NAME_TEMPLATE_OF_DATA_FRAMES, XDS_INP_template)
            
            login_node = None
            if "maxwell" not in reserved_nodes:
                login_node = reserved_nodes.split(",")[0] if "," in reserved_nodes else reserved_nodes

            logger.info(f"Login node for processing: {login_node}")
            # Running XDS
            logger.info(f"Running XDS in {processing_folder}/xds")
            xds_start(os.path.join(processing_folder,'xds'), 'xds_par',
                    user, reserved_nodes, slurm_partition, sshPrivateKeyPath, sshPublicKeyPath, login_node=login_node)

            #running autoPROC
            #logger.info(f"Running autoPROC in {processing_folder}")
            #command_for_data_processing = f"process -d {os.path.join(current_data_processing_folder,'autoPROC')} -I {folder_with_raw_data}"
            #xds_start(os.path.join(processing_folder,'autoPROC'), f'{command_for_data_processing}',
            #        user, ["maxwell"], slurm_partition, sshPrivateKeyPath, sshPublicKeyPath, login_node=login_node)
            
            # Create flag file    
            Path(processing_folder, 'flag.txt').touch()
