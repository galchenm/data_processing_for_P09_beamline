#!/usr/bin/env python3
# coding: utf8
# Written by Galchenkova M., Tolstikova A., Yefanov O., 2022 (revised)

import os
import sys
import glob
import gemmi
import re
import shutil
import subprocess
from string import Template
from pathlib import Path
import shlex
from utils.nodes import are_the_reserved_nodess_overloaded
from utils.templates import filling_template_rotational
from utils.log_setup import setup_logger

def build_ssh_command(USER, sshPrivateKeyPath, login_node):
    return (
        f"/usr/bin/ssh -o BatchMode=yes -o CheckHostIP=no -o StrictHostKeyChecking=no "
        f"-o GSSAPIAuthentication=no -o GSSAPIDelegateCredentials=no -o PasswordAuthentication=no "
        f"-o PubkeyAuthentication=yes -o PreferredAuthentications=publickey -o ConnectTimeout=10 "
        f"-l {USER} -i {sshPrivateKeyPath} {login_node}"
    )

def build_sbatch_script(job_name, command_for_data_processing, out_file, err_file,
                        partition, reservation=None, time=None, mem=None, nice=None):
    lines = [
        "#!/bin/sh\n",
        f"#SBATCH --job-name={job_name}\n",
        f"#SBATCH --partition={partition}\n",
        "#SBATCH --nodes=1\n",
        f"#SBATCH --output={out_file}\n",
        f"#SBATCH --error={err_file}\n",
    ]
    if reservation:
        lines.append(f"#SBATCH --reservation={reservation}\n")
    if time:
        lines.append(f"#SBATCH --time={time}\n")
    if mem:
        lines.append(f"#SBATCH --mem={mem}\n")
    if nice:
        lines.append(f"#SBATCH --nice={nice}\n")

    lines += [
        "source /etc/profile.d/modules.sh\n",
        "module load xray autoproc\n",
        f"{command_for_data_processing}\n"
    ]
    return lines

def xds_start(current_data_processing_folder, command_for_data_processing,
            USER, reserved_nodes, SLURM_PARTITION,
            sshPrivateKeyPath, sshPublicKeyPath,
            login_node=None):
    """Prepare and submit the XDS job via SLURM."""

    folder = Path(current_data_processing_folder)
    job_name = folder.name
    slurmfile = folder / f"{job_name}_XDS.sh"
    out_file = folder / f"{job_name}_XDS.out"
    err_file = folder / f"{job_name}_XDS.err"

    is_maxwell = "maxwell" in reserved_nodes
    ssh_command = ""

    if login_node:
        ssh_command = build_ssh_command(USER, sshPrivateKeyPath, login_node)

    if not is_maxwell:
        if not are_the_reserved_nodes_overloaded(reserved_nodes):
            sbatch_script = build_sbatch_script(
                job_name, command_for_data_processing, out_file, err_file,
                partition=SLURM_PARTITION, reservation=reserved_nodes
            )
        else:
            sbatch_script = build_sbatch_script(
                job_name, command_for_data_processing, out_file, err_file,
                partition="allcpu,upex,short"
            )
    else:
        sbatch_script = build_sbatch_script(
            job_name, command_for_data_processing, out_file, err_file,
            partition="allcpu,upex", time="12:00:00", mem="500000", nice="100"
        )

    with open(slurmfile, 'w') as fh:
        fh.writelines(sbatch_script)
    os.chmod(slurmfile, 0o755)

    # Submit the job
    submit_command = f'{ssh_command} "sbatch {slurmfile}"' if ssh_command else f'sbatch {slurmfile}'
    subprocess.run(submit_command, shell=True, check=True)

def rotational_processing(folder_with_raw_data, current_data_processing_folder, ORGX, ORGY,
DISTANCE_OFFSET, command_for_data_processing, XDS_INP_template,
USER, reserved_nodes, SLURM_PARTITION, sshPrivateKeyPath, sshPublicKeyPath):
    """Main function to process the command line arguments and call the filling_template_rotational function."""

    # Setup logger
    logger = setup_logger(log_dir=current_data_processing_folder.split('processed')[0] + 'processed', log_name="rotational_processing")
    logger.info("Starting rotational data processing...")
    logger.info(f"Processing folder: {folder_with_raw_data}")
    logger.info(f"Current data processing folder: {current_data_processing_folder}")
    logger.info(f"XDS template: {XDS_INP_template}")
    
    os.makedirs(current_data_processing_folder, exist_ok=True)
    os.makedirs(os.path.join(current_data_processing_folder, 'xds'), exist_ok=True)
    os.makedirs(os.path.join(current_data_processing_folder, 'autoPROC'), exist_ok=True)

    ORGX = float(ORGX) if ORGX != "None" else 0
    ORGY = float(ORGY) if ORGY != "None" else 0
    DISTANCE_OFFSET = float(DISTANCE_OFFSET)

    res = [
        os.path.join(folder_with_raw_data, file)
        for file in os.listdir(folder_with_raw_data)
        if os.path.isfile(os.path.join(folder_with_raw_data, file)) and (
            (file.endswith(".h5") or file.endswith(".cxi")) and 'master' in file or file.endswith(".cbf"))
    ]
    res.sort()

    if res:
        NAME_TEMPLATE_OF_DATA_FRAMES = res[0]
        if 'master' in NAME_TEMPLATE_OF_DATA_FRAMES:
            NAME_TEMPLATE_OF_DATA_FRAMES = re.sub(r'_master\.', '_??????.', NAME_TEMPLATE_OF_DATA_FRAMES)
        else:
            NAME_TEMPLATE_OF_DATA_FRAMES = re.sub(r'\d+\.', lambda m: '?' * (len(m.group()) - 1) + '.', NAME_TEMPLATE_OF_DATA_FRAMES)

        filling_template_rotational(folder_with_raw_data, current_data_processing_folder, ORGX, ORGY,
                        DISTANCE_OFFSET, NAME_TEMPLATE_OF_DATA_FRAMES, command_for_data_processing,
                        XDS_INP_template)
        login_node = None
        if "maxwell" not in reserved_nodes:
            login_node = reserved_nodes.split(",")[0] if "," in reserved_nodes else reserved_nodes

        logger.info(f"Running XDS in {current_data_processing_folder}")
    
        xds_start(os.path.join(current_data_processing_folder,'xds'), 'xds_par',
        USER, reserved_nodes, SLURM_PARTITION, sshPrivateKeyPath, sshPublicKeyPath, login_node=login_node)
        #running autoPROC
        logger.info(f"Running autoPROC in {current_data_processing_folder}")
    
        command_for_data_processing = f"process -d {os.path.join(current_data_processing_folder,'autoPROC')} -I {folder_with_raw_data}"
        xds_start(os.path.join(current_data_processing_folder,'autoPROC'), f'{command_for_data_processing}',
                USER, "maxwell", SLURM_PARTITION, sshPrivateKeyPath, sshPublicKeyPath, login_node=login_node)
        
    
        Path(current_data_processing_folder, 'flag.txt').touch()
