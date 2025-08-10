#!/usr/bin/env python3
# coding: utf8
# Written by Galchenkova M., Tolstikova A., Yefanov O., 2022-2025

"""
Example of usage:
-offline mode
python3 autoprocessing.py -config configuration.yaml --offline --u galchenm --maxwell

-offline with block of interest
python3 autoprocessing.py -config configuration.yaml --offline --f block_runs.lst --u galchenm --maxwell

-offline with force feature
python3 autoprocessing.py -config configuration.yaml --offline --f block_runs.lst --force --u galchenm --maxwell

-online mode
python3 autoprocessing.py -config configuration.yaml --path /asap3/petra3/gpfs/p09/2022/data/11016565/raw/lyso/lamdatest_lyso3/rotational_001

or

python3 autoprocessing.py --path /asap3/petra3/gpfs/p09/2022/data/11016565/raw/lyso/lamdatest_lyso3/rotational_001

This script is designed to automate the data processing workflow for the P09 beamline at DESY.
It handles both online and offline processing modes, allowing users to process data from specific runs or blocks of runs.
It reads configuration settings from a YAML file, sets up the necessary folder structure,
and executes data processing commands based on the specified experiment method (rotational or serial).

"""
import logging
import yaml
import os
import sys
from datetime import datetime
import glob
import re
from string import Template
import shutil
import subprocess
import shlex
import time
import json
import argparse
from utils.serial import serial_processing
from utils.wedges import wedges_processing
from utils.rotational import rotational_processing
from utils.log_setup import setup_logger

#This is needed to check the number of running/pending processes
CURRENT_PATH_OF_SCRIPT = os.path.dirname(__file__)
MAX_PENDING_JOBS = 200

class CustomFormatter(argparse.RawDescriptionHelpFormatter,
                    argparse.ArgumentDefaultsHelpFormatter):
    pass

def parse_cmdline_args():
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter)
    parser.add_argument('-config','--config', type=str, help="The full path to configuration file")
    parser.add_argument('--offline', default=False, action='store_true', help="Use this flag if you want to run this script for offline automatic data processing")
    parser.add_argument('--online', dest='offline', action='store_false', help="Use this flag if you want to run this script for online data processing per each run")
    parser.add_argument('--path', default=None, type=str, help="Use this flag and associate it with the current raw folder to process if you are using online mode per each run")
    parser.add_argument('--blocks', default=None, type=str, help="Use this flag and associate it with the file with list of runs you want to reprocess, for that before use --offline attribute")
    parser.add_argument('--u', default=None, type=str, help="Use this flag and associate it with the username you want to use for data processing")
    parser.add_argument('--maxwell', default=False, action='store_true', help="Use this flag and associate it with the Maxwell cluster, if you are using it")
    parser.add_argument('--force', default=False, action='store_true', help="Use this flag if you want to force rerunning in the same processed folder")
    return parser.parse_args()

def serial_start(
            folder_with_raw_data, 
            current_data_processing_folder,
            configuration,
            is_force,
            is_maxwell
            ):
    """Starts the serial data processing for the given raw data folder."""
    
    # Extracting parameters from the configuration
    user = configuration['user']
    reserved_nodes = configuration['reserved_nodes'] if not is_maxwell else "maxwell"
    slurm_partition = configuration['slurmPartition']
    sshPrivateKeyPath =  configuration["sshPrivateKeyPath"]
    sshPublicKeyPath =  configuration["sshPublicKeyPath"]
    ORGX = configuration['crystallography']['ORGX']
    ORGY = configuration['crystallography']['ORGY']
    
    #distance_offset is the offset for recalculation of real detector distance
    distance_offset = configuration['crystallography']['distance_offset'] 
    
    cell_file = configuration['crystallography']['cell_file']
    geometry_filename_template = configuration["crystallography"]["geometry_for_processing"]
    
    data_h5path = configuration['crystallography']['data_h5path'] 
    logger = logging.getLogger('app')
    logger.info(f'INFO: Running serial_processing')
    serial_processing(
        folder_with_raw_data=folder_with_raw_data,
        current_data_processing_folder=current_data_processing_folder,
        ORGX=ORGX,
        ORGY=ORGY,
        distance_offset=distance_offset,
        geometry_filename_template=geometry_filename_template,
        cell_file=cell_file,
        data_h5path=data_h5path,
        user=user,
        reserved_nodes=reserved_nodes,
        slurm_partition=slurm_partition,
        sshPrivateKeyPath=sshPrivateKeyPath,
        sshPublicKeyPath=sshPublicKeyPath
    )

def xds_start(
            folder_with_raw_data, 
            current_data_processing_folder,
            configuration,
            is_force,
            is_maxwell
            ):
    """Starts the XDS data processing for the given raw data folder."""
    
    # Extracting parameters from the configuration
    user = configuration['user']
    reserved_nodes = configuration['reserved_nodes'] if not is_maxwell else "maxwell"
    slurm_partition = configuration['slurmPartition']
    sshPrivateKeyPath = configuration["sshPrivateKeyPath"]
    sshPublicKeyPath = configuration["sshPublicKeyPath"]
    
    #ORGX and ORGY are the origin of the detector that is needed for xds data processing
    ORGX = configuration['crystallography']['ORGX']
    ORGY = configuration['crystallography']['ORGY']
    
    #distance_offset is the offset for recalculation of real detector distance required for XDS
    distance_offset = configuration['crystallography']['distance_offset'] 
    
    command_for_processing_rotational = configuration['crystallography']['command_for_processing_rotational']
    XDS_INP_template = configuration['crystallography']['XDS_INP_template']

    logger = logging.getLogger('app')
    logger.info(f'INFO: Running rotational_processing')

    rotational_processing(
        folder_with_raw_data=folder_with_raw_data, 
        current_data_processing_folder=current_data_processing_folder,
        ORGX=ORGX, 
        ORGY=ORGY, 
        distance_offset=distance_offset, 
        command_for_data_processing=command_for_processing_rotational, 
        XDS_INP_template=XDS_INP_template,
        user=user, 
        reserved_nodes=reserved_nodes, 
        slurm_partition=slurm_partition, 
        sshPrivateKeyPath=sshPrivateKeyPath, 
        sshPublicKeyPath=sshPublicKeyPath
    )

def wedges_xds_start(
            folder_with_raw_data, 
            current_data_processing_folder,
            configuration,
            is_force,
            is_maxwell,
            reference_dataset="!REFERENCE_DATA_SET"
            ):
    """Starts the data processing for the given raw data folder of wedge scan."""
    
    # Extracting parameters from the configuration
    user = configuration['user']
    reserved_nodes = configuration['reserved_nodes'] if not is_maxwell else "maxwell"
    slurm_partition = configuration['slurmPartition']
    sshPrivateKeyPath = configuration["sshPrivateKeyPath"]
    sshPublicKeyPath = configuration["sshPublicKeyPath"]
    
    #ORGX and ORGY are the origin of the detector that is needed for xds data processing
    ORGX = configuration['crystallography']['ORGX']
    ORGY = configuration['crystallography']['ORGY']
    
    #distance_offset is the offset for recalculation of real detector distance required for XDS
    distance_offset = configuration['crystallography']['distance_offset'] 
    
    command_for_processing_rotational = configuration['crystallography']['command_for_processing_rotational']
    XDS_INP_wedges_template = configuration['crystallography']['XDS_INP_wedges_template']

    logger = logging.getLogger('app')
    logger.info(f'INFO: Running wedges_processing')

    wedges_processing(folder_with_raw_data, current_data_processing_folder,
                    ORGX, ORGY, distance_offset, command_for_processing_rotational, 
                    XDS_INP_wedges_template, reference_dataset, user, reserved_nodes, 
                    slurm_partition, sshPrivateKeyPath, sshPublicKeyPath)

def run(root, configuration, is_force, is_maxwell):
    """Main processing entry point for one dataset folder."""
    logger = logging.getLogger('app')
    logger.info(f'We are here: {root}')
    raw_dir = configuration['crystallography']['raw_directory']
    proc_dir = configuration['crystallography']['processed_directory']
    user = configuration['user']
    
    files = [f for f in os.listdir(root) if os.path.isfile(os.path.join(root, f))]
    info_path = os.path.join(root, 'info.txt')
    if not (os.path.exists(info_path) and os.path.getsize(info_path) > 0 and len(files) > 1):
        logger.info(f"In {root} there is no usable info.txt file.")
        return
    
    frames_per_position = 1  # Default value
    # Read experiment method and frames/position
    with open(info_path, 'r') as f:
        method = next(f).split(':')[-1].strip()
        for line in f:
            if 'frames/position:' in line.lower():
                frames_per_position = int(line.split(':')[-1].strip())
                break

    subpath = root[len(raw_dir):].lstrip(os.sep)
    proc_subpath = os.path.join(proc_dir, subpath)

    logger.info(f'Processing {proc_subpath} with method: {method}')

    if not os.path.exists(proc_subpath):
        os.makedirs(proc_subpath, exist_ok=True)
        os.chmod(proc_subpath, 0o777)

    # Get pending SLURM jobs
    try:
        pending_cmd = f'squeue -u {user} -t pending'
        pending_jobs = subprocess.check_output(shlex.split(pending_cmd)).decode().splitlines()
    except subprocess.CalledProcessError:
        pending_jobs = []

    # Handle forced re-processing
    flag_file = os.path.join(proc_subpath, 'flag.txt')
    if is_force and os.path.exists(flag_file):
        for f in os.listdir(proc_subpath):
            f_path = os.path.join(proc_subpath, f)
            try:
                if os.path.isfile(f_path) or os.path.islink(f_path):
                    os.unlink(f_path)
                elif os.path.isdir(f_path):
                    shutil.rmtree(f_path)
            except Exception as e:
                logger.warning(f'Failed to delete {f_path}: {e}')
        logger.info(f'Cleared old results in {proc_subpath}')

    # Skip if already processed or SLURM overloaded
    if os.path.exists(flag_file) or len(pending_jobs) > MAX_PENDING_JOBS or \
        os.path.exists(os.path.join(proc_subpath, 'CORRECT.LP')) or \
        os.path.exists(os.path.join(proc_subpath, 'XYCORR.LP')):
        logger.info(f'{proc_subpath} is skipped')
        

    if method == 'rotational':
        logger.info(f"XDS: {root}")
        xds_start(root, proc_subpath, configuration, is_force, is_maxwell)
    elif method == 'grid step' and frames_per_position > 1:
        logger.info(f"WEDGES: {root}")
        wedges_xds_start(root, proc_subpath, configuration, is_force, is_maxwell, reference_dataset="!reference_dataset")
    else:
        logger.info(f"SERIAL: {root}")
        serial_start(root, proc_subpath, configuration, is_force, is_maxwell)


def find_and_parse_metadata(base_path):
    """Finds and parses the first valid beamtime-metadata*.json file in the given directory.
    This function searches recursively for files matching the pattern beamtime-metadata*.json
    and returns the first one that contains the required fields: beamtimeId and onlineAnalysis.
    If no such file is found, it raises a FileNotFoundError.
    If the file is found but does not contain the required fields, it skips that file and continues searching.
    If a valid file is found, it returns a dictionary with the beamtimeId, reservedNodes,
    sshPrivateKeyPath, sshPublicKeyPath, userAccount, and the file path itself
    as an optional field.
    Parameters:
        base_path (str): The root directory where the beamtime metadata files are stored.
    Returns:
        dict: A dictionary containing the beamtimeId, reservedNodes, sshPrivateKeyPath,
        beamtimeId: 11016750
        "slurmPartition": "ponline_p09"
        reservedNodes: 'max-p3a020'
        sshPrivateKeyPath: shared/id_rsa
        sshPublicKeyPath: shared/id_rsa.pub
        userAccount: bttest04
    """
    logger = logging.getLogger('app')
    logger.info("Parsing metadata...")
    # Recursive search for files like beamtime-metadata*.json
    base_path = base_path.split('/raw')[0] if '/raw' in base_path else base_path
    
    if not os.path.exists(base_path):  # Check if the base path exists
        raise FileNotFoundError(f"The base path {base_path} does not exist.")
    pattern = os.path.join(f"{base_path}", "beamtime-metadata*.json")
    json_files = glob.glob(pattern, recursive=True)
    
    for json_file in json_files:
        try:
            with open(json_file, "r") as f:
                data = json.load(f)

            # Ensure required keys exist
            if all(k in data for k in ["beamtimeId", "onlineAnalysis"]):
                online = data["onlineAnalysis"]
                result = {
                    "beamtimeId": data["beamtimeId"],
                    "corePath": data["corePath"],
                    "reservedNodes": ",".join(online.get("reservedNodes", [])) if online.get("reservedNodes") else "",
                    "sshPrivateKeyPath": online.get("sshPrivateKeyPath"),
                    "sshPublicKeyPath": online.get("sshPublicKeyPath"),
                    "userAccount": online.get("userAccount"),
                    "file": json_file,
                    "slurmPartition": online.get("slurmPartition")
                }
                return result 

        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.info(f"Skipping {json_file}: {e}")
            continue

    raise FileNotFoundError("No valid beamtime-metadata*.json file found with required fields.")

def filling_configuration_file(configuration_file_template, processed_directory=None):
    """
    Fills a YAML configuration template if it contains placeholders.

    Parameters:
    - configuration_file_template (str): Path to the YAML template file.
    - processed_directory (str or None): Path to save filled config. If None, will be extracted.

    Returns:
    - str: Path to the actual configuration YAML file to use.
    """
    logger = logging.getLogger('app')
    
    with open(configuration_file_template, "r") as f:
        template_text = f.read()
    
    # Check if it's a template (contains $PLACEHOLDER-style variables)
    if "$" not in template_text:
        return configuration_file_template  # Not a template

    templates_folder = os.path.join(CURRENT_PATH_OF_SCRIPT, 'templates')
    values = {
        "XDS_INP_template": os.path.join(templates_folder, 'XDS.INP'),
        "XDS_INP_wedges_template": os.path.join(templates_folder, 'XDS_WEDGES.INP'),
        "geometry_for_processing": os.path.join(templates_folder, 'pilatus6M.geom')
    }

    filled_template = Template(template_text).safe_substitute(values)

    # Try to extract processed_directory from filled content if not given
    if processed_directory is None:
        try:
            temp_config = yaml.safe_load(filled_template)
            processed_directory = temp_config['crystallography']['processed_directory']
            os.makedirs(processed_directory, exist_ok=True)
            os.chmod(processed_directory, 0o777)
            
        except Exception as e:
            raise ValueError("Failed to determine processed_directory") from e

    now = datetime.now()
    output_file = os.path.join(processed_directory, f"filled_config_{now.month}-{now.year}.yaml")

    with open(output_file, "w") as f:
        f.write(filled_template)
    
    logger.info(f"Filled configuration template saved to {output_file}")
    return output_file

def creating_folder_structure(
    processed_directory
):
    """
    Creates the necessary folder structure for raw and processed data.

    Parameters:
    - processed_directory (str): Path to the processed data directory.
    """

    if not os.path.exists(processed_directory):
        os.makedirs(processed_directory)
        os.chmod(processed_directory, 0o777)

def main():
    """Main entry point for the autoprocessing script."""
    
    #reading configuration file
    args = parse_cmdline_args()
    configuration_file = args.config if args.config is not None else f'{CURRENT_PATH_OF_SCRIPT}/templates/configuration_template.yaml'

    #If the configuration file is a template, we fill it with values from the beamtime JSON file
    configuration_file = filling_configuration_file(configuration_file) 

    with open(configuration_file,'r') as file:
        configuration = yaml.safe_load(file)

    raw_directory = configuration['crystallography']['raw_directory']
    processed_directory = configuration['crystallography']['processed_directory']
    
    
    is_force = args.force
    is_maxwell = args.maxwell

    
    while True: 
        #Wait while the directory with raw data appeared
        if os.path.exists(raw_directory):
            break

    #Creating the folder structure for processed data
    creating_folder_structure(processed_directory)

    setup_logger(processed_directory)
    logger = logging.getLogger('app')
    
    result_parsed_metadata = find_and_parse_metadata(raw_directory)
    
    beamtimeId = result_parsed_metadata['beamtimeId'] 
    corePath = result_parsed_metadata['corePath']
    reservedNodes = result_parsed_metadata['reservedNodes'] if not args.maxwell is None else ["maxwell"]
    sshPrivateKeyPath = os.path.join(raw_directory.split('/raw')[0], result_parsed_metadata['sshPrivateKeyPath'])
    sshPublicKeyPath = result_parsed_metadata['sshPublicKeyPath']
    user = result_parsed_metadata['userAccount'] if args.u is None else args.u
    slurmPartition = result_parsed_metadata['slurmPartition'] 
    configuration.update({
    "beamtimeId": beamtimeId,
    "reserved_nodes": reservedNodes,
    "sshPrivateKeyPath": sshPrivateKeyPath,
    "sshPublicKeyPath": sshPublicKeyPath,
    "user": user,
    "slurmPartition": slurmPartition,
    })
    
    logger.info(f"Configuration: {configuration}")
    logger.info(f"Beamtime ID: {beamtimeId}")
    logger.info(f"Core Path: {corePath}")
    logger.info(f"Reserved Nodes: {reservedNodes}")
    
    
    if args.offline:
        to_process = []
        blocks_of_files = args.blocks
        if blocks_of_files is not None:
            with open(blocks_of_files, 'r') as file:
                for line in file:
                    line = line.strip()
                    if len(line) > 0 and line not in to_process: 
                        to_process.append(line)
            

            for root, dirs, files in os.walk(raw_directory):
                for pattern in to_process:
                    
                    if pattern in root[len(raw_directory):]:
                        run(root, configuration, is_force, is_maxwell)
                        logger.info(f'INFO: Processed {root}')
        else:
            while True: #main cycle for inspection folders and running data processing
                for root, dirs, files in os.walk(raw_directory):
                    run(root, configuration, is_force, is_maxwell)
                    logger.info(f'INFO: Processed {root}')
                time.sleep(2)
    else:
        if args.path is None:
            logger.error('ERROR: YOU HAVE TO GIVE THE ABSOLUTE PATH TO THE RAW FOLDER YOU ARE GOING TO PROCESS IF YOU ARE IN THIS MODE!')
        else:
            logger.info("Processing single folder")
            run(args.path, configuration, is_force, is_maxwell)
            logger.info(f'INFO: Processed {args.path}')

if __name__ == "__main__":
    main()