# AutoProcessing for P09 Beamline

This repository contains the **auto-processing script** for the P09 beamline data at DESY PETRA III. The script automates data processing workflows, supporting both **online** and **offline** modes, to process crystallography experimental data.

---

## Overview

The `autoprocessing.py` script is designed to handle automated data processing at the P09 beamline, which specializes in resonant scattering and diffraction experiments.

### Features

* Supports **online** mode (process data as it is acquired).
* Supports **offline** mode (batch processing of raw data folders or specified run blocks).
* Reads configuration parameters from a YAML file.
* Supports forced re-processing of datasets.
* Integrates with cluster job submission (SLURM) and can run on the Maxwell cluster.
* Handles different experiment methods: *rotational* and *serial*.
* Automatically creates necessary folder structures.
* Parses metadata from beamtime JSON files to configure processing environment.

---

## Usage

Run the script using Python 3 with the following options:

### Offline Mode

Process data offline, either for all runs or a block of runs:

```bash
python3 autoprocessing.py -config configuration.yaml --offline --u <username> --maxwell
```

Process a specific block of runs (list of runs in a text file):

```bash
python3 autoprocessing.py -config configuration.yaml --offline --blocks block_runs.lst --u <username> --maxwell
```

Force re-processing on the same data folder:

```bash
python3 autoprocessing.py -config configuration.yaml --offline --blocks block_runs.lst --force --u <username> --maxwell
```

### Online Mode

Process data as it arrives by specifying the raw data folder:

```bash
python3 autoprocessing.py --path /path/to/raw/data/folder
```

or with a config file:

```bash
python3 autoprocessing.py -config configuration.yaml --path /path/to/raw/data/folder
```

---

## Configuration

The script expects a YAML configuration file specifying paths and parameters, such as:

* Raw data directory
* Processed data directory
* Cluster settings (SLURM partition, reserved nodes)
* Crystallography-specific parameters (detector origin, distance offset)
* Paths to SSH keys for cluster access

A template configuration file (`configuration_template.yaml`) is included and automatically filled with beamtime metadata during runtime.

---

## Requirements

* Python 3
* Python packages: `pyyaml`, `argparse`
* Access to SLURM cluster for job submission
* SSH keys configured for cluster access
* The `turbo-index-p09`, `xds.py`, and `serial.py` scripts in the same directory

---

## Logging

The script creates a log file `Auto-processing-P09-beamline.log` in the current working directory, capturing detailed information about processing steps and status.

---

## Authors

* M. Galchenkova
* A. Tolstikova
* O. Yefanov

(2022-2025)

