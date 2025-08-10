import os
import shutil
from pathlib import Path
from string import Template
from utils.extract import extract_value_from_info
from utils.UC import parse_UC_file, parse_cryst1_and_spacegroup_number
from utils.cbf_head_reader import retrieving_info_from_cbf
from utils.resolution import calculation_high_resolution
import glob

def filling_template_rotational(folder_with_raw_data, current_data_processing_folder, ORGX=0, ORGY=0,
                    DISTANCE_OFFSET=0, NAME_TEMPLATE_OF_DATA_FRAMES='blabla',
                    command_for_data_processing='xds_par', XDS_INP_template=None):
    """Fills the geometry template with parameters extracted from info.txt and prepares for data processing."""
    folder_with_raw_data = Path(folder_with_raw_data)
    current_data_processing_folder = Path(current_data_processing_folder)

    shutil.copy(XDS_INP_template, current_data_processing_folder / 'xds/template.INP')

    info_path = folder_with_raw_data / 'info.txt'
    if not info_path.exists() or info_path.stat().st_size == 0:
        print(f"Error: info.txt not found or empty in {folder_with_raw_data}")
        return

    DETECTOR_DISTANCE = extract_value_from_info(info_path, "distance") + DISTANCE_OFFSET
    
    ORGX = ORGX or extract_value_from_info(info_path, "ORGX")
    ORGY = ORGY or extract_value_from_info(info_path, "ORGY")
    NFRAMES = extract_value_from_info(info_path, "frames", fallback=1, is_float=False)
    STARTING_ANGLE = extract_value_from_info(info_path, "start angle")
    OSCILLATION_RANGE = extract_value_from_info(info_path, "degrees/frame")
    WAVELENGTH = extract_value_from_info(info_path, "wavelength")

    cell_matches = glob.glob(str(Path(folder_with_raw_data) / "*.cell"))
    if cell_matches:
        cell_file = cell_matches[0]
        a, b, c, alpha, beta, gamma, SPACE_GROUP_NUMBER = parse_UC_file(cell_file)
    else:
        pdb_matches = glob.glob(str(Path(folder_with_raw_data) / "*.pdb"))
        if pdb_matches:
            cell_file = pdb_matches[0]
            a, b, c, alpha, beta, gamma, SPACE_GROUP_NUMBER = parse_UC_file(cell_file)
        else:
            a, b, c, alpha, beta, gamma, SPACE_GROUP_NUMBER = None, None, None, None, None, None, 0
    template_data = {
        "DETECTOR_DISTANCE": DETECTOR_DISTANCE,
        "ORGX": ORGX,
        "ORGY": ORGY,
        "NFRAMES": NFRAMES,
        "NAME_TEMPLATE_OF_DATA_FRAMES": NAME_TEMPLATE_OF_DATA_FRAMES,
        "STARTING_ANGLE": STARTING_ANGLE,
        "OSCILLATION_RANGE": OSCILLATION_RANGE,
        "WAVELENGTH": WAVELENGTH,
        "SPACE_GROUP_NUMBER": f"SPACE_GROUP_NUMBER = {SPACE_GROUP_NUMBER}" if SPACE_GROUP_NUMBER else "!SPACE_GROUP_NUMBER",
        "UNIT_CELL_CONSTANTS": f"UNIT_CELL_CONSTANTS = {a:.2f} {b:.2f} {c:.2f} {alpha:.2f} {beta:.2f} {gamma:.2f}" if None not in [a, b, c, alpha, beta, gamma] else "!UNIT_CELL_CONSTANTS",
    }

    with open(current_data_processing_folder / 'xds/template.INP', 'r') as f:
        src = Template(f.read())
    with open(current_data_processing_folder / 'xds/XDS.INP', 'w') as f:
        f.write(src.substitute(template_data))
    os.chmod(current_data_processing_folder/ 'xds/XDS.INP', 0o777)
    os.remove(current_data_processing_folder / 'xds/template.INP')
    

def filling_template_serial(folder_with_raw_data, current_data_processing_folder,
                    geometry_filename_template, data_h5path,
                    ORGX=0, ORGY=0, DISTANCE_OFFSET=0,
                    cell_file=None):
    """Fills the geometry template with parameters extracted from info.txt and prepares for data processing."""
    
    os.chdir(current_data_processing_folder)
    template_geom_path = Path(current_data_processing_folder) / 'template.geom'
    shutil.copy(geometry_filename_template, template_geom_path)

    info_path = Path(folder_with_raw_data) / 'info.txt'
    if not info_path.exists() or info_path.stat().st_size == 0:
        print(f"No valid info.txt found in {folder_with_raw_data}")
        return

    with open(info_path) as f:
        content = f.read()

    DETECTOR_DISTANCE = extract_value_from_info(info_path, "distance") + DISTANCE_OFFSET
    DETECTOR_DISTANCE /= 1000
    ORGX = ORGX or extract_value_from_info(info_path, "ORGX")
    ORGY = ORGY or extract_value_from_info(info_path, "ORGY")
    NFRAMES = extract_value_from_info(info_path, "frames", fallback=1, is_float=False)
    STARTING_ANGLE = extract_value_from_info(info_path, "start angle")
    OSCILLATION_RANGE = extract_value_from_info(info_path, "degrees/frame")
    WAVELENGTH = extract_value_from_info(info_path, "wavelength")
    PHOTON_ENERGY = 12400 / WAVELENGTH
    
    if not cell_file:
        # Try to find a .cell or .pdb file in the raw data folder
        cell_matches = glob.glob(str(Path(folder_with_raw_data) / "*.cell"))
        if cell_matches:
            cell_file = cell_matches[0]
        else:
            pdb_matches = glob.glob(str(Path(folder_with_raw_data) / "*.pdb"))
            if pdb_matches:
                cell_file = pdb_matches[0]

    indexing_method = extract_value_from_info(info_path, "indexing_method", fallback="mosflm-latt-nocell", is_string=True)
    template_data = {
        "DETECTOR_DISTANCE": DETECTOR_DISTANCE,
        "ORGX": -ORGX,
        "ORGY": -ORGY,
        "PHOTON_ENERGY": PHOTON_ENERGY,
        "data_h5path": data_h5path
    }

    with open(template_geom_path, 'r') as f:
        src = Template(f.read())

    with open('geometry.geom', 'w') as monitor_file:
        monitor_file.write(src.substitute(template_data))

    template_geom_path.unlink()
    
    return indexing_method, cell_file, NFRAMES


def filling_template_wedges(folder_with_raw_data, current_data_processing_folder, ORGX=0, ORGY=0, position=None,
                    first_image_index=1, last_image_index=10000, REFERENCE_DATA_SET="!REFERENCE_DATA_SET", DISTANCE_OFFSET=0, 
                    NAME_TEMPLATE_OF_DATA_FRAMES='blabla', XDS_INP_template=None
                    ):
    """Fills the geometry template with parameters extracted from info.txt and prepares for data processing."""
    folder_with_raw_data = Path(folder_with_raw_data)
    current_data_processing_folder = Path(current_data_processing_folder)
    shutil.copy(XDS_INP_template, current_data_processing_folder / 'xds/template.INP')

    info_path = folder_with_raw_data / 'info.txt'
    if not info_path.exists() or info_path.stat().st_size == 0:
        print(f"Error: info.txt not found or empty in {folder_with_raw_data}")
        return

    DETECTOR_DISTANCE = extract_value_from_info(info_path, "distance") + DISTANCE_OFFSET
    
    ORGX = ORGX or extract_value_from_info(info_path, "ORGX")
    ORGY = ORGY or extract_value_from_info(info_path, "ORGY")
    #NFRAMES = extract_value_from_info(info_path, "frames", fallback=1, is_float=False)
    OSCILLATION_RANGE = extract_value_from_info(info_path, "degrees/frame")
    WAVELENGTH = extract_value_from_info(info_path, "wavelength")

    cell_matches = glob.glob(str(Path(folder_with_raw_data) / "*.cell"))
    if cell_matches:
        cell_file = cell_matches[0]
        a, b, c, alpha, beta, gamma, SPACE_GROUP_NUMBER = parse_UC_file(cell_file)
    else:
        pdb_matches = glob.glob(str(Path(folder_with_raw_data) / "*.pdb"))
        if pdb_matches:
            cell_file = pdb_matches[0]
            a, b, c, alpha, beta, gamma, SPACE_GROUP_NUMBER = parse_UC_file(cell_file)
        else:
            a, b, c, alpha, beta, gamma, SPACE_GROUP_NUMBER = None, None, None, None, None, None, 0
            
    if REFERENCE_DATA_SET in ["!REFERENCE_DATA_SET", "None"]:
        REFERENCE_DATA_SET_matches = glob.glob(str(Path(folder_with_raw_data) / "XDS_ASCII.HKL"))
        if REFERENCE_DATA_SET_matches:
            REFERENCE_DATA_SET = REFERENCE_DATA_SET_matches[0]
        else:
            REFERENCE_DATA_SET = "!REFERENCE_DATA_SET"
    
    cbf_to_open = NAME_TEMPLATE_OF_DATA_FRAMES.replace("?????", "00001")
    N_PIXELS_TO_THE_SHORT_EDGE, N_PIXELS_TO_THE_LONG_EDGE, pixel_size = retrieving_info_from_cbf(cbf_to_open)
    high_res = calculation_high_resolution(DETECTOR_DISTANCE, WAVELENGTH, N_PIXELS_TO_THE_SHORT_EDGE, N_PIXELS_TO_THE_LONG_EDGE, pixel_size)
    template_data = {
        "DETECTOR_DISTANCE": DETECTOR_DISTANCE,
        "ORGX": ORGX,
        "ORGY": ORGY,
        "NAME_TEMPLATE_OF_DATA_FRAMES": NAME_TEMPLATE_OF_DATA_FRAMES,
        "OSCILLATION_RANGE": OSCILLATION_RANGE,
        "WAVELENGTH": WAVELENGTH,
        "first_image_index": first_image_index,
        "last_image_index": last_image_index, #assume the crystal is not dead by this time
        "REFERENCE_DATA_SET": REFERENCE_DATA_SET,
        "SPACE_GROUP_NUMBER": f"SPACE_GROUP_NUMBER = {SPACE_GROUP_NUMBER}" if SPACE_GROUP_NUMBER else "!SPACE_GROUP_NUMBER",
        "UNIT_CELL_CONSTANTS": f"UNIT_CELL_CONSTANTS = {a:.2f} {b:.2f} {c:.2f} {alpha:.2f} {beta:.2f} {gamma:.2f}" if None not in [a, b, c, alpha, beta, gamma] else "!UNIT_CELL_CONSTANTS",
        "INCLUDE_RESOLUTION_RANGE": f"50.0 {high_res}", #default parameters
        "ROTATION_AXIS": "1.0 0.0 0.0" if int(position) % 2 == 0 else "-1.0 0.0 0.0"
    }

    with open(current_data_processing_folder / 'xds/template.INP', 'r') as f:
        src = Template(f.read())
    with open(current_data_processing_folder / 'xds/XDS.INP', 'w') as f:
        f.write(src.substitute(template_data))

    os.remove(current_data_processing_folder / 'xds/template.INP')
