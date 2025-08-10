import os
import math

def calculation_high_resolution(detector_distance, wavelength, N_pixels_to_the_short_edge=2462, N_pixels_to_the_long_edge=2526, pixel_size=0.000172):
    """Calculate the high resolution based on the detector distance, wavelength, and pixel dimensions.
    Args:
        detector_distance (float): The distance from the detector to the sample in mm.
        wavelength (float): The wavelength of the X-ray in Angstroms.
        N_pixels_to_the_short_edge (int): Number of pixels in the X direction.
        N_pixels_to_the_long_edge (int): Number of pixels in the Y direction.
    Returns:
        float: The calculated high resolution in Angstroms.
    """
    
    N_pixels_to_the_short_edge //= 2
    N_pixels_to_the_long_edge //= 2

    distance_to_the_short_edge = pixel_size * N_pixels_to_the_short_edge # [m] 
    distance_to_the_long_edge = pixel_size * N_pixels_to_the_long_edge # [m]

    detector_distance /= 1000 # m

    resolution_to_the_short_edge = wavelength / (2* math.sin(0.5 * math.atan(distance_to_the_short_edge/detector_distance)))
    resolution_to_the_long_edge = wavelength / (2* math.sin(0.5 * math.atan(distance_to_the_long_edge/detector_distance)))

    return max(resolution_to_the_short_edge, resolution_to_the_long_edge)
