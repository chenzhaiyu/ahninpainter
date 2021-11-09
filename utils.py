"""
General utility functions.
"""

import glob
import warnings

import numpy as np
from osgeo import gdal
from pathlib import Path
from PIL import Image


def nodata(image):
    """
    Return no-data flag, and mask of no-data pixels, if any.
    :param image: gdal image
    :return: whether given image has no-data pixels
    """
    band = image.GetRasterBand(1)
    value = band.GetNoDataValue()
    array = band.ReadAsArray()
    has_nodata = value in array
    mask_nodata = np.zeros(array.shape)
    if has_nodata:
        mask_nodata = value != array
    return has_nodata, mask_nodata


def convert_image(input_path, output_path, suffix='.jpg', backend='gdal', n_channels=1,
                  min_value=None, max_value=None, exclude_nodata=False, mode='I;16'):
    """
    Convert images to a specified format.
    :param mode: image mode if as greyscale
    :param input_path: path to input image
    :param output_path: path to output image
    :param suffix: 'jpg'
    :param backend: 'pillow' or 'gdal'
    :param n_channels: number of channels to output
    :param min_value: min value to scale to
    :param max_value: max value to scale to
    :param exclude_nodata: exclude images with no-data pixels if set True
    :return: None
    """
    assert suffix == ".jpg" or suffix == ".jpeg" or suffix == ".png"
    if exclude_nodata and backend != 'gdal':
        backend = 'gdal'
        warnings.warn('excluded_nodata enabled. backend switched to gdal')

    Path(output_path).parent.mkdir(exist_ok=True, parents=True)

    if backend == 'gdal':
        image = gdal.Open(input_path)
        has_nodata, _ = nodata(image)
        if exclude_nodata and has_nodata:
            # print(f'{input_path} contains no-data pixel(s). excluded from processing.')
            return input_path

        options_list = [
            f'-ot Byte',
            f'-of JPEG',
            f'-b {n_channels}',
            f'-scale {min_value} {max_value}' if min_value is not None and max_value is not None else f'-scale'
        ]
        options_string = " ".join(options_list)

        gdal.Translate(
            output_path,
            input_path,
            options=options_string
        )

    elif backend == 'pillow':
        image = Image.open(input_path)
        if n_channels == 1:
            image = image.convert(mode)
        elif n_channels == 3:
            image = image.convert('RGB')
        else:
            raise ValueError(f'n_channels has to be 1 or 3. got {n_channels} instead.')
        image.save(output_path, quality=100)
    else:
        raise ValueError(f'backend has to be gdal or pillow. got {backend} instead.')


def glob_tile_ids(pattern):
    """
    Get available tile folders.
    :param pattern: pattern path to search for
    :return: list of available file paths
    """
    tile_ids = glob.glob(pattern)
    return [Path(i).stem for i in tile_ids]


if __name__ == '__main__':
    pass
