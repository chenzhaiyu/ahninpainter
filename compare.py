"""
Compare AHN 3/4 raster.
"""

import multiprocessing
from pathlib import Path
import logging
import shutil

import hydra
import numpy as np
from osgeo import gdal
from tqdm import tqdm
from omegaconf import DictConfig

log = logging.getLogger(__name__)


def difference(path_reference, path_target):
    """
    Compute pixel-wise absolute difference between two raster.
    No-data pixels are ignored.
    :param path_reference: path to reference raster
    :param path_target: path to target raster
    :return: difference
    """
    reference = gdal.Open(str(path_reference))
    target = gdal.Open(str(path_target))

    if (reference.RasterXSize == 1 and reference.RasterYSize == 1) or \
            (target.RasterXSize == 1 and target.RasterYSize == 1):
        # empty tif
        # this is no longer useful for the updated geoflow where footprint is used to determine the image size
        return np.array([0])

    # they should have identical dimension to compare
    assert reference.RasterXSize == target.RasterXSize and \
           reference.RasterYSize == target.RasterYSize

    band_reference = reference.GetRasterBand(1)
    array_reference = band_reference.ReadAsArray()

    band_target = target.GetRasterBand(1)
    array_target = band_target.ReadAsArray()

    nodata_reference = band_reference.GetNoDataValue()
    nodata_target = band_target.GetNoDataValue()

    # zero the nodata pixels for both raster
    nodata_mask = np.bitwise_or(array_reference == nodata_reference, array_target == nodata_target)
    array_reference[nodata_mask] = 0
    array_target[nodata_mask] = 0

    return np.absolute(array_target - array_reference)


def changed(difference_array, **kwargs):
    """
    Verdict if the building is changed.
    :param difference_array: array of difference
    :return: True/False
    """
    changed_ = {}
    if 'mean' in kwargs:
        mean = np.mean(difference_array)
        changed_['mean'] = True if mean > kwargs['mean'] else False
    if 'maxima' in kwargs:
        maxima = np.max(difference_array)
        changed_['maxima'] = True if maxima > kwargs['maxima'] else False
    if 'sum' in kwargs:
        sum_ = np.sum(difference_array)
        changed_['sum'] = True if sum_ > kwargs['sum'] else False
    if 'count_larger_than' in kwargs:
        count_larger_than = np.sum(difference_array > kwargs['count_larger_than'][0])
        changed_['count_larger_than'] = True if count_larger_than > kwargs['count_larger_than'][1] else False
    if 'percentage_larger_than' in kwargs:
        percentage_larger_than = np.sum(difference_array > kwargs['percentage_larger_than'][0]) / difference_array.size
        changed_['percentage_larger_than'] = True if percentage_larger_than > kwargs['percentage_larger_than'][
            1] else False
    return changed_


def copy_tif(path_reference, path_target, reference_dir, target_dir, save_dir):
    """
    Copy reference and target to specified dir if they are changed.
    :return: None
    """
    # create dir structure as tile_number/raster_number
    path_reference_copy = save_dir / path_reference.relative_to(reference_dir).with_suffix('.reference.tif')
    path_target_copy = save_dir / path_target.relative_to(target_dir).with_suffix('.target.tif')

    path_reference_copy.parent.mkdir(exist_ok=True, parents=True)
    path_target_copy.parent.mkdir(exist_ok=True, parents=True)

    try:
        shutil.copyfile(path_reference, path_reference_copy)
        shutil.copyfile(path_target, path_target_copy)
    except shutil.SameFileError:
        log.error('Source and destination represents the same file.')
    except IsADirectoryError:
        log.error('Destination is a directory.')
    except PermissionError:
        log.error('Permission denied.')


def compare(args):
    """
    Compare reference and target.
    :param args: path_reference, path_target
    :return: whether the building is changed
    """
    path_reference, path_target, reference_dir, target_dir, save_dir, metrics = args
    difference_array = difference(path_reference, path_target)
    changed_ = changed(difference_array, **metrics)

    if True in changed_.values():
        copy_tif(path_reference, path_target, reference_dir, target_dir, save_dir)
        return changed_, path_reference


@hydra.main(config_path='conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    Compare the difference with multi-processing.
    :return: None
    """
    reference_dir = Path(cfg.compare.reference_dir)
    target_dir = Path(cfg.compare.target_dir)
    reference_paths = reference_dir.rglob('*' + 'tif')

    args = []
    for path_reference in reference_paths:
        path_target = target_dir / path_reference.relative_to(reference_dir).with_suffix('.tif')
        args.append((path_reference, path_target, cfg.compare.reference_dir, cfg.compare.target_dir,
                     cfg.compare.save_dir, cfg.compare.metrics))

    pool = multiprocessing.Pool(processes=cfg.threads if cfg.threads else multiprocessing.cpu_count())

    # https://stackoverflow.com/a/40133278
    counter = 0
    for r in tqdm(pool.imap_unordered(compare, args), total=len(args)):
        if r is not None:
            counter += 1
            if cfg.compare.verbose:
                log.info(str(r[0]) + ' for ' + str(r[1]))
    log.info(f' {counter} / {len(args)} has changed.')


if __name__ == '__main__':
    multi_run()
