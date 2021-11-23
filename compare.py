"""
Compare AHN 3/4 raster.
"""

import multiprocessing
from pathlib import Path
import logging

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
        return np.array([0])

    # they should have identical dimension to compare
    assert reference.RasterXSize == target.RasterXSize and \
           reference.RasterYSize == target.RasterYSize

    band_reference = reference.GetRasterBand(1)
    array_reference = band_reference.ReadAsArray()

    band_target = reference.GetRasterBand(1)
    array_target = band_target.ReadAsArray()

    nodata_reference = band_reference.GetNoDataValue()
    nodata_target = band_target.GetNoDataValue()

    # zero the nodata pixels for both raster
    nodata_mask = np.bitwise_or(array_reference == nodata_reference, array_target == nodata_target)
    array_reference[nodata_mask] = 0
    array_target[nodata_mask] = 0

    return np.absolute(array_target - array_reference)


def change(difference_array, path_reference, **kwargs):
    """
    Verdict if the building is changed.
    :param difference_array: array of difference
    :param path_reference: path to reference
    :return: True/False
    """
    changed = {}
    if 'mean' in kwargs:
        mean = np.mean(difference_array)
        changed['mean'] = True if mean > kwargs['mean'] else False
    if 'maxima' in kwargs:
        maxima = np.max(difference_array)
        changed['maxima'] = True if maxima > kwargs['maxima'] else False
    if 'sum' in kwargs:
        sum_ = np.sum(difference_array)
        changed['sum'] = True if sum_ > kwargs['sum'] else False
    if 'count_larger_than' in kwargs:
        count_larger_than = np.sum(difference_array > kwargs['count_larger_than'][0])
        changed['count_larger_than'] = True if count_larger_than > kwargs['count_larger_than'][1] else False
    if 'percentage_larger_than' in kwargs:
        percentage_larger_than = np.sum(difference_array > kwargs['percentage_larger_than'][0]) / difference_array.size
        changed['percentage_larger_than'] = True if percentage_larger_than > kwargs['percentage_larger_than'][1] else False
    return changed, path_reference


def compare(args):
    """
    Compare reference and target.
    :param args: path_reference, path_target
    :return: whether the building is changed
    """
    path_reference, path_target, metrics = args
    difference_array = difference(path_reference, path_target)
    return change(difference_array, path_reference, **metrics)


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
        args.append((path_reference, path_target, cfg.compare.metrics))

    pool = multiprocessing.Pool(processes=cfg.compare.threads if cfg.compare.threads else multiprocessing.cpu_count())

    counter = 0
    # https://stackoverflow.com/a/40133278
    for c, filepath in tqdm(pool.imap_unordered(compare, args), total=len(args)):
        if True in c.values():
            counter += 1
            log.info(str(c) + str(filepath))
    log.info(f' {counter} / {len(args)} has changed')

    # fallback to single-thread
    # for arg in args:
    #     c, filepath = compare(arg)
    #     if True in c.values():
    #         counter += 1
    #         log.info(str(c) + str(filepath))
    # log.info(f' {counter} / {len(args)} has changed')


if __name__ == '__main__':
    multi_run()
