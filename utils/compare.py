"""
Compare AHN 3/4 raster for change detection.

A building is considered changed from AHN 3 to AHN 4
if the raster differs significantly between the two.
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
from hydra.utils import instantiate

log = logging.getLogger(__name__)


class RasterComparison:
    """
    Raster comparison between AHN 3/4.
    """
    def __init__(self, **kwargs):
        self.reference_tif_dir = kwargs['reference_tif_dir']
        self.target_tif_dir = kwargs['target_tif_dir']
        self.save_tif_dir = kwargs['save_tif_dir']

        self.reference_las_dir = kwargs['reference_las_dir']
        self.target_las_dir = kwargs['target_las_dir']
        self.save_las_dir = kwargs['save_las_dir']

        self.metrics = kwargs['metrics']

        self.copy_tif = kwargs['copy_tif']
        self.copy_las = kwargs['copy_las']

    @staticmethod
    def difference(path_reference, path_target):
        """
        Compute pixel-wise absolute difference between two raster.
        No-data pixels are ignored.

        Parameters
        ----------
        path_reference: Path
            Path to reference raster
        path_target: Path
            Path to target raster

        Returns
        -------
        difference: None or (m, n) float
            Pixel-wise difference array, or None if no matching
        """
        reference = gdal.Open(str(path_reference))
        if path_target.is_file():
            target = gdal.Open(str(path_target))
        else:
            return None

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

    def changed(self, difference_array):
        """
        Verdict if the building is changed.

        Multiple metrics are available upon specification in config:
            * mean: mean per-pixel difference
            * maxima: maxima per-pixel difference
            * sum: sum of the differences
            * count_larger_than: count of height-differed pixels
            * percentage_larger_than: percentage of height-differed pixels.
        If the variation(s) are less than the specified threshold then the building is considered changed.

        Parameters
        ----------
        difference_array: (m, n) float
            Difference array

        Returns
        -------
        changed_: None or bool
            Whether the building is changed, or None if no matching
        """
        changed_ = {}
        if difference_array is None:
            return None
        if 'mean' in self.metrics:
            mean = np.mean(difference_array)
            changed_['mean'] = True if mean > self.metrics['mean'] else False
        if 'maxima' in self.metrics:
            maxima = np.max(difference_array)
            changed_['maxima'] = True if maxima > self.metrics['maxima'] else False
        if 'sum' in self.metrics:
            sum_ = np.sum(difference_array)
            changed_['sum'] = True if sum_ > self.metrics['sum'] else False
        if 'count_larger_than' in self.metrics:
            count_larger_than = np.sum(difference_array > self.metrics['count_larger_than'][0])
            changed_['count_larger_than'] = True if count_larger_than > self.metrics['count_larger_than'][1] else False
        if 'percentage_larger_than' in self.metrics:
            percentage_larger_than = np.sum(
                difference_array > self.metrics['percentage_larger_than'][0]) / difference_array.size
            changed_['percentage_larger_than'] = True if \
                percentage_larger_than > self.metrics['percentage_larger_than'][1] else False
        return changed_

    def copy_file(self, path_reference, path_target, filetype):
        """
        Copy reference and target to specified dir if they are changed.

        Parameters
        ----------
        path_reference: Path
            Path to reference file
        path_target: Path
            Path to target file
        filetype: str
            Type of files to copy, can be 'tif' or 'las'
        """
        # create dir structure as tile_number/raster_number
        stem = path_reference.relative_to(self.reference_tif_dir)
        if filetype == 'tif':
            path_reference_copy = self.save_tif_dir / stem.with_suffix('.reference.tif')
            path_target_copy = self.save_tif_dir / stem.with_suffix('.target.tif')
        elif filetype == 'las':
            path_reference = self.reference_las_dir / stem.with_suffix('.las')
            path_target = self.target_las_dir / stem.with_suffix('.las')
            path_reference_copy = self.save_las_dir / stem.with_suffix('.reference.las')
            path_target_copy = self.save_las_dir / stem.with_suffix('.target.las')
        else:
            raise ValueError('Input must be tif or las file.')

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

    def compare(self, args):
        """
        Compare reference and target to conclude if it is changed.

        Parameters
        ----------
        args: (Path, Path)
            (path_reference, path_target)

        Returns
        -------
        as_int: int {-1, 0, 1}
            Whether the building is changed
            -1: No matching
             0: Not changed
             1: Changed

        changed_: None or (m, n) float
            Changed metrics, or None if no matching
        path_reference.stem: str
            Filename stem
        """
        path_reference, path_target = args
        difference_array = self.difference(path_reference, path_target)
        changed_ = self.changed(difference_array)

        if changed_ is None:
            return -1, None, path_reference.stem

        elif True in changed_.values():
            if self.copy_tif:
                self.copy_file(path_reference, path_target, filetype='tif')
            if self.copy_las:
                self.copy_file(path_reference, path_target, filetype='las')
            return 1, changed_, path_reference.stem

        else:
            return 0, None, path_reference.stem


@hydra.main(config_path='conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    Compare the difference with multi-processing.

    Parameters
    ----------
    cfg: DictConfig
        Hydra config
    """
    reference_dir = Path(cfg.compare.reference_tif_dir)
    target_dir = Path(cfg.compare.target_tif_dir)
    reference_paths = reference_dir.rglob('*' + 'tif')
    target_paths = target_dir.rglob('*' + 'tif')

    args = []
    for path_reference in reference_paths:
        path_target = target_dir / path_reference.relative_to(reference_dir).with_suffix('.tif')
        args.append((path_reference, path_target))

    # for buildings that only exists since AHN 4: directly considered changed
    targets_only = []
    targets_match = {arg[1] for arg in args}
    for path_target in target_paths:
        if path_target not in targets_match:
            targets_only.append(path_target.stem)

    pool = multiprocessing.Pool(processes=cfg.threads if cfg.threads else multiprocessing.cpu_count())

    # initialise
    raster_comparison = instantiate(cfg.compare)

    # https://stackoverflow.com/a/40133278
    counter = 0
    with open('changed.txt', 'w') as file_changed:
        for r in tqdm(pool.imap_unordered(raster_comparison.compare, args), total=len(args)):
            if r[0] == -1:
                log.warning(f'Non-existing target: {r[-1]}')
            elif r[0] == 1:
                log.debug(f'Changed {r[1]}: {r[-1]}')
                file_changed.write(r[-1])
                file_changed.write('\n')
                counter += 1
            elif r[0] == 0:
                log.debug(f'Unchanged {r[1]}: {r[-1]}')
            else:
                log.error(f'Unexpected error: {r[-1]}')
                raise ValueError
        log.info(f'New buildings: {len(targets_only)}')
        for target_only in targets_only:
            log.debug(f'Changed (new): {target_only}')
            file_changed.write(target_only)
            file_changed.write('\n')
            counter += 1

        log.info(f' {counter} / {len(args)} has changed.')


if __name__ == '__main__':
    multi_run()
