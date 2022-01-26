"""
Overlay low-res and high-res raster to form complete high-res raster.
"""

from pathlib import Path
import multiprocessing
import logging

from tqdm import tqdm
from omegaconf import DictConfig
import hydra
from osgeo import gdal


log = logging.getLogger(__name__)


def overlay(args):
    """
    Overlay two raster (in order).

    Parameters
    ----------
    args: (str, str, str)
        Path to low-res raster, path to high-res raster, path to output raster
    """
    path_low, path_high, path_out = args
    path_out.parent.mkdir(exist_ok=True, parents=True)
    gdal.Warp(str(path_out), [str(path_low), str(path_high)], format='GTiff')


@hydra.main(config_path='conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    Overlay low-res and high-res with multi-processing.

    Parameters
    ----------
    cfg: DictConfig
        Hydra config
    """
    dir_low = Path(cfg.overlay.dir_low)
    dir_high = Path(cfg.overlay.dir_high)
    dir_out = Path(cfg.overlay.dir_output)
    paths_low = dir_low.rglob('*' + 'tif')

    args = []
    for path_low in paths_low:
        path_high = dir_high / path_low.relative_to(dir_low).with_suffix('.tif')
        path_out = dir_out / path_low.relative_to(dir_low).with_suffix('.tif')
        args.append((path_low, path_high, path_out))

    pool = multiprocessing.Pool(processes=cfg.threads if cfg.threads else multiprocessing.cpu_count())

    for _ in tqdm(pool.imap_unordered(overlay, args), total=len(args)):
        pass


if __name__ == '__main__':
    multi_run()
