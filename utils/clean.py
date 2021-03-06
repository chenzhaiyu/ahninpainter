"""
Clean invalid images.

Invalid images are those with size 1*1.
"""

from pathlib import Path
import multiprocessing
import os

import hydra
from osgeo import gdal
from omegaconf import DictConfig
from tqdm import tqdm


def clean_image(path):
    """
    Clean one invalid image (1x1).

    Parameters
    ----------
    path: Path
        Path to image
    """
    image = gdal.Open(str(path))
    if image.RasterXSize == 1 and image.RasterYSize == 1:
        try:
            os.remove(path)
            print('removed file:', path)
        except OSError:
            pass


@hydra.main(config_path='../conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    CLean images with multi-processing.

    Parameters
    ----------
    cfg: DictConfig
        Hydra config
    """
    input_dir = Path(cfg.clean.input_dir)
    input_paths = list(input_dir.rglob('*' + 'tif'))

    pool = multiprocessing.Pool(processes=cfg.threads if cfg.threads else multiprocessing.cpu_count())
    # https://stackoverflow.com/a/40133278
    for _ in tqdm(pool.imap_unordered(clean_image, input_paths), total=len(input_paths)):
        pass


if __name__ == '__main__':
    multi_run()
