"""
Split data into train/val/test sets.
"""

from pathlib import Path
import subprocess
import multiprocessing

from tqdm import tqdm
from osgeo import gdal
import hydra
from omegaconf import DictConfig

from utils import nodata


def run(args):
    """
    One single run on one thread.

    Parameters
    ----------
    args: (str, DictConfig)
        Input path, hydra config
    """
    input_path, cfg = args
    input_dir = Path(cfg.split.input_dir)
    output_dir = Path(cfg.split.output_dir)

    tile = int(input_path.parent.stem)

    image = gdal.Open(str(input_path))
    has_nodata, _ = nodata(image)
    if has_nodata:
        partition = 'test'
    elif tile in cfg.split.val:
        partition = 'val'
    elif cfg.split.train and tile in cfg.split.train or not cfg.split.train:
        partition = 'train'
    else:
        # cfg.split.train and tile not in cfg.split.train
        return

    output_path = output_dir / partition / input_path.relative_to(input_dir)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    subprocess.run(["rsync"] + [str(input_path)] + [str(output_path)])


@hydra.main(config_path='conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    Split data into train/val/test.

    Parameters
    ----------
    cfg: DictConfig
        Hydra config
    """
    input_dir = Path(cfg.split.input_dir)
    input_paths = input_dir.rglob('*' + cfg.split.suffix)

    args = [(path, cfg) for path in input_paths]

    if cfg.threads > 1:
        pool = multiprocessing.Pool(processes=cfg.threads)
        # https://stackoverflow.com/a/40133278
        for _ in tqdm(pool.imap_unordered(run, args), total=len(args)):
            pass

    else:
        for arg in tqdm(args):
            run(arg)


if __name__ == '__main__':
    multi_run()
