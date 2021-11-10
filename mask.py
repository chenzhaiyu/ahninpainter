import multiprocessing
from pathlib import Path

from tqdm import tqdm
from PIL import Image
from osgeo import gdal
import hydra
from omegaconf import DictConfig

from utils import nodata


def extract_mask(args):
    """
    Extract mask from nodata (test) image.
    :param args: image path and save path
    :return: None
    """
    path_in, path_out = args
    image = gdal.Open(str(path_in))
    has_nodata, mask_nodata = nodata(image)
    if has_nodata:
        Image.fromarray(mask_nodata).save(path_out)


@hydra.main(config_path='conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    Extract masks from no-data images with multi-processing.
    :return: None
    """
    input_dir = Path(cfg.mask.input_dir)
    output_dir = Path(cfg.mask.output_dir)
    input_paths = input_dir.rglob('*' + 'tif')

    args = []
    for path_in in input_paths:
        path_out = output_dir / path_in.relative_to(input_dir).with_suffix('.jpg')
        path_out.parent.mkdir(exist_ok=True, parents=True)
        args.append((path_in, path_out))

    pool = multiprocessing.Pool(processes=cfg.mask.threads if cfg.mask.threads else multiprocessing.cpu_count())
    # https://stackoverflow.com/a/40133278
    for _ in tqdm(pool.imap_unordered(extract_mask, args), total=len(args)):
        pass


if __name__ == '__main__':
    multi_run()
