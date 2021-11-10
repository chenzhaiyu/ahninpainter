import multiprocessing
from pathlib import Path

from tqdm import tqdm
from PIL import Image
from osgeo import gdal

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


def multi_run(data_dir, save_dir, threads=0):
    """
    Extract masks from no-data images with multi-processing.
    :return: None
    """
    input_dir = Path(data_dir)
    save_dir = Path(save_dir)
    input_paths = input_dir.rglob('*' + 'tif')

    args = []
    for path_in in input_paths:
        path_out = save_dir / path_in.relative_to(data_dir).with_suffix('.jpg')
        path_out.parent.mkdir(exist_ok=True, parents=True)
        args.append((path_in, path_out))

    pool = multiprocessing.Pool(processes=threads if threads else multiprocessing.cpu_count())
    # https://stackoverflow.com/a/40133278
    for _ in tqdm(pool.imap_unordered(extract_mask, args), total=len(args)):
        pass


if __name__ == '__main__':
    multi_run(data_dir='/Users/zhaiyu/Datasets/inpaint/ahn4/tif_all',
              save_dir='/Users/zhaiyu/Datasets/temp',
              threads=0)
