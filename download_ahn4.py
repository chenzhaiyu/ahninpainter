import os
from pathlib import Path
import wget
from tqdm import tqdm
import ssl
import urllib


def download(save_dir='./', tiles_path='./tilenames.txt', url_base='https://ns_hwh.fundaments.nl/hwh-ahn/ahn4/01_LAZ/'):
    """
    Download AHN4 point clouds.
    :param url_base: remote url base
    :param save_dir: where to save the data
    :param tiles_path: file of tile names
    :return: None
    """

    save_dir = Path(save_dir)
    tiles_path = Path(tiles_path)

    # disable ssl certificate
    if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

    with open(tiles_path) as filenames:
        tile_names = filenames.readlines()

    os.chdir(save_dir)

    tile_names_existing = []
    tile_names_downloaded = []
    tile_names_unavailable = []

    for tile_name in tqdm(tile_names):
        tile_name = tile_name.strip()
        if (save_dir / tile_name).exists():
            tile_names_existing.append(tile_name)
        else:
            try:
                tile_names_downloaded.append(tile_name)
                url_tile = url_base + tile_name
                wget.download(url_tile)
            except urllib.error.HTTPError:
                tile_names_unavailable.append(tile_name)

    print('downloaded tiles: ', tile_names_downloaded)
    print('unavailable tiles: ', tile_names_unavailable)


if __name__ == '__main__':
    download(save_dir='./')
