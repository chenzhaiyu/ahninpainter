"""
Download AHN 4 data via public URL.
"""

import os
from pathlib import Path
import wget

from tqdm import tqdm
import ssl
import urllib
from omegaconf import DictConfig
import hydra


@hydra.main(config_path='../conf', config_name='config')
def download_ahn(cfg: DictConfig):
    """
    Download AHN4 point clouds.

    Parameters
    ----------
    cfg: DictConfig
        url_base: str
            Remote url base
        save_dir: str
            Path to save the data
        tiles_path: str
            Path to save tile names
    """

    save_dir = Path(hydra.utils.to_absolute_path(cfg.db.local_dir))
    tiles_path = cfg.db.tilenames
    url_base = cfg.db.url_ahn

    # disable ssl certificate
    if not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

    with open(hydra.utils.to_absolute_path(tiles_path)) as filenames:
        tile_names = filenames.readlines()

    save_dir.mkdir(exist_ok=True, parents=True)
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
    download_ahn()
