"""
Combine AHN 3/4 CityJSON models.
"""

import subprocess
from pathlib import Path
import multiprocessing
import logging

from tqdm import tqdm
from omegaconf import DictConfig
from hydra.utils import instantiate
import hydra

log = logging.getLogger(__name__)


class CityModelMerger:
    def __init__(self, **kwargs):
        self.data_dir = kwargs['data_dir']
        self.temp_dir = kwargs['temp_dir']

        self.prefix_ahn4 = kwargs['prefix_ahn4']
        self.prefix_ahn34 = kwargs['prefix_ahn34']
        self.do_append_id = kwargs['do_append_id']

        self.changed_ids = kwargs['changed_ids']
        self.changed_temp = kwargs['changed_temp']

    def append_id(self):
        """
        Create `—id {identificatie}` records in change list.
        """
        with open(self.changed_ids, 'r') as fin:
            lines = fin.readlines()

        Path(self.changed_temp).parent.mkdir(exist_ok=True)
        with open(self.changed_temp, 'w') as fout:
            for line in lines:
                out = '--id ' + line
                fout.write(out)

    def subset(self, args):
        """
        Subset CityObjects in CityJSON files.
        :param args: (fused AHN (when unchanged), AHN 4 (when changed))
        :return: None
        """
        path_cj_a, path_cj_b = args
        subprocess.run(f"cjio {path_cj_a} subset --exclude $(cat {self.changed_temp}) save {path_cj_a.with_suffix('.subset.json')}", shell=True)
        subprocess.run(f"cjio {path_cj_b} subset  $(cat {self.changed_temp}) save {path_cj_b.with_suffix('.subset.json')}", shell=True)

    def merge(self, args):
        """
        :param args: ({path_cj_a}_subset.json, {path_cj_b}_subset.json)
        Merge the two subsets.
        """
        path_cj_a, path_cj_b = args[0].with_suffix('.subset.json'), args[1].with_suffix('.subset.json')
        path_cj_out = (Path(self.temp_dir) / path_cj_a.name[5:]).with_suffix('.merged.json')
        subprocess.run(f"cjio {path_cj_a} merge {path_cj_b} save {path_cj_out}", shell=True)


@hydra.main(config_path='conf', config_name='config')
def multi_run(cfg: DictConfig):
    """
    Merge AHN 3/4 with multi-processing.
    :return: None
    """

    data_dir = Path(cfg.merge.data_dir)
    ahn4_paths = data_dir.rglob(f'{cfg.merge.prefix_ahn4}*' + '.json')

    args = []
    for ahn4_path in ahn4_paths:
        ahn34_name = cfg.merge.prefix_ahn34 + ahn4_path.name[5:]
        ahn34_path = ahn4_path.with_name(ahn34_name)
        args.append((ahn4_path, ahn34_path))

    pool = multiprocessing.Pool(processes=cfg.threads if cfg.threads else multiprocessing.cpu_count())

    # initialise
    merger = instantiate(cfg.merge)

    if cfg.merge.do_append_id:
        merger.append_id()

    # https://stackoverflow.com/a/40133278
    for r in tqdm(pool.imap_unordered(merger.subset, args), total=len(args)):
        pass

    for r in tqdm(pool.imap_unordered(merger.merge, args), total=len(args)):
        pass


if __name__ == '__main__':
    multi_run()

