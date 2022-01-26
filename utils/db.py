"""
Data retrieval from PostGIS database.

Footprints and corresponding point clouds are downloaded within given ROI.
"""

import os
import subprocess
import logging
from itertools import chain
from pathlib import Path

import fiona
import psycopg2
import json
from shapely import wkt as wkt_func
from shapely.geometry import mapping
from tqdm import tqdm


logger = logging.getLogger(__name__)

# debug mode processes only a fraction of files
# set False to process entire data
DEBUG = True


class DBExplorer:
    """
    Database handler.
    """
    def __init__(self, dsn, local_dir):
        self.dsn = dsn
        self.local_dir = Path(local_dir)

        self.connection = None
        self.roi = None
        self.identificaties = None

        # starting with CPython 3.6, dictionaries return items in the order you inserted them
        self.tiles = {}  # mapping between identificaties and tiles
        self.wkts = {}   # mapping between identificaties and wkt

        self.local_dir.mkdir(exist_ok=True)
        self._connect_db()

    def _connect_db(self):
        """
        Open connection to DB.
        """
        if self.connection and self.connection.closed == 0:
            return
        try:
            self.connection = psycopg2.connect(dsn=self.dsn)
            logger.debug(f"Opened connection to {self.connection.get_dsn_parameters()}")
        except psycopg2.OperationalError:
            logger.exception("I'm unable to connect to the database")
            raise

    def _get_wkt(self, identificatie, table="bag3d_v21031_7425c21b", attribute="pand"):
        """
        Get wkt polygon from identificatie.

        Parameters
        ----------
        identificatie: str
            Identificatie of building instance
        table: str
            Table name
        attribute: str
            Attribute name

        Returns
        -------
        as_str: str
            WKT polygon
        """
        # cached wkt for known identificaties
        if identificatie in self.wkts:
            return self.wkts[identificatie]
        self._connect_db()
        with self.connection.cursor() as cursor:
            sql_query = f"SELECT ST_AsEWKT(geometrie) from {table}.{attribute} " \
                        f"where identificatie = '{identificatie}'; "
            cursor.execute(sql_query)
            wkt = cursor.fetchone()[0]
        return wkt

    def get_identificaties(self, roi, table="bag3d_v21031_7425c21b", attribute="pand", srs=28992):
        """
        Get building identificaties within an ROI

        Parameters
        ----------
        roi: str
            Polygon region of interests
        table: str
            Schema name
        attribute: str
            Attribute name
        srs: int
            reference system
        """
        # query NL.IMBAG.Pand. within Amsterdam area
        with self.connection.cursor() as cursor:
            sql_query = f"SELECT identificatie FROM {table}.{attribute} WHERE ST_Intersects(ST_SetSRID(" \
                        f"'{roi}'::geometry, {srs}), geometrie);"
            cursor.execute(sql_query)
            identificaties = cursor.fetchall()
            identificaties = [i[0] for i in identificaties]
            self.identificaties = identificaties[:5] if DEBUG else identificaties

    @staticmethod
    def _wkt_to_gpkg(wkt_list, gpkg_file):
        """
        Convert wkt to gpkg.

        Parameters
        ----------
        wkt_list: list[str]
            List of wkt
        gpkg_file: str
            Path to gpkg file
        """
        # define a linestring feature geometry with one attribute
        schema = {
            'geometry': 'Polygon',
            'properties': {'id': 'int'},
        }

        # write a new shapefile
        with fiona.open(gpkg_file, 'w', 'GPKG', schema) as c:
            for i, line in enumerate(wkt_list):
                shape = wkt_func.loads(line)
                c.write({
                    'geometry': mapping(shape),
                    'properties': {'id': i},
                })

    def _write_gkpg(self):
        """
        Write footprints into geopackage files.
        """
        assert self.identificaties
        for identificatie in self.identificaties:
            output_dir = Path(self.local_dir, identificatie)

            try:
                output_dir.mkdir(exist_ok=True)
            except OSError or FileNotFoundError:
                logger.warning("Creation of the directory %s failed" % output_dir)

            wkt = self._get_wkt(identificatie)
            self._wkt_to_gpkg([wkt.split(';')[1]], os.path.join(output_dir, f"{identificatie}.gpkg"))

    def _get_tile_ids(self, table="ahn3", attribute="tiles_200m"):
        """
        Get tile ids.

        Parameters
        ----------
        table: str
            Table name
        attribute: str
            attribute name
        """
        for identificatie in tqdm(self.identificaties):
            wkt = self._get_wkt(identificatie)
            # cache wkt for known identificaties
            self.wkts[identificatie] = wkt
            with self.connection.cursor() as cursor:
                cursor.execute(f"select fid from {table}.{attribute} where ST_Intersects(geom, '{wkt}');")
                tiles = [row[0] for row in cursor.fetchall()]
                assert len(tiles) >= 1
                self.tiles[identificatie] = tiles

    def write_tiles(self, filepath):
        """
        Write tiles mapping into a json file.

        Parameters
        ----------
        filepath: str
            Path to save json file
        """
        with open(filepath, 'w') as f:
            json.dump(self.tiles, f)

    @property
    def _laz_paths(self, prefix="godzilla:/data/pointcloud/AHN4/tiles_200m/t_{", suffix="}.laz"):
        """
        Retuen paths to raw laz files on server.

        Parameters
        ----------
        prefix: str
            Prefix to LAZ paths
        """
        return f"{prefix}" + ','.join(chain.from_iterable(self.tiles.values())) + f"{suffix}"

    def download_footprints(self):
        """
        Download footprint given identificaties.
        """
        self._get_tile_ids()
        self._write_gkpg()

    def download_pointclouds(self, temp_dir="./temp", remove_raw=True):
        """
        Download point clouds given identificaties.

        Parameters
        ----------
        temp_dir: str
            Temp dir to save raw laz tiles (to be merged)
        remove_raw: bool
            Remove downloaded raw laz tiles if set True
        """
        # rsync only once for all laz files (authorisation)
        laz_dir = self.local_dir / temp_dir
        try:
            laz_dir.mkdir(exist_ok=True)
        except OSError or FileNotFoundError:
            logger.warning("Creation of the directory %s failed" % laz_dir)

        logger.info(f"downloading AHN files")
        try:
            subprocess.run(["rsync"] + [self._laz_paths] + [str(laz_dir)])
        except FileNotFoundError:
            logger.warning("rsync not installed. unable to sync files from server.")
            return

        logger.info(f"merging AHN files")
        for i, identificatie in enumerate(tqdm(self.tiles)):
            laz_files = [f"{laz_dir}/t_{tid}.laz" for tid in self.tiles[identificatie]]

            try:
                subprocess.run(
                    ["pdal", "merge"] + laz_files + [os.path.join(self.local_dir, f"{identificatie}/{identificatie}.laz")])
            except FileNotFoundError:
                logger.warning("pdal not installed. unable to merge laz files.")

        logger.info(f"removing raw tiles")
        if remove_raw:
            import shutil
            shutil.rmtree(laz_dir)


def run_godzilla():
    """
    Run data retrieval from https://godzilla.bk.tudelft.nl/.
    """
    host = "localhost"
    port = 5432
    dbname = "baseregisters"
    local_dir = "./data"

    logger.info(f"connecting to database {dbname} on {host}:{port}")

    user = input("user: ")
    password = input("password: ")

    db_explorer = DBExplorer(dsn=f"host={host} port={port} dbname={dbname} user={user} password={password}",
                             local_dir=f"{local_dir}")
    db_explorer.get_identificaties(roi='POLYGON((110000 494000,110000 474300,131000 474300,131000 494000,110000 '
                                       '494000))')  # Amsterdam region
    db_explorer.download_footprints()
    db_explorer.download_pointclouds()


if __name__ == '__main__':
    run_godzilla()
