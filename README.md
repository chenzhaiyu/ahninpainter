# AHN Inpainter

This repository includes a toolset for inpainting the AHN point clouds.

## Introduction

### What is AHN?

[*Actueel Hoogtebestand Nederland*](https://www.ahn.nl/) (AHN) is the digital elevation map covering the whole of the Netherlands. It was captured by aerial LiDAR, and contains detailed and precise height information represented by point clouds.

### Why inpaint it?

Due to occlusion, scanning artefacts, and anisotropy of reflectance, the resulting point clouds are not homogeneously distributed: a portion of points are missing from certain regions of the measured ground objects.

![Faculty of Architecture](./docs/bk.png) 

These no-data gaps would contaminate data integrity, and subsequently result in anomalies in downstream applications, e.g., building reconstruction ([3D BAG](https://3dbag.nl/)). Therefore, it would be helpful to have those no-data gaps filled up, thus inpainting is needed.

### How to inpaint it?

Since an aerial point cloud typically depicts a 2.5D terrain model, instead of inpainting the 3D point cloud, it is more efficient to lower one dimension thus to inpaint the 2D raster. To this end, **AHN Inpainter** incorporates a toolset for inpainting AHN rasters with neural networks.

## Requirements

### Install dependencies

```bash
pip install -r requirements.txt
```

Refer to `networks/{submodules}` for additional requirements to run respective neural networks. Currently only [DFNet](https://dl.acm.org/doi/10.1145/3343031.3351002) and [GLCIC](https://dl.acm.org/doi/abs/10.1145/3072959.3073659?casa_token=0-rmKkKqKlMAAAAA:VIw6o1unLfxogk0kSL4EK5nAeteZWs0VDq19utHpe9L443fn5QSZEDd_P70_3noLQtXXnveF6EV3gg) are supported.

 ## Usage

There are self-contained Python files under `utils`, with their respective configurations under `conf`. 

```
clean.py      Clean invalid raster
compare.py    Compare AHN 3/4 raster
db.py         Retrieve footprint and point clouds from PostGIS database
download.py   Download AHN 4 data
mask.py       Create mask from raster with no-data pixels
merge.py      Merge AHN 3/4 CityJSON models
overlay.py    Overlay low-res and high-res raster
split.py      Split data into train/val/test sets
utils.py      General utilities
```

Navigate to `networks/{submodules}` for usages for respective neural networks.

## Get started

### Rasterise point clouds

The point cloud of a building is rasterised into a GeoTIFF image where an intensity value represents the highest height value of the points that fall into the pixel. In case no points are associated with the pixel, a no-data value is assigned. In addition, the raster size is determined by the size of the footprint as well as the specified resolution. For ground pixels outside the footprint, monotonic estimated height values are assigned. 

[Geoflow](https://github.com/geoflow3d/geoflow) is used for the rasterisation process, and [Tile Processor](https://github.com/tudelft3d/tile-processor) is used to parallelise this process across multiple pre-defined tiles:

```bash
tile_processor --loglevel INFO run --threads {num_threads} AHN PCRasterise  {path_config.yml} $(cat tile_ids.txt)
```

A typical configuration file `config.yml`  is of the following structure:

```yaml
database:
    dbname: baseregisters
    host: godzilla.bk.tudelft.nl
    port: {port}
    user: {user}
    password: {password}

features:
    schema: reconstruction_input
    table: reconstruction_input
    field:
        pk: gid
        geometry: geometrie
        uniqueid: identificatie

elevation:
    directories:
        -   {dir_tiles}:
                file_pattern: "t_{tile}.laz"
                priority: 1

features_tiles:
    boundaries:
        schema: tiles
        table: bag_tiles_3k
        field:
            pk: tile_id
            geometry: tile_polygon
            tile: tile_id
    index:
        schema: tiles
        table: bag_index_3k
        field:
            pk: gid
            tile: tile_id

elevation_tiles:
    boundaries:
        schema: {ahn3/ahn4}
        table: tiles_200m
        field:
            pk: fid
            geometry: geom
            tile: fid
            version: ahn_version

output:
    dir: {path_output}

path_executable: {path_geof}
path_flowchart: {path_flowchart.json}
doexec: true
path_toml: {path_geof.toml}
path_lasmerge: {path_lasmerge64}
path_ogr2ogr: {path_ogr2ogr}
```

In `geof.toml` there are a few rasterisation options:

```toml
CELLSIZE=0.50     # cellsize (m) of each pixel
USE_TIN=false      # whether to enable TIN interpolation to reduce gaps
L=0.0             # largest triangle length if TIN is enabled
USE_GROUND_POINTS=true  # whether to include ground points
```

### Prepare train/val/test data

1. Overlay raster of different resolution:

```bash
# configuration at conf/overlay
python utils/overlay.py
```

To facilitate high-quality training data that entails both completeness and high resolution, two rasters of different resolution/completeness from the same building are overlaid.

![overlay](./docs/overlay.png)

2. Clean invalid 1x1 raster from the generated raster:

```bash
# configuration at conf/clean
python utils/clean.py
```

3. Split the data into train/val/test:

```bash
# configuration at conf/split
python utils/split.py
```

`train` and `val` sets only include complete rasters without no-data pixels, while `test` set only includes rasters with no-data pixels. The former two are used to train and evaluate the neural networks with known height reference, while the latter one acts as *wild* examples where no height reference is given.

4. Extract mask from no-data (test) raster:

```bash
# configuration at conf/mask
python utils/mask.py
```

### Train the inpainting network(s)

Follow the `networks/{submodules}/README.md` for respective instructions to train the inpainting networks.

### Evaluate and convert rasters to point clouds

The inpainting can be evaluated on examples with known height reference (i.e., `val`), or examples without height reference (i.e., `test` or the extrapolated).

### More

With the [transition from AHN 3 to AHN 4](https://www.ahn.nl/ahn-4), a considerable amount of buildings have physically changed while others have not. A change detection pipeline is provided to compare the AHN 3/4 rasters: a building is considered changed from AHN 3 to AHN 4 if the raster differs significantly between the two:

```bash
# configuration at conf/compare
python utils/compare.py
```

With the change detection result, it is possible to merge two sets of city models:

```bash
# configuration at conf/merge
python utils/merge.py
```

In addition, `db.py` and `download.py` provide data retrieval methods from a database or via a public URL.
