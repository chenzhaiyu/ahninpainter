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

Since an aerial point cloud typically depicts a 2.5D terrian model, instead of inpainting the 3D point cloud, it is more efficient to lower one dimension thus to inpaint the 2D raster. To this end, **AHN Inpainter** incorporates a toolset for inpainting AHN rasters with neural networks.

## Requirements

### Install dependencies

```bash
pip install -r requirements.txt
```

Refer to `networks/{submodules}` for additional requirements to run respective neural networks. Currently only [DFNet](https://dl.acm.org/doi/10.1145/3343031.3351002) and [GLCIC](https://dl.acm.org/doi/abs/10.1145/3072959.3073659?casa_token=0-rmKkKqKlMAAAAA:VIw6o1unLfxogk0kSL4EK5nAeteZWs0VDq19utHpe9L443fn5QSZEDd_P70_3noLQtXXnveF6EV3gg) are supported.

 ## Usage

There are self-contained Python files under `utils`, with their respective configurations under `conf`. 

```
clean.py     Clean invalid raster
compare.py   Compare AHN 3/4 raster
db.py        Retrieve footprint and point clouds from PostGIS database
download.py  Download AHN 4 data
mask.py      Create mask from raster with no-data pixels
merge.py     Merge AHN 3/4 CityJSON models
overlay.py   Overlay low-res and high-res raster
split.py     Split data into train/val/test sets
utils.py     General utilities
```

Navigate to `networks/{submodules}` for usages for respective neural networks.
