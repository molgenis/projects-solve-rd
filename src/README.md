# RD3 EMX Files

In this folder, you can find all of the EMX files used in RD3. All models were written in yaml format and converted using [EMX Convert](https://github.com/davidruvolo51/yaml-emx-convert). The models are described in the following table.

| model                          | description                              |
|--------------------------------|------------------------------------------|
| `base_rd3.yaml`                | Base EMX markup for the main RD3 package |
| `base_rd3_portal.yaml`         | Base EMX markup for the RD3 portal       |
| `rd3_freeze.yaml`              | EMX markup for new RD3 releases          |
| `rd3_novelomics.yaml`          | EMX for Novel omics subpackage           |
| `rd3_portal_demographics.yaml` | EMX for new portal table (tbd)           |
| `rd3_portal_novelomics.yaml`   | Portal package for novel omics data      |
| `rd3_portal_release.yaml`      | Portal EMX for new RD3 staging tables    |

For more information about rendering and customizing the EMX models, see the script `emx/emx.py`.
