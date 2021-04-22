# Molgenis Solve RD Database

The `molgenis-solve-rd` repository contains all of the scripts, EMX files, and other files for updating the RD3 database.

## Overview

Scripts, data files, and other files are stored in a number of subfolders.

- `data/`: In this folder you can find all outputs (data, emx, etc.) from the scripts located in the `R/` and `python` folder. All files stored here are imported into the solve-rd database using the scripts located in `shell/`
- `public/`: This folder contains the custom molgenis apps, documents, other files imported into the Solve-RD databases (non-data files). Additional information can be found in the `public/README.md` file.
- `python/`: All python scripts that run in Molgenis as scheduled jobs are stored in this folder.
- `R/`: This folder has R scripts that were used to build EMX files from datasets, process data, and other tasks. Outputs from these scripts are located in `data/`, and then imported using one of scripts in `shell/`. The R workspace uses [renv](https://github.com/rstudio/renv) to manage packages.
- `shell/`: scripts for sending data to the production/acceptance servers
- `www/`: various static assets (i.e., images, documents, etc.)

In addition, the `archive` folder has files that I'm not ready to remove. The `dev` folder is exlained in the following section.

## Getting Started

### Configuring R

In the `dev` folder, you will find the `dev.R` file. Use this file to configure the R workspace.

If you are working with R scripts, you will need to initialize the R library on your local machine. This workspace uses [renv](https://github.com/rstudio/renv) to manage R packages. Run the following command in the R console to initialize the R library locally.

```r
renv::restore()
```
