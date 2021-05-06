![Solve RD: The Solving Unsolved Rare Diseases Database](www/images/Solve-RD.png)

# Molgenis Solve RD Database

The purpose of the RD3 database is to track and find samples processed by [Centro Nacional De Análisis Genómico (CNAG)](https://www.cnag.crg.eu) and submited to the [European Genome-phenome Archive (EGA)](https://ega-archive.org). For more information, follow the links below.

- The Solve-RD project site: [http://solve-rd.eu](http://solve-rd.eu)
- RD3 Data Model repository: [https://github.com/molgenis/RD3_database](https://github.com/molgenis/RD3_database)

## What's in this repository?

The `molgenis-solve-rd` repository contains all of the scripts and other files for maintaining the RD3 database.

- `data/`: This folder contains various EMX files for entities that were added after the initial release
- `public/`: This folder contains the custom molgenis apps that are used in the Molgenis database. Additional information can be found in the `public/README.md` file.
- `python/`: Various python scripts for prcessing data and other jobs
- `R/`: Various R scripts for processing data and other jobs
- `shell/`: scripts for importing data into Molgenis
- `www/`: various static assets (i.e., images, documents, etc.)

In addition, the `archive` folder has files that I'm not ready to remove. The `dev` folder is exlained in the following section.

## Getting Started

### Configuring R

In the `dev` folder, you will find the `dev.R` file. Use this file to configure the R workspace.

If you are working with R scripts, you will need to initialize the R library on your local machine. This workspace uses [renv](https://github.com/rstudio/renv) to manage R packages. Run the following command in the R console to initialize the R library locally.

```r
renv::restore()
```
