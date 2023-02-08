![Solve RD: The Solving Unsolved Rare Diseases Database](www/images/Solve-RD.png)

# Molgenis Solve RD Database

The purpose of the RD3 database is to track and find samples processed by [Centro Nacional De Análisis Genómico (CNAG)](https://www.cnag.crg.eu) and submited to the [European Genome-phenome Archive (EGA)](https://ega-archive.org). For more information, follow the links below.

- The Solve-RD project site: [http://solve-rd.eu](http://solve-rd.eu)
- RD3 Data Model repository: [https://github.com/molgenis/RD3_database](https://github.com/molgenis/RD3_database)

If you would like to view the data model of RD3, please see the data model schema here: [https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md](https://github.com/molgenis/molgenis-solve-rd/blob/main/model/schemas/rd3.md).

## What's in this repository?

The `molgenis-solve-rd` repository contains all of the scripts and other files for maintaining the SolveRD instance of the RD3 data model. Here you can find the mapping scripts, extensions to data model, custom applications used in RD3, and much more.

- `dist`: built EMX files ready for import
- `examples`: code examples for using the API
- `lookups`: lookups used in the RD3 database
- `model`: all of the EMX modules that were added to the RD3 data model; mapping scripts
- `rd3`: code used to maintain the RD3 database
- `shell`: scripts for syncing data on the clusters
- `templates`: csv templates for collaborators
- `www`: images, documents, and other things related to the RD3 database

## Get Started

To get started, clone the repository and activate the python virtual environment.

### Connecting to RD3 via the API

Many of the scripts in the `rd3` folder, if not all, require an account to run and credentials are stored in an `.env` file. If you do not have an RD3 (local molgenis user) account, please request one. When you an account, create a `.env` file in the main folder (`molgenis-solve-rd`). In the file, you should have the following variables defined.

```txt
# hosts
MOLGENIS_ACC_HOST=...
MOLGENIS_PROD_HOST=...

# user credentials per host
MOLGENIS_PROD_USR="..."
MOLGENIS_PROD_PWD="..."

MOLGENIS_ACC_USR="..."
MOLGENIS_ACC_PWD="..."
```

The `.env` file is loaded into a script using function `load_dotenv()` from the `dotenv` library.
