# Molgenis Solve RD Database

The `molgenis-solve-rd` contains all of the scripts and workflows for updating the Molgenis SolveRD database.

## Getting Started

Many of the scripts and workflows are written in R. It is recommended to have the latest version of R installed from [CRAN](https://cran.r-project.org). This workspace also uses the [renv](https://cran.r-project.org/package=renv) package. Once R is installed, the project library can be restored using the following command.

```r
renv::restore()
```

## Project Structure

Scripts, data files, and other files are stored in a number of subfolders.

- data: all outputs from the scripts located in the `R/` folder. The files here are imported into the solve-rd database using the scripts located in `shell/`
- public: custom molgenis apps, documents, other files imported into the Solve-RD databases.
- python: data processing scripts that run in Molgenis
- R: R workflows for generating EMX for new tables, transforming data, mapping variables, etc.
- shell: scripts for sending data to the production/acceptance servers
- www: various static assets (i.e., images, documents, etc.)

### Workspace Management and Local Development Environment

In the `dev` folder, you will find the `dev.R` file. Use this file to configure the R environment. This workspace uses `renv` to manage the R library. Use `renv::restore()` to install R packages locally. `renv` is &mdash; in principle &mdash; similar to yarn/npm. The `renv` folder contains all of the local R package library configuration files.

You can also find the docker files for creating a local Molgenis instance in `dev/dev-env`. This is useful if you would like to test scripts locally. Export the `rd3` EMX from the acceptance server and upload into the local instance.
