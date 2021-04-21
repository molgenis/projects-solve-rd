#'////////////////////////////////////////////////////////////////////////////
#' FILE: dev.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-02-09
#' MODIFIED: 2021-03-09
#' PURPOSE: workspace management
#' STATUS: on.going
#' PACKAGES: see below
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

#' init renv
#' renv::init()
renv::status()
renv::snapshot()

#' init 'project' for easier management
usethis::create_project(".")

#' pkgs
renv::install("dplyr")
renv::install("tibble")
renv::install("openxlsx")
renv::install("janitor")
renv::install("tidyr")
