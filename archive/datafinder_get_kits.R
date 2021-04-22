#'////////////////////////////////////////////////////////////////////////////
#' FILE: datafinder_get_kits.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-04-14
#' MODIFIED: 2021-04-14
#' PURPOSE: find distinct values for `capture` in `rd3_freeze1_labinfo`
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# pkgs
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))

# utils
source("R/utils.R")
m <- molgenis$new(host = "https://solve-rd-acc.gcc.rug.nl")
# m <- molgenis$new(host = "https://solve-rd.gcc.rug.nl")
m$login(username = "")


# pull data from `rd3_freeze1_labinfo`
kits <- m$get(
    table = "rd3_freeze1_labinfo",
    num = 10000,
    attrs = c("experimentID", "capture")
)

# summarize
kits %>%
    distinct(capture) %>%
    arrange(capture) %>%
    pull(capture) %>%
    jsonlite::write_json(
        x = .,
        path = "data/_tests/enrichment_kits.json",
        pretty = TRUE,
        auto_unbox = TRUE
    )
