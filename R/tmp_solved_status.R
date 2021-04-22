#'////////////////////////////////////////////////////////////////////////////
#' FILE: tmp_solved_status.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-02-19
#' MODIFIED: 2021-03-10
#' PURPOSE: create emx for solved status intermediate table
#' STATUS: working
#' PACKAGES: dplyr, tibble, readr
#' COMMENTS: This script was used to generate the EMX structure for the
#' solved status intermediate table
#'////////////////////////////////////////////////////////////////////////////

#' pkgs
suppressPackageStartupMessages(library(dplyr))


#' define ojbect
emx <- structure(list(), class = "emx")

#' define attributes
emx$attribs <- tibble::tribble(
    ~name, ~dataType,
    "id", "string",
    "subject", "string",
    "solved", "bool",
    "recontact", "string",
    "contact", "email"
) %>%
    mutate(
        idAttribute = case_when(
            name == "id" ~ TRUE,
            TRUE ~ FALSE
        ),
        auto = case_when(
            name == "id" ~ TRUE,
            TRUE ~ FALSE
        ),
        entity = "solved"
    )

#' save as csv file
readr::write_csv(
    x = emx$attribs,
    path = "data/tmp-solved-status/rd3_portal_solved_attributes.csv"
)

#' head over to shell/solvedstatus_upload.sh