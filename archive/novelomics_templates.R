#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_template.sR
#' AUTHOR: David Ruvolo
#' CREATED: 2021-03-03
#' MODIFIED: 2021-03-09
#' PURPOSE: create template for novelomics data
#' STATUS: working
#' PACKAGES: openxlsx; dplyr; readr; tibble; janitor
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

suppressPackageStartupMessages(library(dplyr))

readr::read_csv(file = "data/novelomics/novelomics_attributes.csv")

bgi <- openxlsx::read.xlsx(
    xlsxFile = "data/novelomics-source/All Shipments_BGI_final_lists_EGA.xlsx"
) %>%
    tibble::as_tibble() %>%
    janitor::clean_names(.) %>%
    head(., n = 1) %>%
    readr::write_csv(
        x = .,
        path = "data/novelomics-templates/rd3_portal_novelomics_samples.csv"
    )

#'//////////////////////////////////////

# import, clean colnames, and write wgs template

wgs <- openxlsx::read.xlsx(
    # nolint start
    xlsxFile = "data/novelomics-source/SolveRD_metadata_SR-WGS_subbatchs_1-1_to_2-4_.SRD.V0.9.1.xlsx",
    # nolint end
    sheet = 1,
    startRow = 3
) %>%
    tibble::as_tibble(.) %>%
    janitor::clean_names(.) %>%
    head(., n = 1) %>%
    readr::write_csv(
        x = .,
        path = "data/novelomics-templates/rd3_portal_novelomics_experiment.csv"
    )
