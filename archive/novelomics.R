#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-02-08
#' MODIFIED: 2021-04-22
#' PURPOSE: cleaning and transformation of NO data into Molgenis EMX format
#' STATUS: ongoing
#' PACKAGES: dplyr; readr; janitor; openxlsx; tibble
#' COMMENTS: The purpose of this script is to systematically create the
#' EMX structure for the Novel Omics intermediate table. This data structure
#' will be imported into RD3 / Portal / novelomics /
#'////////////////////////////////////////////////////////////////////////////

#' pkgs (see `dev/dev.R` for more info)
suppressPackageStartupMessages(library(dplyr))

#'//////////////////////////////////////

#' ~ 1 ~
#' Clean `All Shipments_BGI_final_lists_EGA.xlsx`
#' There is not much that is needed to be done for these dataset. To make sure
#' the input dataset meets EMX requirements, we must clean the variable names.
#' Use the `janitor` pkg to do this.
#'
#' I'm also returned the dataset an R tibble class. It is much faster for tidy
#' operations.
bgi <- openxlsx::read.xlsx(
        # nolint start
        xlsxFile = "data/novelomics-source/...",
        # nolint end
        sep.names = " "
    ) %>%
    tibble::as_tibble()

#' Clean names
bgi_original_names <- colnames(bgi)
bgi <- janitor::clean_names(bgi)

#'//////////////////////////////////////

#' ~ 2 ~
#' Clean `SolveRD_metadata_SR-WGS_subbatchs_1-1_to_2-4_.SRD.V0.9.1`
#' Like the BGI dataset, we need to make sure all column names are cleaned,
#' and that the dataset has a unique row identifier. I suspect that a new
#' ID will be created using a similar formula as the BGI data.

#' Load data, clean names, and return as a tibble object. Since the file has
#' three header rows, I will ignore the first two
# nolint start
wgs <- openxlsx::read.xlsx(
        "data/novelomics-source/...",
        sheet = 1,
        startRow = 3,
        sep.names = " "
    ) %>%
    tidyr::as_tibble(.)
# nolint end

#' Clean names
wgs_original_names <- names(wgs)
wgs <- janitor::clean_names(wgs)

#'//////////////////////////////////////

#' ~ 2 ~
#' Build EMX File
#' Now that the data is clean, we can create the EMX file for the Novel Omics
#' table in the Solve RD Molgenis database. According to the handover notes,
#' this should be imported into the `rd3_portal` package. There are no
#' notes that this should be a subpackage, so I will structure the EMX as
#' entities. This can be changed a later date.

#' Init emx object
emx <- structure(list(), class = "emx.structure")

#' ~ 2a ~
#' Create a subpackage
emx$packages <- tibble::tribble(
    # nolint start
    ~id, ~label, ~description, ~parent,
    # "rd3_portal", "RD3 Portal Data", "RD3 portal, containing data submitted by CNAG", "",
    "rd3_portal_novelomics", "Novel Omics", " Novel Omics Sample and Experiment Information", "rd3_portal"
    # nolint end
)

#' ~ 2b ~
#' First, I will init the entities as it looks like that was done with the
#' existing tables
emx$entities <- tibble::tribble(
    ~name, ~label, ~description,
    "shipment", "shipment", "Information on Shipments",
    "experiment", "Experiment", "Information on Experiments",
) %>%
    mutate(
        package = "rd3_portal_novelomics"
    ) %>%
    as.data.frame()

#' ~ 2b ~
#' Create Attributes
#' The attributes sheet must list the `name` of the variables and the entity
#' that it is associated with. `label` is also useful, but it isn't required.
#' I will use the original column names as the label. You must also create
#' attributes per entity, but I create the initial structure and build the
#' attributes entity-by-entity.

#' Create Attributes for BGI data
emx$attributes <- data.frame(
    entity = "shipment",
    name = c(colnames(bgi), "molgenis_id"),
    description = c(bgi_original_names, "molgenis_id")
)

#' Create attributes for Experiment Info
emx$attributes <- emx$attributes %>%
    rbind(
        data.frame(
            entity = "experiment",
            name = c(colnames(wgs), "molgenis_id"),
            description = c(wgs_original_names, "molgenis_id")
        )
    ) %>%
    mutate(
        idAttribute = case_when(
            name == "molgenis_id" ~ TRUE,
            TRUE ~ FALSE
        ),
        nillable = case_when(
            name == "molgenis_id" ~ FALSE,
            TRUE ~ TRUE
        ),
        auto = case_when(
            name == "molgenis_id" ~ TRUE,
            TRUE ~ FALSE
        )
    )

#'//////////////////////////////////////

#' ~ 3 ~
#' Save data

#' write emx file
#' wb <- openxlsx::createWorkbook()
#' openxlsx::addWorksheet(wb, sheetName = "packages")
#' openxlsx::addWorksheet(wb, sheetName = "entities")
#' openxlsx::addWorksheet(wb, sheetName = "attributes")
#' openxlsx::writeData(wb, sheet = "packages", x = emx$packages)
#' openxlsx::writeData(wb, sheet = "entities", x = emx$entities)
#' openxlsx::writeData(wb, sheet = "attributes", x = emx$attributes)
#' openxlsx::saveWorkbook(
#'     wb,
#'     "data/novelomics/rd3_portal_novelomics.xlsx",
#'     overwrite = TRUE
#' )

#' write BGI and WGS data as csv files
readr::write_csv(emx$packages, "data/novelomics/sys_md_Package.csv")
readr::write_csv(emx$attributes, "data/novelomics/novelomics_attributes.csv")


# readr::write_csv(bgi, "data/_tests/rd3_portal_novelomics_shipment.csv")
# readr::write_csv(wgs, "data/novelomics-experiment/novelomics_experiment.csv")


#'//////////////////////////////////////

#' ~ OLD CODE ~

#' Let's check the IDs. For EMX format, each row must have a unique ID.
#' bgi %>% dim()
#' bgi %>% distinct(participant_subject) %>% `[[`(1) %>% length()
#' bgi %>% distinct(sample_id) %>% `[[`(1) %>% length()
#' bgi %>% distinct(solve_rd_experiment_id) %>% `[[`(1) %>% length()

#' None of the variables will provide a unique ID per-row. Calculate the
#' frequency of repeated IDs by number of repeats (i.e., bin frequency
#' and then summarize that)
#' bgi %>%
#'    group_by(participant_subject) %>%
#'    count(name = "frequency_bin") %>%
#'    ungroup() %>%
#'    group_by(frequency) %>%
#'    count(name = "count")
#'
#' To make sure the BGI dataset meets EMX requirements, I will create a new
#' unique identifier. This is for Molgenis only and it will not replace any
#' existing ID. The new variable will be a concatenated value of participant
#' ID, sample ID, experiment ID batch. (this should be enough for the EMX)
# bgi$molgenis_id <- paste(
#     bgi$participant_subject,
#     bgi$sample_id,
#     bgi$solve_rd_experiment_id,
#     bgi$batch,
#     sep = "_"
# )

# check `molgenis_id` to make sure it is indeed a TRUE unique identifier
# bgi %>%
#     group_by(molgenis_id) %>%
#     count() %>%
#     ungroup() %>%
#     group_by(n) %>%
#     count(name = "count")

#' Let's check the IDs. For EMX format, each row must have a unique ID. Let's
#' check the same variables as the BGI dataset
# wgs %>% dim()
# wgs %>% distinct(subject_id) %>% `[[`(1) %>% length()
# wgs %>% distinct(sample_id) %>% `[[`(1) %>% length()
# wgs %>% distinct(project_experiment_dataset_id) %>% `[[`(1) %>% length()

#' Let's check a few more
# wgs %>% distinct(file_name) %>% `[[`(1) %>% length()  # this is good!

#' Filename is naturally the best option, but to make sure it is properly linked
#' concatenate participant, sample, project Ids
# wgs$molgenis_id <- paste0(
#     wgs$subject_id,
#     wgs$sample_id,
#     wgs$file_name
# )

#' Check the length of `molgenis_id`
# wgs %>% NROW() == wgs %>% distinct(molgenis_id) %>% `[[`(1) %>% length()