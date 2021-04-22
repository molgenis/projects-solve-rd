#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_shipment.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-03-11
#' MODIFIED: 2021-03-11
#' PURPOSE: create emx for the BGI Shippment information
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


suppressPackageStartupMessages(library(dplyr))

#' load data
samples <- readr::read_csv(
    file = "data/novelomics/novelomics_samples.csv",
    col_types = readr::cols()
)


#' ~ 1 ~
#' Build New Entity
#' Variables of interest from the novel samples and experiment data will be
#' mapped to the RD3 structure (i.e., standarize colnames, types, etc.).
#' Once complete, the two objects will be joined to create a main novel omics
#' entity. I will leave the raw data in `rd3_portal_novelomics_*`. To get
#' started, it is easier to merge the datasets, and then map the column
#' names. First reduce the datasets, select variables that we want
samples <- samples %>%
    select(
        sampleID = sample_id,
        subjectID = participant_subject,
        seqType = type_of_analysis,
        tissueType = tissue_type,
        materialType = sample_type,
        experimentID = solve_rd_experiment_id,
        project_batch_id = batch
    )


#' ~ 2 ~
#' Map colum names to SolveRD names
#' You will have to search for the EMX `name` attribute and the corresponding
#' `label`. Use experiment EMX to find mappings.
samples_attribs <- data.frame(
    entity = "novelomics-shipment",
    name = c(colnames(samples), "molgenis_id")
) %>%
    mutate(
        label = case_when(
            # name == "sampleID" ~ "sampleID",
            name == "subjectID" ~ "SubjectID",
            # name == "seqType" ~ "seqType",
            name == "tissueType" ~ "Tissue Types",
            # name == "materialType" ~ "materialType",
            # name == "experimentID" ~ "experimentID",
            # name == "project_batch_id" ~ "project_batch_id",
            TRUE ~ name
        ),
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

#' write data into `data/novelomics-shipment`
readr::write_csv(
    x = samples_attribs,
    file = "data/novelomics-shipment/novelomics-shipment_attributes.csv"
)
