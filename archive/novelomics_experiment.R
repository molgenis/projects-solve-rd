#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_experiment.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-02-10
#' MODIFIED: 2021-03-11
#' PURPOSE: mapping novelomics tables ->> RD3 names ->> new entity
#' STATUS: working
#' PACKAGES: dplyr; readr;
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

#' pkgs
suppressPackageStartupMessages(library(dplyr))

#' load data
experiment_raw <- readr::read_csv(
    file = "data/novelomics/novelomics_experiment.csv",
    col_types = readr::cols()
)

#'//////////////////////////////////////

#' ~ 1 ~
#' Build New Entity
#' Variables of interest from the novel samples and experiment data will be
#' mapped to the RD3 structure (i.e., standarize colnames, types, etc.).
#' Once complete, the two objects will be joined to create a main novel omics
#' entity. I will leave the raw data in `rd3_portal_novelomics_*`. To get
#' started, it is easier to merge the datasets, and then map the column
#' names.

experiment <- experiment_raw %>%
    select(
        file_path,
        file_name,
        unencrypted_md5_checksum,
        file_type,
        file_group_id,
        batch_id,
        project_experiment_dataset_id,
        sample_id,
        subject_id,
        sequencing_center,
        platform_model,
        library_source,
        library_strategy,
        library_selection,
        paired_nominal_length,
        experiment_type,
        tissue_type,
        sample_type,
        project_batch_id,
        run_ega_id,
        file_ega_id,
        experiment_ega_id
    )

#'//////////////////////////////////////

#' ~ 2 ~
#' Create WGS Entity

#' ~ 2a ~
#' Map colum names to SolveRD names
#' You will have to search for the EMX `name` attribute
#' and the corresponding `label`.
wgs <- experiment %>%
    select(
        EGA = file_ega_id, # label: EGA
        EGApath = file_path, # label: EGA PATH
        name = file_name, # label: Filename
        md5 = unencrypted_md5_checksum, # label: md5
        typeFile = file_type, # label: typeFile
        filegroupID = file_group_id, # label: File Group ID
        batchID = batch_id, # label: batchID
        experimentID = project_experiment_dataset_id, # label: experimentID
        sampleID = sample_id, # label: sampleIS
        subjectID = subject_id, # label: SubjectID
        sequencingCentre = sequencing_center, # label: SequencingCentre
        sequencer = platform_model, # label: sequencer
        libraryType = library_source, # label: libraryType
        capture = library_strategy, # labe: Enrichment Kit
        librarySelectionId = library_selection, # label: librarySelectionId
        # label: paired_nominal_length
        pairedNominalLength = paired_nominal_length,
        seqType = experiment_type, # label: seqType
        tissueType = tissue_type, # label: Tissue Types
        materialType = sample_type, # label: materialType,
        project_batch_id, # label: project_batch_id
        run_ega_id, # label: run_ega_id
        experiment_ega_id # experiment_ega_id,
    )

#' ~ 2b ~
#' Build Attributes
wgs_attributes <- data.frame(
    entity = "novelomics-experiment",
    name = c(colnames(wgs), "molgenis_id")
) %>%
    mutate(
        label = case_when(
            # name == "EGA" ~ "EGA",
            name == "EGApath" ~ "EGA PATH",
            name == "name" ~ "Filename",
            # name == md5 ~ "md5",
            # name == typeFile ~ "typeFile",
            name == "filegroupID" ~ "File Group ID",
            # name == "batchID" ~ "batchID",
            # name == "experimentID" ~ "experimentID",
            # name == "sampleID" ~ "sampleID",
            name == "subjectID" ~ "SubjectID",
            name == "sequencingCentre" ~ "SequencingCentre",
            name == "sequencer" ~ "sequencer",
            name == "libraryType" ~ "Library Source",
            # name == "librarySelectionId" ~ "librarySelectionId",
            name == "capture" ~ "Enrichment Kit",
            # name == "pairedNominalLength" ~ "pairedNominalLength",
            # name == "seqType" ~ "seqType",
            name == "tissueType" ~ "Tissue Types",
            # name == "materialType" ~ "materialType",
            # name == "project_batch_id" ~ "project_batch_id",
            # name == "run_ega_id" ~ "run_ega_id",
            # name == "experiment_ega_id" ~ "experiment_ega_id",
            # name == "molgenis_id" ~ "molgenis_id",
            TRUE ~ as.character(name)
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

#' ~ 2c ~
#' Write data

readr::write_csv(
    x = wgs_attributes,
    file = "data/novelomics-experiment/novelomics-experiment_attributes.csv"
)
# readr::write_csv(
#     x = wgs,
#     file = "data/novelomics-experiment/novelomics-experiment.csv"
# )
# readr::write_csv(
#     x = wgs,
#     file = "data/novelomics-experiment/rd3_portal_novelomics_experiment.csv"
# )

# afterwards, head over to shell/novelomics_updates.sh and run part 2