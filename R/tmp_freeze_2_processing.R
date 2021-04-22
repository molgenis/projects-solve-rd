#'////////////////////////////////////////////////////////////////////////////
#' FILE: tmp_freeze_2_processing.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-03-12
#' MODIFIED: 2021-04-22
#' PURPOSE: transform data from freeze2 intermediate table into Freeze2 tables
#' STATUS: working; ongoing
#' PACKAGES: NA
#' COMMENTS: This script is a one time job for processing freeze2 data from
#' the intermediate tables into the appropriate Freeze 2 entities
#'////////////////////////////////////////////////////////////////////////////

# pkgs
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))

# utils
source("R/utils.R")
# m <- molgenis$new(host = "https://solve-rd-acc.gcc.rug.nl")
m <- molgenis$new(host = "https://solve-rd.gcc.rug.nl")
m$login(username = "admin")

#'////////////////////////////////////////////////////////////////////////////

#' ~ 0 ~
#' Get Data + Apply Recodings
#' There aren't that many recodings/transforms that are needed to be applied at
#' the global level. You can recode IDs ("_original", "VS", etc.) at the global
#' level, but it might be better to do this case-by-case as I need access to the
#' original IDs.

d <- m$get(table = "rd3_portal_tmpFreeze2", num = 4000)

#' Recode (list changes here)
#'
#' - ERN-RITA => ERNRITA: rather than added a new record in the ERN table
#'      recode the ERNS that do not match
d <- d %>%
    mutate(
        samples_ERN = case_when(
            samples_ERN == "ERN-RITA" ~ "ERNRITA",
            TRUE ~ as.character(samples_ERN)
        ),
        subject_ERN = case_when(
            subject_ERN == "ERN-RITA" ~ "ERNRITA",
            TRUE ~ as.character(subject_ERN)
        )
    )


#'////////////////////////////////////////////////////////////////////////////

#' ~ 1 ~
#' Process Lookup Tables First
#' The import process will fail if lookup tables aren't updated beforehand.
#' Manually pull and merge Freeze2 Reference Tables and update them with new
#' entities from Freeze2. The tables that need to be updated are listed below.
#' The molgenis client has a method for preparing data into JSON format. Use
#' `prep_data`, and then `push_data`. There are 1000 row limits
#'
#' TABLES TO UPDATE
#' - [x] Patch Information: `rd3_path`
#' - [x] Organisations: `rd3_organisation`
#' - [x] ERN: `rd3_ERN` (shouldn't really be needed, but good to check)


#' ~ 1a ~
#' update Patch Information Table
tibble::tribble(
    ~id, ~patch_date, ~description,
    "freeze2_original", "2021-03-15", "Freeze2 Original Data"
) %>%
    m$prep(x = .) %>%
    m$push(table = "rd3_patch", x = .)


#' ~ 1b ~
#' Identify new organisations in freeze2 data and add to rd3
organisations <- d %>%
    select(
        name = organisation_name,
        identifier = organisation_identifier
    ) %>%
    distinct(name, .keep_all = TRUE) %>%
    arrange(name)


#' pull existing organisation data
orgs <- m$get(table = "rd3_organisation") %>% arrange(name)


# filter new organisations and push changes to RD3 Table
new_orgs <- organisations %>% filter(!name %in% c(orgs$name))
m$push("rd3_organisation", m$prep(new_orgs))

jsonlite::write_json(
    x = new_orgs,
    path = "data/backgrup_new_orgs_2021_03_17.json",
    pretty = TRUE,
    auto_unbox = FALSE
)

#' ~ 1c ~
#' Add New ERN Information
erns <- d %>%
    select(
        identifier = subject_ERN,
        label = subject_ERN
    ) %>%
    distinct(identifier, .keep_all = TRUE)


# pull data from molgenis `rd3_ERN`
ern_molgenis_lookup <- m$get("rd3_ERN")


# filter Freeze2 ERN data for new ERNS
new_erns <- erns %>% filter(!identifier %in% c(ern_molgenis_lookup$identifier))


#'////////////////////////////////////////////////////////////////////////////

#' ~ 2 ~
#' Process Data for Freeze 2 Subjects
#' Pull the necessary data for the table `rd3_freeze2_subject` and
#' `rd3_freeze2_subjecInfo` There should only be one row per subject. Please
#' be advised that not all data comes from CNAG. All "missing" fields come
#' from other sources and will be added at a later date. I have left the
#' "missing" variables as comments to demonstrate this


#' ~ 2a ~
#' Process data for `rd3_freeze2_subject`
subjects <- d %>%
    select(
        id = samples_subject,
        subjectID = samples_subject,
        # sex1 =  # sex ("M", "F")
        # fid =   # family ID
        # mid =   # maternal ID
        # pid =   # paternal ID
        # clinial_status =  # affected status (bool)
        # disease = #  disease
        # phenotype = # phenotype
        # hasNotPhenotype = ,
        # phenopackagesId = ,
        # variant = ,
        organisation = subject_organisation,
        ERN = subject_ERN,
        solved = subject_solved,  # this needs to be recoded
        date_solved = subject_date_solved,
        # remarks =  # missing
        matchMakerPermission = subject_matchMakerPermission,
        recontact = subject_recontact#,
        # contact = # missing
        # retracted = # missing (blank anyways)
    ) %>%
    distinct(subjectID, .keep_all = TRUE) %>%
    mutate(
        patch = "freeze2_original",
        id = paste0(id, "_original"),
        solved = case_when(
            solved == "unsolved" ~ FALSE,
            solved == "solved" ~ TRUE
        )
    ) %>%
    select(id, everything())

#' check dims
dim(subjects)
subjects %>% distinct(subjectID) %>% NROW()
NROW(subjects) == subjects %>% distinct(subjectID) %>% NROW()


#' split data into objects less than the 1000 row API limit
subjects_prepped <- subjects %>%
    group_by((row_number() - 1) %/% (n() / 4)) %>%
    nest %>%
    pull(data)


#' import
m$push("rd3_freeze2_subject", m$prep(subjects_prepped[[1]]))
m$push("rd3_freeze2_subject", m$prep(subjects_prepped[[2]]))
m$push("rd3_freeze2_subject", m$prep(subjects_prepped[[3]]))
m$push("rd3_freeze2_subject", m$prep(subjects_prepped[[4]]))


#' ~ 2b ~
#' Process data for `rd3_freeze2_subjectinfo`
subjectinfo <- d %>%
    select(
        id = subject_id,
        subjectID = subject_id
    ) %>%
    distinct(subjectID, .keep_all = TRUE) %>%
    mutate(
        id = paste0(id, "_original"),
        subjectID = id,
        patch = "freeze2_original"
    ) %>%
    select(id, everything())

#' check
dim(subjectinfo)
subjects %>% distinct(subjectID) %>% NROW()
NROW(subjectinfo) == subjects %>% distinct(subjectID) %>% NROW()

#' split data into objects less than the 1000 row API limit
subjectinfo_prepped <- subjectinfo %>%
    group_by((row_number() - 1) %/% (n() / 4)) %>%
    nest %>%
    pull(data)


#' import
m$push("rd3_freeze2_subjectinfo", m$prep(subjectinfo_prepped[[1]]))
m$push("rd3_freeze2_subjectinfo", m$prep(subjectinfo_prepped[[2]]))
m$push("rd3_freeze2_subjectinfo", m$prep(subjectinfo_prepped[[3]]))
m$push("rd3_freeze2_subjectinfo", m$prep(subjectinfo_prepped[[4]]))


#'//////////////////////////////////////

#' ~ 2 ~
#' Process Data for Freeze 2 Samples
samples <- d %>%
    select(
        sampleID = samples_id,
        alternativeIdentifier = samples_alternativeIdentifier,
        subject = subject_id,
        # sex1 =  # missing (blank anyways)
        # sex2 = # missing (blank anyways)
        tissueType = samples_tissueType,
        # materialType = # missing (blank anyways)
        # flag = , # missing QC Failed
        organisation = subject_organisation,
        ERN = samples_ERN
        # retracted =  , # missing ("y", "n")
        # dateAvailable = , # missing (blank anyways)
        # anatomicalLocation = , # missing (blank anyways)
    ) %>%
    mutate(
        subject = paste0(subject, "_original"),
        sampleID = paste0("VS", sampleID),
        id = paste0(sampleID, "_original"),
        patch = "freeze2_original"
    ) %>%
    select(id, everything())


#' check
dim(samples)
samples %>% distinct(sampleID) %>% NROW()
samples %>% distinct(subject) %>% NROW()


#' split data into objects less than the 1000 row API limit
samples_prepped <- samples %>%
    group_by((row_number() - 1) %/% (n() / 4)) %>%
    nest %>%
    pull(data)


#' import
m$push("rd3_freeze2_sample", m$prep(samples_prepped[[1]]))
m$push("rd3_freeze2_sample", m$prep(samples_prepped[[2]]))
m$push("rd3_freeze2_sample", m$prep(samples_prepped[[3]]))
m$push("rd3_freeze2_sample", m$prep(samples_prepped[[4]]))

#'//////////////////////////////////////

#' ~ 3 ~
#' Process Data for Freeze 2 Lab Info

labinfo <- d %>%
    select(
        sample = labinfo_sample,
        capture = labinfo_capture,
        libraryType = labinfo_libraryType,
        # flowcell =  # missing (blank anyways)
        # barcode =  # missing (blank anyways)
        # samplePosition = # missing (blank anyways)
        library =  labinfo_library,
        # sequencingCentre = # missing
        sequencer = labinfo_sequencer, # missing
        seqType = labinfo_seqType
        # arrayID =  # missing (blank anyways)
        # mean_cov # missing (blank anyways)
        # c20 = # missing
        # retracted = # missing
    ) %>%
    mutate(
        id = paste0(sample, "_original"),
        experimentID = sample,
        libraryType = case_when(
            libraryType == "genomics" ~ "Genomic",
            TRUE ~ libraryType
        ),
        library = as.character(library),
        seqType = case_when(
            seqType == "Whole Exome Sequencing" ~ "WXS",
            seqType == "Whole Genome Sequencing with or without PCR" ~ "WGS",
            TRUE ~ seqType
        ),
        sample = paste0("VS", sample, "_original"),
        patch = "freeze2_original"
    ) %>%
    select(id, experimentID, sample, everything())

#' check
dim(labinfo)
labinfo %>% distinct(sample)  %>% NROW()
labinfo %>% distinct(experimentID)  %>% NROW()


#' split data into objects less than the 1000 row API limit
labinfo_prepped <- labinfo %>%
    group_by((row_number() - 1) %/% (n() / 4)) %>%
    nest %>%
    pull(data)

#' push
m$push("rd3_freeze2_labinfo", m$prep(labinfo_prepped[[1]]))
m$push("rd3_freeze2_labinfo", m$prep(labinfo_prepped[[2]]))
m$push("rd3_freeze2_labinfo", m$prep(labinfo_prepped[[3]]))
m$push("rd3_freeze2_labinfo", m$prep(labinfo_prepped[[4]]))