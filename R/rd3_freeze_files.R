#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_freeze_files.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-03-23
#' MODIFIED: 2021-03-23
#' PURPOSE: add experimentID to table
#' STATUS: in.progress
#' PACKAGES: NS
#' COMMENTS: This script adds the column `experimentID` to the Files table.
#' This allows the analysis team to push sandbox metadata into the table.
#'////////////////////////////////////////////////////////////////////////////

# pkgs
suppressPackageStartupMessages(library(dplyr))

# utils
source("R/utils.R")
# m <- molgenis$new(host = "https://solve-rd-acc.gcc.rug.nl")
# m <- molgenis$new(host = "https://solve-rd.gcc.rug.nl")
m$login("admin")

#'//////////////////////////////////////

#' ~ 1 ~
#' Pull main data from `rd3_freeze1_file`

# define limits of API request
batches <- list(
    rows = 241576,     # get from molgenis
    limit = 10000,     # API row limit
    id = 1,            # set batch ID
    start = 0          # init counter (i.e., starting row)
)
batches$groups <- batches$rows %/% batches$limit
batches

# pull data in batches following the 10k row API limit
files <- purrr::map(seq_len(batches$groups), function(.x) {

    # run request
    d <- m$get(
        table = "rd3_freeze1_file",
        start = format(batches$start, scientific = FALSE),
        num = batches$limit
    )

    # increment counters
    print(
        rjson::toJSON(
            list(
                id = batches$id,
                start = batches$start,
                end = batches$start + batches$limit
            )
        )
    )
    batches$start <<- batches$start + batches$limit
    batches$id <<- batches$id + 1

    return(d)
})

# pull remainder (i.e., 240000+)
batch_25 <- m$get(
    table = "rd3_freeze1_file",
    start = format(batches$start, scientific = FALSE),
    num = batches$limit
)

#' ~ 1b ~
#' Pull additional data from `rd3_freeze1_samples` and `rd3_freeze1_labinfo`
samples <- m$get(
    table = "rd3_freeze1_sample",
    attr = c("id", "sampleID"),
    start = 0,

    num = format(10000, scientific = FALSE)
)
labinfo <- m$get(
    table = "rd3_freeze1_labinfo",
    attr = c("id", "experimentID", "sample"),
    start = 0,
    num = format(10000, scientific = FALSE)
)

#'//////////////////////////////////////

#' ~ 2 ~
#' Merge Datasets and prep data for reupload

# collapse files and supplementry batch
rd3_files <- dplyr::bind_rows(files, batch_25)

# Create experimentID
rd3_files <- rd3_files %>%
    mutate(
        patch = case_when(
            patch == "Freeze1 Original data" ~ "freeze1_original",
            TRUE ~ patch
        ),
        # WTF?!?!? I have to recode to import????
        # use rd3_fileTypes
        typeFile = case_when(
            typeFile == "BAI" ~ "bai",
            typeFile == "BAM" ~ "bam",
            typeFile == "BED" ~ "bed",
            typeFile == "CRAM" ~ "cram",
            typeFile == "FastQ" ~ "fastq",
            typeFile == "phenopacket" ~ "json",
            typeFile == "PED" ~ "ped",
            typeFile == "gVCF" ~ "vcf",
            TRUE ~ typeFile
        ),
        filepath_sandbox = NA
    )

# merge experiment IDs
rd3_files <- rd3_files %>%
    left_join(
        .,
        labinfo %>%
            select(-id, sample, experimentID),
        by = c("samples" = "sample")
    )

# merge `_original` ID
rd3_files <- rd3_files %>%
    left_join(
        .,
        samples %>%
            select(sampleID, samples2 = id),
        by = c("samples" = "sampleID")
    )

# replace samples with samples2
rd3_files$samples <- rd3_files$samples2
rd3_files$samples2 <- NULL


#'//////////////////////////////////////

#' ~ 3 ~
#' Prep for import

# prep files for import (drop samples column)
rd3_files_prepped <- rd3_files %>%
    group_by((row_number() - 1) %/% (n() / 3)) %>%
    tidyr::nest(.) %>%
    pull(data)

# write and upload each item individually
# readr::write_csv(
#     x = rd3_files_prepped[[1]],
#     file = "data/one-offs/rd3_freeze1_file.csv",
#     na = ""
# )

readr::write_csv(
    x = rd3_files_prepped[[2]],
    file = "data/one-offs/rd3_freeze1_file.csv",
    na = ""
)

readr::write_csv(
    x = rd3_files_prepped[[3]],
    file = "data/one-offs/rd3_freeze1_file.csv",
    na = ""
)

# import
# m$push(
#     table = "rd3_freeze1_file",
#     x = rd3_files_prepped
# )

# log out
m$logout()