#'/////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_cohorts.R
#' AUTHORS: Joeri van der Velde, David Ruvolo
#' CREATED: 2022-05-10
#' MODIFIED: 2022-05-10
#' PURPOSE: Creating Cohorts example
#' STATUS: stable
#' PACKAGES: **run renv::restore()**
#' COMMENTS: See README.md and rd3_cohorts.Rmd for more information
#'/////////////////////////////////////////////////////////////////////////////


dotenv::load_dot_env()
source("https://solve-rd.gcc.rug.nl/molgenis.R")

molgenis.login(
    username = Sys.getenv("MOLGENIS_USERNAME"),
    password = Sys.getenv("MOLGENIS_PASSWORD")
)

# read query url and extract subject identifiers
rd_nexus_query <- readLines("DiscoveryNexusQueryResult.txt")
subjects <- paste(
    paste0(
        stringr::str_extract_all(
            string = rd_nexus_query,
            pattern = "P\\d+"
        )[[1]],
        "_original"
    ),
    collapse = ","
)

# retrieve sample metadata
samples <- molgenis.get(
    entity = "rd3_freeze1_sample",
    q = paste0("subject=in=(", subjects, ")"),
    attributes = "id,sampleID,subject"
)

# retrieve file metadata
sample_identifiers <- paste(samples$id, collapse = ",")
files <- molgenis.get(
    entity = "rd3_freeze1_file",
    q = paste0(
        "samples=in=(", sample_identifiers, ")",
        ";typeFile==vcf"
    ),
    attributes = "name, typeFile, samples, experimentID"
)

# save filename and sample identifier to file
write.table(
    files[, c("name", "samples")],
    file = "cohort_file_metadata.txt",
    row.names = FALSE
)
