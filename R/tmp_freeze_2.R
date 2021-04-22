#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze2_emx.R
#' AUTHOR: David Ruvolo
#' CREATED: 2021-02-11
#' MODIFIED: 2021-03-10
#' PURPOSE: solve status emx script
#' STATUS: working
#' PACKAGES: dplyr; readr
#' COMMENTS: script for generating the emx for the solve status data
#' intermediate data. The mappings are listed below.
#'
#' rd3_organisation -> id
#' rd3_organisation -> name
#'
#' rd3_subject -> id/identifier
#' rd3_subject -> organization
#' rd3_subject -> ERN
#' rd3_subject -> solved
#' rd3_subject -> matchMakerPermission
#' rd3_subject -> noIncidentalFindings   # see recontact located in consent
#'
#' rd3_sample -> id/identifier
#' rd3_sample -> organization
#' rd3_sample -> alternativeIdentifier
#' rd3_sample -> ERN
#' rd3_sample -> tissueType
#' rd3_sample -> sequencingCentre    # not found
#' rd3_sample -> subject
#'
#' rd3_labinfo -> identifier
#' rd3_labinfo -> capture
#' rd3_labinfo -> sequencer
#' rd3_labinfo -> libraryType
#' rd3_labinfo -> library
#' rd3_labinfo -> seqType
#' rd3_labinfo -> sample
#'
#' Perhaps it would be easier to create this entity using the following
#' column naming formula: `table_name` where name is the EMX name
#'////////////////////////////////////////////////////////////////////////////

#' pkgs
suppressPackageStartupMessages(library(dplyr))


#' ~ 1 ~
#' Download EMX attributes
#' in order to create the EMX file, navigate to System >> Meta >> Attributes.
#' In the 'Data Items to Filter' menu, click the 'Filter Wizard'. In the new
#' menu, enter the following items into the `Entity` input box:
#'
#'      Freeze1 Samples
#'      Freeze1 Labinfo
#'      Organisation (RD3_organisation)
#'      Freeze1 Subject
#'
#' Scroll down to the end of the page and click 'Download'. The following
#' options should be applied.
#'
#'      - Column Names: 'Attribute Names'
#'      - Entity Values: 'Entity Labels'
#'      - Download Type: 'csv'
#'
#' Download file. Move the file from your machine's default download folder
#' into `data/molgenis-exports/`. Adjust the filename below.

#' load file
attribs <- readr::read_csv(
    file = "data/molgenis-exports/sys_md_Attribute_2021-03-10_10_28_50.csv",
    col_types = readr::cols()
)

#' to start off, let's filter the data for the desired attributes. Use the
#' list object as a guide
selected_attribs <- attribs %>%
    filter(
        # filter for organisation attributes
        name %in% c("name", "identifier") & entity == "Organisation" |

        # filter for Freeze1 Subject attributes
        name %in% c(
            "id",
            "organisation",
            "ERN",
            "solved",
            "date_solved",
            "matchMakerPermission",
            "recontact"   # double check attributes sheet
        ) & entity == "Freeze1 Subject" |

        # filter for Freeze1 LabInfo attributes
        name %in% c(
            "id",
            "capture",
            "sequencer",
            "libraryType",
            "library",
            "seqType",
            "sample"
        ) & entity == "Freeze1 LabInfo" |

        # filter for Freeze1 Subject Attributes
        name %in% c(
            "id",
            "organisation",
            "alternativeIdentifier",
            "ERN",
            "tissueType",
            "subject"
        ) & entity == "Freeze1 Samples"
    )

#' from the 'selected_attribs', select required variables and map to EMX
#' names. We don't need a lot of ther other properties as this is only a
#' temporary table
emx <- selected_attribs %>%
    select(
        name,
        source_entity = entity,
        # dataType = type,              # not required for intermediate tables
        # idAttribute = isIdAttribute,  # not required for intermediate tables
        # parent, # not allowed!        # not required for intermediate tables
        # refEntity = refEntityType,    # not required for intermediate tables
        # nillable = isNullable,        # not required for intermediate tables
        # auto = isAuto,                # not required for intermediate tables
        # visible = isVisible,          # not required for intermediate tables
        label,
        description,
        # readOnly = isReadOnly,        # not required for intermediate tables
        # unique = isUnique,            # not required for intermediate tables
        # tags                          # not required for intermediate tables
    ) %>%
    mutate(
        entity = "tmpFreeze2",
        # recode name: append freezeN output table name
        source_entity = case_when(
            grepl("Freeze1", source_entity) ~ gsub(
                pattern = "Freeze1",
                replacement = "",
                x = source_entity
            ),
            TRUE ~ source_entity
        ),
        source_entity = tolower(trimws(source_entity)),
        name = paste0(source_entity, "_", name),

        # append: label into description (if applicable)
        description = case_when(
            !is.na(label) ~ paste0(label, ": ", description),
            TRUE ~ as.character(description)
        )
    ) %>%
    select(-source_entity, -label) %>%
    bind_rows(
        tibble::tribble(
            ~name, ~description, ~entity,
            "id", "auto generated molgenis ID", "tmpFreeze2"
        ),
        .
    ) %>%
    mutate(
        idAttribute = case_when(
            name == "id" ~ TRUE,
            TRUE ~ FALSE
        ),
        auto = case_when(
            name == "id" ~ TRUE,
            TRUE ~ FALSE
        )
    )

#' save
readr::write_csv(
    x = emx,
    path = "data/tmp-freeze-2/tmpFreeze2_attributes.csv",
    na = ""
)

#' afterwards, head over to: solvedstatus_import.sh