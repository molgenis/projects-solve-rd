#'////////////////////////////////////////////////////////////////////////////
#' FILE: emx.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-16
#' MODIFIED: 2021-09-16
#' PURPOSE: incorporate YAML to EMX generator
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


from emxconvert.convert import Convert

# ~ 0 ~
# Compile EMX for RD3 Portal Releases
# Comple the YAML-EMX markup for new RD3 staging tables
# See `emx/src/rd3_portal_release.yaml` for additional notes

convertPortalReleaseEmx = Convert(files = [
    'emx/src/rd3_portal.yaml', # import portal first
    'emx/src/rd3_portal_release.yaml'
])

convertPortalReleaseEmx.convert()
convertPortalReleaseEmx.packages
convertPortalReleaseEmx.entities
convertPortalReleaseEmx.attributes

convertPortalReleaseEmx.write(
    name = 'rd3_portal_release',
    format = 'xlsx',
    outDir = 'emx/dist/'
)

#//////////////////////////////////////

# ~ 1 ~
# Compile EMX for RD3 Portal Novelomics 

convertPortalNovelomicsEmx = Convert(
    files = [
        'emx/src/rd3_portal.yaml',
        'emx/src/rd3_portal_novelomics.yaml'
    ]
)

convertPortalNovelomicsEmx.convert()
convertPortalNovelomicsEmx.packages
convertPortalNovelomicsEmx.entities
convertPortalNovelomicsEmx.attributes

convertPortalNovelomicsEmx.write(
    name = 'rd3_portal_novelomics',
    format = 'xlsx',
    outDir = 'emx/dist/'
)


#//////////////////////////////////////

# ~ 2 ~
# Convert EMX for RD3 Portal Demographics Table

convertPortalDemographicsEmx = Convert(
    files = [
        'emx/src/rd3_portal_demographics.yaml'
    ]
)

convertPortalDemographicsEmx.convert()
convertPortalDemographicsEmx.packages
convertPortalDemographicsEmx.entities
convertPortalDemographicsEmx.attributes

convertPortalDemographicsEmx.write(
    name = 'rd3_portal_demographics',
    format = 'xlsx',
    outDir = 'emx/dist/'
)