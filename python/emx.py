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
import re

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


#//////////////////////////////////////

# ~ 3 ~ 
# Convert EMX for RD3 Release (i.e., new freeze)


# define function that recodes <freeze_number> with a new release number
def setEmxRelease(data, releaseNumr: str = None, releaseTitle: str = None):
    """setEmxRelease
    replace <freeze_number> with the desired release number
    
    @param data: list, post-converted EMX component (e.g., packages, entities, etc.)
    @param releaseNumr : str, the new release (e.g., "freeze4")
    @param releasetitle : str, name for the release (e.g., "Freeze 4")
    @returns list
    """
    releaseTitle = releaseNumr if releaseTitle is None else releaseTitle
    for d in data:
        for el in d:
            if el in ['package','entity', 'name', 'refEntity']:
                d[el] = re.sub(
                    pattern = r'(([fF]reeze)?\<freeze_number\>)',
                    repl = str(releaseNumr),
                    string = d[el]
                )
            if el in ['label', 'description']:
                d[el] = re.sub(
                    pattern = r'(([fF]reeze)?\<freeze_number\>)',
                    repl = str(releaseTitle),
                    string = d[el]
                )


convertFreezeEmx = Convert(
    files = [
        'emx/src/base_rd3.yaml',
        'emx/src/rd3_freeze.yaml'
    ]
)

convertFreezeEmx.convert()

# recode RD3 release: use freezeN as pattern
rNumr = "freeze3"
rName = "Freeze3"

setEmxRelease(convertFreezeEmx.packages, releaseNumr = rNumr, releaseTitle = rName)
setEmxRelease(convertFreezeEmx.entities, releaseNumr = rNumr, releaseTitle = rName)
setEmxRelease(convertFreezeEmx.attributes, releaseNumr = rNumr, releaseTitle = rName)

convertFreezeEmx.write(name = 'rd3_freeze3', format = 'xlsx', outDir = 'emx/dist/')