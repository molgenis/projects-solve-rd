#'////////////////////////////////////////////////////////////////////////////
#' FILE: emx.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-16
#' MODIFIED: 2022-01-31
#' PURPOSE: incorporate YAML to EMX generator
#' STATUS: stable
#' PACKAGES: emxconvert
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from yamlemxconvert.convert import Convert
import pandas as pd
import re

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


# write attributes as csv template
def writeEmxTemplate(
    entities: list = None,
    attributes: list = None,
    format: str = 'csv',
    outDir: str = '.'
):
    """Write EMX Templates
    
    Write the emx attributes as an excel file
    
    @param entities   (list) : EMX entities in a package
    @param attributes (list) : EMX attributes
    @param format     (str)  : output file format ('csv' or 'xlsx')
    @param outDir     (str)  : location to save file (default: '.', i.e., current)
    """
    
    if not (format in ['csv', 'xlsx']):
        raise ValueError('Error in writeEmxTemplate: unknown format `{}`'.format(str(format)))
    
    for entity in entities:
        pkgEntity = entity['package'] + '_' + entity['name']
        
        attribs = [x['name'] for x in attributes if x['entity'] == pkgEntity]
        data = pd.DataFrame(index = None, columns = range(len(attribs)))
        data.columns = attribs
        
        file = f'{outDir}/{pkgEntity}.{format}'
        if format == 'csv': data.to_csv(file, index = False)
        if format == 'xlsx': data.to_excel(
            file,
            sheet_name = pkgEntity,
            index = False,
            engine = 'xlsxwriter'
        )


#//////////////////////////////////////////////////////////////////////////////


# ~ 0 ~
# Compile EMX for RD3 Portal Releases
# Comple the YAML-EMX markup for new RD3 staging tables
# See `emx/src/rd3_portal_release.yaml` for additional notes

convertPortalReleaseEmx = Convert(files = [
    'emx/src/base_rd3_portal.yaml', # import portal first
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

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Compile EMX for RD3 Portal Novelomics 

convertPortalNovelomicsEmx = Convert(
    files = [
        'emx/src/base_rd3_portal.yaml',
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


# generate CSV templates
writeEmxTemplate(
    entities = convertPortalNovelomicsEmx.entities,
    attributes = convertPortalNovelomicsEmx.attributes,
    format = 'csv',
    outDir = 'templates'
)

# writeEmxTemplate(
#     entities = convertPortalNovelomicsEmx.entities,
#     attributes = convertPortalNovelomicsEmx.attributes,
#     format = 'xlsx',
#     outDir = 'templates'
# )


#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Convert EMX for RD3 Portal Demographics Table

# convertPortalDemographicsEmx = Convert(
#     files = [
#         'emx/src/rd3_portal_demographics.yaml'
#     ]
# )

# convertPortalDemographicsEmx.convert()
# convertPortalDemographicsEmx.packages
# convertPortalDemographicsEmx.entities
# convertPortalDemographicsEmx.attributes

# convertPortalDemographicsEmx.write(
#     name = 'rd3_portal_demographics',
#     format = 'xlsx',
#     outDir = 'emx/dist/'
# )


#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~ 
# Convert EMX for RD3 Release (i.e., new freeze)

convertFreezeEmx = Convert(
    files = [
        'emx/src/base_rd3.yaml',
        'emx/src/base_rd3_freeze.yaml'
    ]
)

convertFreezeEmx.convert()
# convertFreezeEmx.packages
# convertFreezeEmx.entities
# convertFreezeEmx.attributes

# recode RD3 release: use freezeN as pattern
# rNumr = "freeze3"
# rName = "Freeze3"
rNumr = "novelomics"
rName = "Novel Omics"

rFile = 'rd3_' + rNumr

setEmxRelease(convertFreezeEmx.packages, releaseNumr = rNumr, releaseTitle = rName)
setEmxRelease(convertFreezeEmx.entities, releaseNumr = rNumr, releaseTitle = rName)
setEmxRelease(convertFreezeEmx.attributes, releaseNumr = rNumr, releaseTitle = rName)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# OPTIONAL: NOVELOMICS RELEASES
# since models are split across multiple files, the following steps will merge
# the novelomics models with the main release model.

# convert yaml
convertNovelomicsEmx = Convert(files = ['emx/src/rd3_novelomics.yaml'])
convertNovelomicsEmx.convert()
# convertNovelomicsEmx.packages
# convertNovelomicsEmx.entities
# convertNovelomicsEmx.attributes

# remove existing `labinfo` entities
convertFreezeEmx.entities = [
    d for d in convertFreezeEmx.entities if not ('labinfo' == d.get('name'))
]

# remove all `labinfo` attributes
convertFreezeEmx.attributes = [
    d for d in convertFreezeEmx.attributes if not (
        'rd3_novelomics_labinfo' == d.get('entity')
    )
]

# Merge EMX structures
convertFreezeEmx.entities = convertFreezeEmx.entities + convertNovelomicsEmx.entities
convertFreezeEmx.attributes = convertFreezeEmx.attributes + convertNovelomicsEmx.attributes
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Save model
convertFreezeEmx.write(name = rFile, format = 'xlsx', outDir = 'emx/dist/')


#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# GENERAL PORTAL TABLES

convertPortal = Convert(files = ['emx/src/rd3_portal_cluster.yaml'])
convertPortal.convert()
convertPortal.packages
convertPortal.entities
convertPortal.attributes

convertPortal.write(
    name = 'rd3_portal_cluster',
    format = 'xlsx',
    outDir = 'emx/dist/'
)