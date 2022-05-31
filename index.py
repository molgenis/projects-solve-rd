#'////////////////////////////////////////////////////////////////////////////
#' FILE: emx.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-16
#' MODIFIED: 2022-05-31
#' PURPOSE: incorporate YAML to EMX generator
#' STATUS: stable
#' PACKAGES: yamlemxconvert
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from yamlemxconvert.convert import Convert
from yaml import safe_load
from rd3.utils.emxtools import buildEmxTags
import re

def updateReleaseInfo(data, releaseId, releaseLabel):
    """Update Release Info
    For each data element in a record set, look for YAML release tag
    definitions and set the values accordingly
    
    @param data input dataset (record set)
    @param releaseId release identifier
    @param releaseLabel label for the release
    """
    for row in data:
        for key in row.keys():
            row[key]=re.sub('<freeze_identifier>', releaseId, str(row[key]))
            row[key]=re.sub('<freeze_label>', releaseLabel, str(row[key]))


# re.search(r'(\<freeze_identifier\>)', 'this is a <freeze_identifier> test')
# re.sub(r'(\<freeze_identifier\>)', 'dog', 'this is a <freeze_identifier> test')


# read EMX config file
with open('model.yaml', 'r') as file:
    config = safe_load(file)
    file.close()
    del file


# pull active models only
activeEmxModels = [model for model in config['models'] if model['active']]
if not activeEmxModels:
    raise SystemError('Detected no active EMX models to build. Did you forget to add `active: true`?')

# build ENX models
for model in activeEmxModels:
    print('Building EMX for',model['name'])
    emx = Convert(files=model['files'])
    emx.convert()
        
    # build tags (if available)
    if emx.tags:
        tags = emx.tags
        tags.extend(buildEmxTags(emx.packages))
        tags.extend(buildEmxTags(emx.entities))
        tags.extend(buildEmxTags(emx.attributes))
        tags = list({d['identifier']: d for d in tags}.values())
        emx.tags = sorted(tags, key = lambda d: d['identifier'])
        
    # if releases are defined, render EMX files accordingly
    if model.get('releases') and model.get('releaseTemplate'):
        for release in model['releases']:
            print('Building model for release',release)
            releaseEmx=Convert(files=model['releaseTemplate'])
            releaseEmx.convert()

            # fix emx components with current release identifier and label
            updateReleaseInfo(
                data=releaseEmx.packages,
                releaseId=release,
                releaseLabel=model['releases'][release]
            )
            updateReleaseInfo(
                data=releaseEmx.entities,
                releaseId=release,
                releaseLabel=model['releases'][release]
            )
            updateReleaseInfo(
                data=releaseEmx.attributes,
                releaseId=release,
                releaseLabel=model['releases'][release]
            )
                
            emx.packages.extend(releaseEmx.packages)
            emx.entities.extend(releaseEmx.entities)
            emx.attributes.extend(releaseEmx.attributes)

    emx.write(name=model['name'],format='xlsx', outDir='dist')
    emx.write_schema(f"schemas/{model['name']}.md")
