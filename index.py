#'////////////////////////////////////////////////////////////////////////////
#' FILE: emx.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-16
#' MODIFIED: 2022-05-09
#' PURPOSE: incorporate YAML to EMX generator
#' STATUS: stable
#' PACKAGES: yamlemxconvert
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from yamlemxconvert.convert import Convert
from yaml import safe_load
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


# build ENX models
for model in config['models']:
    print('Building EMX for',model['name'])
    emx = Convert(files=model['files'])
    emx.convert()
    
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
                
            emx.entities.extend(releaseEmx.entities)
            emx.attributes.extend(releaseEmx.attributes)

    emx.write(name=model['name'],format='xlsx', outDir='dist')
    emx.write_schema(f"schemas/{model['name']}.md")
