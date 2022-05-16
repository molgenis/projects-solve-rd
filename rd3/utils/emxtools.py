#'////////////////////////////////////////////////////////////////////////////
#' FILE: modeltools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-03-07
#' MODIFIED: 2022-03-07
#' PURPOSE: tools for building RD3 EMX models
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import pandas as pd
import re
from os import path

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

def __emptyEmxTagTemplate__():
    """Empty Emx Tag Template
    Generate a blank dict for tag metadata
    """
    return {
        'identifier': None,
        'label': None,
        'objectIRI': None,
        'codeSystem': None,
        'relationLabel': 'isAssociatedWith',
        'relationIRI': 'http://molgenis.org#isAssociatedWith'
    }

def buildEmxTags(attributes: list = []):
    """Build Emx Tags
    At the attribute level, collate all tags from the `tags` property and
    transform into EMX format.
    @param attributes (list) : the post-converted emx object `attributes` 
    """
    rawTags = []
    for attr in attributes:
        if 'tags' in attr:
                if attr['tags'].split(','):
                    for t in attr['tags'].split(','):
                        rawTags.append(t.strip())
                else:
                    rawTags.append(attr['tags'])
    
    data = []
    tags = list(set(rawTags))
    
    # set known tags that do not follow the default
    # pattern: [a-zA-Z]{1,}_[a-zA-Z0-9]{1,}
    knownDcmiTags = ['http://purl.org/dc/terms/valid']
    knownW3Tags = [
        'https://w3id.org/reproduceme#wasUpdatedBy',
        'https://w3id.org/reproduceme#ProcessedData'
    ]
    knownDcatTags = {
        'https://www.w3.org/TR/vocab-dcat-3/#Property:catalog_dataset' : {
            'label': 'dcat:Dataset' 
        },
        'https://www.w3.org/TR/vocab-dcat-3/#Class:Catalog': {
            'label': 'dcat:Catalog' 
        }
    }
    
    for tag in tags:
        if bool(tag):
            d = __emptyEmxTagTemplate__()
            name = path.basename(tag)
            
            # codes that are split with a forward slash rather than a hyphen
            knownIriVariations = re.search(r'(/(HL7|SNOMEDCT|MESH)/)', tag)
            if knownIriVariations:
                d['identifier'] = tag
                d['label'] = f"{knownIriVariations.group().replace('/','')}:{name}"
                d['codeSystem'] = knownIriVariations.group().replace('/','')
            
            # for other Iri variations
            elif re.search(r'^([a-zA-Z]{1,}#[a-zA-Z]{1,}_[a-zA-Z0-9]{1,})', name):
                d['identifier'] = tag
                d['label'] = name.split('#')[1].replace('_',':')
                d['codeSystem'] = name.split('#')[1].split('_')[0]
                
            # process DCMI tags manually
            elif tag in knownDcmiTags:
                d['identifier'] = tag
                d['label'] = f'DCMI:{name}'
                d['codeSystem'] = 'DCMI'
                
            # tags from w3id.org
            elif tag in knownW3Tags:  
                d['identifier'] = tag
                d['label'] = f"W3ID:{name.split('#')[1]}"
                d['codeSystem'] = 'W3ID'
        
            # dcat tags
            elif tag in knownDcatTags:
                d['identifier'] = tag
                d['label'] = knownDcatTags[tag]['label']
                d['codeSystem'] = 'DCAT'
                
            # set EDAM
            elif re.search(r'^(data_)', name):
                d['identifier'] = tag
                d['codeSystem'] = 'EDAM'

            # default formats
            elif re.search(r'^([a-zA-Z]{1,}_[a-zA-Z0-9]{1,})$', name):
                d['identifier'] = tag
            
            else:
                print(f"Unable to process tag: {d['identifier']}")
                                
            if bool(d['identifier']):
                d['objectIRI'] = tag
                if not bool(d['label']):
                    d['label'] = name.replace('_', ':')
                if not bool(d['codeSystem']): 
                    d['codeSystem'] = name.split('_')[0]
                data.append(d)
    return data