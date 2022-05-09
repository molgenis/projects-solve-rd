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
