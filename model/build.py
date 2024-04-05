# ///////////////////////////////////////////////////////////////////////////////
# FILE: build.py
# AUTHOR: David Ruvolo
# CREATED: 2022-10-03
# MODIFIED: 2022-11-08
# PURPOSE: Build RD3 EMX-YAML Models
# STATUS: stable
# PACKAGES: yamlemxconvert
# COMMENTS: saves using version number. Use mcmd import -p <path> --as rd3
# ///////////////////////////////////////////////////////////////////////////////

import pandas as pd
from yamlemxconvert import Convert, convert2

# ~ 0 ~
# Compile RD3 data model

rd3 = Convert(files=[
    'model/rd3/rd3.yaml',
    'model/rd3/rd3_info.yaml',
    'model/rd3/rd3_lookups.yaml',
    'model/rd3/rd3_variants.yaml'
])

rd3.convert()
rd3.compileSemanticTags()

# manually fix categorical_mrefs --- I'm not sure why this throws an import error
for row in rd3.attributes:
    if row['dataType'] == 'categorical_mref':
        row['dataType'] = 'categoricalmref'

# write file and schemas
filename = f"dist/rd3.{rd3.version}.xlsx"
with pd.ExcelWriter(filename) as wb:
    packages_df = pd.DataFrame(rd3.packages)
    entities_df = pd.DataFrame(rd3.entities)
    attributes_df = pd.DataFrame(rd3.attributes)
    tags_df = pd.DataFrame(rd3.tags)

    packages_df.to_excel(wb, sheet_name='packages', index=False)
    entities_df.to_excel(wb, sheet_name='entities', index=False)
    attributes_df.to_excel(wb, sheet_name='attributes', index=False)
    tags_df.to_excel(wb, sheet_name='tags', index=False)

rd3.write_schema(path="model/schemas/rd3.md")


# OPTIONAL: convert to EMX2

files = [
    'model/rd3/rd3.yaml',
    'model/rd3/rd3_info.yaml',
    'model/rd3/rd3_lookups.yaml',
]

model = []

for file in files:
    emx2 = convert2.Convert2(file=file)
    emx2.convert()
    data = emx2.model['molgenis']
    for row in data:

        # pull URL from EMX1-YAML tags
        if bool(row['semantics']):
            row['semantics'] = row['semantics'].split(' ')[1]

        # recode ref types to ontology type
        if row['columnType'] in ['ref_array', 'ref']:
            row['columnType'] = row['columnType'].replace('ref', 'ontology')
    model.extend(data)

emx2.model['molgenis'] = model

emx2.write(name='rd3_emx2', format="xlsx", outDir='dist')


# ///////////////////////////////////////

# ~ 1 ~
# Build Secondary Data Models

portal = Convert(files=[
    'model/portal/rd3_portal.yaml',
    # 'model/portal/rd3_portal_release.yaml',
    'model/portal/rd3_portal_novelomics.yaml',
    # 'model/portal/rd3_portal_cluster.yaml'
])

portal.convert()
portal.compileSemanticTags()
with pd.ExcelWriter("./dist/rd3_portal.xlsx") as wb:
    packages_df = pd.DataFrame(portal.packages)
    entities_df = pd.DataFrame(portal.entities)
    attributes_df = pd.DataFrame(portal.attributes)
    packages_df.to_excel(wb, sheet_name='packages', index=False)
    entities_df.to_excel(wb, sheet_name='entities', index=False)
    attributes_df.to_excel(wb, sheet_name='attributes', index=False)

# ///////////////////////////////////////

# ~ 2 ~
# Create Cluster Folder

cluster = Convert(files=['model/base_rd3_cluster.yaml',
                  'model/rd3_cluster_results.yaml'])
cluster.convert()
cluster.write('rd3_cluster', outDir="dist")


portalcluster = Convert(files=[
    'model/base_rd3_portal.yaml',
    'model/rd3_portal_cluster.yaml'
])
portalcluster.convert()
portalcluster.write('rd3_portal_cluster', outDir='dist')
