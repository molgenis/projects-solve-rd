#///////////////////////////////////////////////////////////////////////////////
# FILE: build.py
# AUTHOR: David Ruvolo
# CREATED: 2022-10-03
# MODIFIED: 2022-11-08
# PURPOSE: Build RD3 EMX-YAML Models
# STATUS: stable
# PACKAGES: yamlemxconvert
# COMMENTS: saves using version number. Use mcmd import -p <path> --as rd3
#///////////////////////////////////////////////////////////////////////////////

from yamlemxconvert import Convert

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
filename = f"rd3.{rd3.version}"
rd3.write(name=filename, outDir="dist")
rd3.write_schema(path="model/schemas/rd3.md")


#///////////////////////////////////////

# ~ 1 ~
# Build Secondary Data Models

portal = Convert(files = [
  'model/portal/rd3_portal.yaml',
  'model/portal/rd3_portal_release.yaml',
  # 'model/portal/rd3_portal_novelomics.yaml',
  # 'model/portal/rd3_portal_cluster.yaml'
])

portal.convert()
portal.compileSemanticTags()
portal.write(name="rd3_portal", outDir="dist")

#///////////////////////////////////////

# ~ 2 ~
# Create Cluster Folder

cluster = Convert(files=['model/base_rd3_cluster.yaml', 'model/rd3_cluster_results.yaml'])
cluster.convert()
cluster.write('rd3_cluster', outDir="dist")


portalcluster = Convert(files=[
  'model/base_rd3_portal.yaml',
  'model/rd3_portal_cluster.yaml'
])
portalcluster.convert()
portalcluster.write('rd3_portal_cluster', outDir='dist')