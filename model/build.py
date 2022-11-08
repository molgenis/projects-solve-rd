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

# build RD3 emx-yaml model
rd3 = Convert(files=[
  'model/rd3/rd3.yaml',
  'model/rd3/rd3_info.yaml',
  'model/rd3/rd3_lookups.yaml',
  'model/rd3/rd3_variants.yaml'
])

rd3.convert()
rd3.compileSemanticTags()

for row in rd3.attributes:
  if row['dataType'] == 'categorical_mref':
    row['dataType'] = 'categoricalmref'

filename = f"rd3.{rd3.version}"
rd3.write(name=filename, outDir="dist")