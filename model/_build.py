#'////////////////////////////////////////////////////////////////////////////
#' FILE: build.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-09-01
#' MODIFIED: 2022-09-01
#' PURPOSE: build models on demand
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from yamlemxconvert import Convert

emx = Convert(files=[
  'model/base_rd3_portal.yaml',
  'model/rd3_portal_cluster.yaml'
])

emx.convert()
emx.write('rd3_portal_cluster', outDir='dist', includeData=False)