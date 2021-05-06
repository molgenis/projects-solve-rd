# ////////////////////////////////////////////////////////////////////////////
# FILE: novelomics.sh
# AUTHOR: David Ruvolo
# CREATED: 2021-02-09
# MODIFIED: 2021-05-04
# PURPOSE: upload EMX for NovelOmics subpackage in the RD3 Portal 
# DEPENDENCIES: mcmd (molgenis commander)
# COMMENTS:
#   - mcmd: https://github.com/molgenis/molgenis-tools-commander
#   - R: R/novelomics.R
# ////////////////////////////////////////////////////////////////////////////

# install
# pip install molgenis-commander
# pip install --upgrade molgenis-commander

# config
mcmd config add host  # https://solve-rd-acc.gcc.rug.nl/
mcmd config add host  # https://solve-rd.gcc.rug.nl/
mcmd config set host

# //////////////////////////////////////

# ~ 1 ~
# Create Subpackage for storing original data
mcmd import -p data/novelomics/sys_md_Package.csv #--in rd3_portal
mcmd import -p data/novelomics/novelomics_attributes.csv --as attributes --in rd3_portal_novelomics
