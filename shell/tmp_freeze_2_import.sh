# ////////////////////////////////////////////////////////////////////////////
# FILE: freeze2_import.sh
# AUTHOR: David Ruvolo
# CREATED: 2021-02-11
# MODIFIED: 2021-02-15
# PURPOSE: import emx file for solvedStatus intermediate table
# DEPENDENCIES: mcmd (molgenis commander; https://github.com/molgenis/molgenis-tools-commander)
# COMMENTS: run: `R/freeze2_emx.R`
# ////////////////////////////////////////////////////////////////////////////

# install
# pip install molgenis-commander
# pip install --upgrade molgenis-commander

# config
mcmd config add host  # https://solve-rd-acc.gcc.rug.nl/
mcmd config add host  # https://solve-rd.gcc.rug.nl/
mcmd config set host

# import data
mcmd import -p data/tmp-freeze-2/tmpFreeze2_attributes.csv --as attributes --in rd3_portal
