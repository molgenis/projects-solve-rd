# ////////////////////////////////////////////////////////////////////////////
# FILE: solvedstatus_upload.sh
# AUTHOR: David Ruvolo
# CREATED: 2021-02-19
# MODIFIED: 2021-02-19
# PURPOSE: push solved status emx to solve RD
# DEPENDENCIES: in.progress
# COMMENTS: Na
# ////////////////////////////////////////////////////////////////////////////

# set connection
mcmd config set host

# push to server
mcmd import -p data/tmp-solved-status/rd3_portal_solved_attributes.csv --as attributes --in rd3_portal