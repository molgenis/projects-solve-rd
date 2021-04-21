# ////////////////////////////////////////////////////////////////////////////
# FILE: setup.sh
# AUTHOR: David Ruvolo
# CREATED: 2021-02-22
# MODIFIED: 2021-02-22
# PURPOSE: setup of local molgenis env
# DEPENDENCIES: mcmd
# COMMENTS: NA
# ////////////////////////////////////////////////////////////////////////////

# init dir structure
# mkdir data
# mkdir dev
# mkdir dev/dev-env
# mkdir dev/dev-env/data
# mkdir python
# mkdir R
# mkdir shell

# init molgenis dev-env - curl latest version as of file creation date
# https://github.com/molgenis/docker/tree/master/molgenis
# at the time of writing this script, 8.6 is the latest version
curl https://raw.githubusercontent.com/molgenis/docker/master/molgenis/8.6/backend.conf -o dev/dev-env/backend.conf
curl https://raw.githubusercontent.com/molgenis/docker/master/molgenis/8.6/docker-compose.yml -o dev/dev-env/docker-compose.yml
