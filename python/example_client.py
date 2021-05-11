#'////////////////////////////////////////////////////////////////////////////
#' FILE: example_client.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-11
#' MODIFIED: 2021-05-11
#' PURPOSE: example API client config
#' STATUS: working
#' PACKAGES: molgenis.client
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os  # for local testing only
import molgenis.client as molgenis

# set local token
# os.environ['molgenisToken'] = ''

# set current env
env = 'dev'

# configure session
# for urls, `/api/` is required
api = {
    'host': {
        'prod': 'https://my_production_hostname.com/api/',
        'dev' : 'https://my_development_hostname.com/api/'
    },
    'token': {
        'prod': '${molgenisToken}',
        'dev': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
    }
}

# init
m = molgenis.Session(url=api['host'][env], token=api['token'][env])