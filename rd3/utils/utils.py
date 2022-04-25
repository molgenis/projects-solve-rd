#'////////////////////////////////////////////////////////////////////////////
#' FILE: utils.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-04-25
#' MODIFIED: 2022-04-25
#' PURPOSE: misc RD3 tools
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from datetime import datetime
import numpy as np

def statusMsg(*args):
    """Status Message
    Print a log-style message, e.g., "[16:50:12.245] Hello world!"

    @param *args one or more strings containing a message to print
    @return string
    """
    t = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
    print('\033[94m[' + t + '] \033[0m' + ' '.join(map(str, args)))


def addForwardSlash(path: str = None):
    """Add Forward Slash
    Adds forward slash to the end of a path if missing
    
    @param path a string containing a path to a given location (file, url, etc.)
    @returns string
    """
    return path + '/' if path[len(path)-1] != '/' else path
    
def dtFrameToRecords(data):
    """Datatable object to records
    @param data : datatable object
    @return list of dictionaries
    """
    return data.to_pandas().replace({np.nan: None}).to_dict('records')
