"""EMX2 utils
FILE: utils.py
AUTHOR: David Ruvolo
CREATED: 2023-05-10
MODIFIED: 2023-05-10
PURPOSE: misc functions
STATUS: stable
PACKAGES: NA
COMMENTS: NA
"""

from csv import QUOTE_ALL
from rd3.utils.utils import recodeValue


def to_csv(path, data):
    """Save datatable object to csv"""
    return data.to_pandas().to_csv(path, index=False, encoding='UTF-8', quoting=QUOTE_ALL)


def to_csv_str(data):
    """Save datatable object to csv string"""
    return data.to_pandas().to_csv(index=False, encoding='UTF-8', quoting=QUOTE_ALL)


def recode_comma_string(mappings, value):
    """Recode values in a comma-separated string"""
    codes = value.split(',')
    terms = []
    for code in codes:
        value = recodeValue(mappings, code, 'HPO')
        terms.append(value)
    return ','.join(terms)
