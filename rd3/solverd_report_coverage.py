# ///////////////////////////////////////////////////////////////////////////////
# FILE: coverage_report.py
# AUTHOR: David Ruvolo
# CREATED: 2023-06-29
# MODIFIED: 2024-01-24
# PURPOSE: summary script for coverage report
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
# ///////////////////////////////////////////////////////////////////////////////


from os import environ
from datatable import dt, f
from rd3tools.utils import print2, flatten_data
from rd3tools.molgenis import Molgenis
from dotenv import load_dotenv
load_dotenv()

rd3 = Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


def get_subjects_data(columns: str = None):
    """Retrieve subject metadata and return as a datatable frame"""
    data_raw = rd3.get(
        entity='solverd_subjects',
        attributes=','.join(columns),
        batch_size=10000
    )
    data_flat = flatten_data(data_raw, 'subjectID|id|value')
    return dt.Frame(data_flat)


if __name__ == '__main__':
    print2('Retrieving metadata...')
    subj_cols = [
        'subjectID', 'sex1', 'fid', 'mid', 'pid', 'clinical_status', 'solved',
        'disease', 'phenotype', 'hasNotPhenotype', 'organisation', 'ERN', 'partOfRelease'
    ]

    subjects_dt = get_subjects_data(columns=subj_cols)
    total = subjects_dt['subjectID'].nrows
    stats = []
    for col_index, col in enumerate(subj_cols):
        print2('Summarising data for', col, '....')
        observed = subjects_dt[f[col] != None, f[col]].nrows
        stats.append({
            'id': f"coverage-subjects-{col_index}",
            'table': 'Subjects',
            'attribute': col,
            'expected': total,
            'observed': observed,
            'percent': round(observed / total, 4)
        })

    # import
    print2('Importing....')
    stats_dt = dt.Frame(stats)
    rd3.import_dt('rd3stats_coverage', stats_dt)
