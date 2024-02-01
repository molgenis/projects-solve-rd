"""GPAP Experiment report"""

from os import environ
import pandas as pd
from pandas.io.formats import excel
from tqdm import tqdm
from datatable import dt, f
from rd3tools.molgenis import Molgenis, get_table_attribs
from dotenv import load_dotenv
load_dotenv()

excel.ExcelFormatter.header_style = None

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

gpap_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverdportal_experiments',
    nested_columns='name'
)

# ///////////////////////////////////////

# summarise errors

# get a summary of the errors vs non-errors, and reduce to errors only
totals_dt = gpap_dt[:, dt.count(), dt.by(f.has_error)]

errors_dt = gpap_dt[f.has_error, :]

# errors are concatenated into a string, get unique errors
error_types = dt.unique(
    dt.Frame([
        value
        for array in dt.unique(errors_dt['error_type']).to_list()[0]
        for value in array.split(',')
    ])
).to_list()[0]

# find the number of cases per errors
error_summary = []
for error in tqdm(error_types):
    pattern = f".*{error}.*"
    error_count = errors_dt[dt.re.match(f.error_type, pattern), :].nrows
    error_summary.append({
        'has_error': True,
        'error_type': error,
        'count': error_count
    })

# finalise dataset
summary_dt = dt.Frame(error_summary)

# create URLs
URL = ''.join([
    'https://solve-rd.gcc.rug.nl',
    '/menu/data/dataexplorer',
    '?entity=solverdportal_experiments',
    '&hideselect=true',
    '&mod={}',
    '&filter=error_type==%27{}%27'
])


summary_dt['record_url'] = dt.Frame([
    URL.format("data", value.replace(' ', '%20'))
    for value in summary_dt['error_type'].to_list()[0]
])

summary_dt['aggregate_url'] = dt.Frame([
    URL.format("aggregates", value.replace(' ', '%20'))
    for value in summary_dt['error_type'].to_list()[0]
])


# summary_dt[0,'record_url']
# summary_dt[0,'aggregate_url']

# ///////////////////////////////////////

# Create dataset for excel

excel_dt = summary_dt.copy()
excel_dt['record_url'] = dt.Frame([
    f'=HYPERLINK("{value}", "View Data")'
    for value in summary_dt['record_url'].to_list()[0]
])

excel_dt['aggregate_url'] = dt.Frame([
    f'=HYPERLINK("{value}", "View Aggregates")'
    for value in excel_dt['aggregate_url'].to_list()[0]
])

# excel_dt[0,'record_url']
# excel_dt[0,'aggregate_url']

writer = pd.ExcelWriter('gpap_error_summary.xlsx', engine='xlsxwriter')

totals_dt.to_pandas() \
    .to_excel(writer, sheet_name='totals', index=False)

excel_dt.to_pandas() \
    .to_excel(writer, sheet_name='error summary', index=False)


writer.close()

# ///////////////////////////////////////

rd3.import_dt('solverdportal_experiment_counts', summary_dt)
