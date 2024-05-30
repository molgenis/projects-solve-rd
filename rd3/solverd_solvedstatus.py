"""Solve-RD Solved Status
FILE: solverd_solvedstatus.py
AUTHOR: David Ruvolo
CREATED: 2023-03-20
MODIFIED: 2024-05-30
PURPOSE: update solved status metadata in RD3
STATUS: stable
PACKAGES: **see below**
COMMENTS: NA
"""
# from os import environ
# from dotenv import load_dotenv
from datatable import dt, f
from datetime import datetime
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, recode_value, flatten_data, timestamp
# load_dotenv()


def set_data_period():
    """Get period from time script is run and the first of the month
    :returns array containing start (first day of the current month) and end dates (today)
    :rtype: datetime array
    """
    today = datetime.today()
    month = today.month
    year = today.year
    first_of_month = datetime(year, month, 1)
    return [first_of_month, today]


def collapse_remarks(*args):
    """Reduce multiple strings into a single string and filter NoneTypes"""
    values = list(args)
    values_cleaned = list(filter(None, values))
    return ';'.join(values_cleaned)


rd3 = Molgenis('http://localhost/api/', token='${molgenisToken}')
# rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
# rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])
# rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata
print2('Connecting to RD3 and retrieving data....')

current_period = [date.strftime("%Y-%m-%d") for date in set_data_period()]
QUERY = f'process_status==N;(date_solved=ge={current_period[0]};date_solved=le={current_period[1]})'

# Retrieve data and flatten to data table objects
# q='process_status=="N";(date_solved=ge=2024-05-01;date_solved=le=2024-05-20)',
portal_data_raw = rd3.get(
    entity='rd3_portal_recontact_solved',
    q=QUERY,
    batch_size=10000
)

portal_data = flatten_data(portal_data_raw, 'id')
portal_dt = dt.Frame(portal_data)

# retrive subject metadata and flatten; solved status metadata will be added later on
subjects_raw = rd3.get('solverd_subjects', batch_size=1000)
subjects = flatten_data(subjects_raw, 'subjectID|id|value')
subjects_dt = dt.Frame(subjects)
subjects_dt = subjects_dt[f.retracted != 'Y', :]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process current subject metadata
# To determine how to update solved status with incoming metadata, figure out
# which records should be updated and which records should be ignored
print2('Processing current subject information...')

# Flag cases that were solved before the start of the project. These will be ignored
subjects_dt['should_update'] = True
subjects_dt['should_update'] = dt.Frame([
    False if (
        row[1] == '2019-10-01' and
        row[2] == 'Solved before the initial start of the project'
    ) else row[0]
    for row in subjects_dt[:, ['should_update', 'date_solved', 'remarks']].to_tuples()
])

# print counts
should_update_counts = subjects_dt[:, dt.count(), dt.by(f.should_update)]
print2(
    'Summary of subjects that will be updated:\n\n',
    '\tAvailable:   \t', should_update_counts[
        f.should_update, 'count'
    ].to_list()[0][0],
    '\n'
    '\tIgnored*:\t', should_update_counts[
        f.should_update == False, 'count'
    ].to_list()[0][0],
    '\n\n',
    '*Ignored means the subject was solved before the start of the project (2019-10-01)'
)

# compile a list of subject ids that should be updated
subject_ids = subjects_dt['subjectID'].to_list()[0]
solved_subject_ids = subjects_dt[f.solved, 'subjectID'].to_list()[0]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process incoming metadata
print2('Processing incoming metadata...')

# Identify unknown subjects
# Check to see if there are any unknown identifiers and review if applicable
portal_dt['is_unknown_subject'] = dt.Frame([
    value not in subject_ids
    for value in portal_dt['subject'].to_list()[0]
])

# for any unknown subjects, make sure they are flagged properly
portal_dt[['history', 'remark', 'process_status']] = dt.Frame([
    ('Y', 'subject not yet in RD3', 'N')
    if value is True else (None, None, None)
    for value in portal_dt['is_unknown_subject'].to_list()[0]
])

# warn if there are any unknown subjects
unknown_count = portal_dt[f.is_unknown_subject, 'subject'].nrows
if unknown_count > 0:
    print2(
        f"Identified unknown subjects (n={unknown_count})",
        'Isolating records and importing...'
    )

    # select records where the subject is unknown and import
    unknown_dt = portal_dt[f.is_unknown_subject, :]
    rd3.import_dt('rd3_portal_recontact_solved', unknown_dt)

    # remove unknown subject cases from dataset and determine if script should exit
    portal_dt = portal_dt[f.is_unknown_subject == False, :]
    if not portal_dt.nrows:
        ERR_MSG = 'Error: All records contain unknown subject IDs. Please check this'
        raise SystemExit(ERR_MSG)

# ///////////////////////////////////////

# Process solved status metadata
print2('Processing new solved status data...')

# make sure all incoming solved status values are valid. If not, stop processing
solved_status_mappings = {'solved': True, 'unsolved': False, 'nA': None}
incoming_vals = dt.unique(portal_dt['solved']).to_list()[0]

for val in incoming_vals:
    if val not in solved_status_mappings:
        raise ValueError(f"Solved status value {val} is not valid")


# recode solved status
portal_dt['is_solved'] = dt.Frame([
    recode_value(solved_status_mappings, value, 'Solved Status')
    for value in portal_dt['solved'].to_list()[0]
])


# set remark value for solved status and flag cases where they should be updated
default_solved_record = (None, 'Y', 'P', True, False)
portal_dt[[
    'remark', 'history', 'process_status', 'should_update', 'should_update_solved'
]] = dt.Frame([
    (
        collapse_remarks(
            row[3], f"Solved status changed from solved to unsolved on {row[2]}"
        ),
        'Y',
        'P',
        True,
        True
    )
    if row[0] is True and row[1] in subject_ids and row[1] not in solved_subject_ids
    else default_solved_record
    for row in portal_dt[
        :, ['is_solved', 'subject', 'date_solved', 'remark']
    ].to_tuples()
])

# ///////////////////////////////////////

# Process recontact data
# Recontact information does not need to be cleaned. If the existing value is
# unknown or the incoming value is different, then replace the value in the
# subjects table with the incoming value
print2('Processing recontact data...')

portal_dt[[
    'remark', 'history', 'process_status', 'should_update', 'should_update_recontact'
]] = dt.Frame([
    (
        collapse_remarks(row[2], 'Recontact info updated'),
        'Y',
        'P',
        True,
        True
    )
    if subjects_dt[f.subjectID == row[1], 'recontact'].to_list()[0][0] != row[0]
    else (row[2], 'Y', 'P', True, False)
    for row in portal_dt[:, ['recontact', 'subject', 'remark']].to_tuples()
])

# portal_dt[:, dt.count(), dt.by(f.should_update_solved)]
# portal_dt[:, dt.count(), dt.by(f.should_update_recontact)]

# ///////////////////////////////////////

# Set remark for records that have not changed
print2('Identifying records that are processed and have no changes...')

portal_dt['should_update_count'] = dt.Frame([
    sum([int(value) for value in row])
    for row in portal_dt[
        :, ['should_update', 'should_update_recontact', 'should_update_solved']
    ].to_tuples()
])

portal_dt['remark'] = dt.Frame([
    'No new information' if row[0] == 1 else row[1]
    for row in portal_dt[:, ['should_update_count', 'remark']].to_tuples()
])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Update database with new data
# Update subject metadata with new contact and recontact information, as well as
# new solved status information. To reduce import times, indicate which
# records should be imported (i.e., changed) and separate

print2('Updating values in the subjects dataset...')

# reduce data to update records only
update_dt = portal_dt[
    f.should_update & (f.should_update_recontact | f.should_update_solved), :
]

update_ids = update_dt['subject'].to_list()[0]
subjects_dt['should_import'] = False

print2(
    'Will update records of', len(update_ids), 'subjects', '\n',
    '  New Solved Cases:', update_dt[f.should_update_solved, :].nrows, '\n',
    '  Recontact Cases:', update_dt[f.should_update_recontact, :].nrows, '\n'
)

for subj_id in update_ids:
    row = update_dt[f.subject == subj_id, :]

    # update solved status
    if row['should_update_solved'].to_list()[0][0] is True:
        new_solved_values = row[:, ['is_solved', 'date_solved']].to_tuples()[0]
        new_solved_values += (True,)
        subjects_dt[
            f.subjectID == subj_id, ['solved', 'date_solved', 'should_import']
        ] = new_solved_values

    # update recontact status
    if row['should_update_recontact'].to_list()[0][0] is True:
        new_recontact_values = (row['recontact'].to_list()[0][0], True)
        subjects_dt[
            f.subjectID == subj_id, ['recontact', 'should_import']
        ] = new_recontact_values


# isolate records to import to reduce import times
subjects_import_dt = subjects_dt[f.should_import, :]
subjects_import_dt['dateRecordUpdated'] = timestamp()
subjects_import_dt['wasUpdatedBy'] = 'rd3-bot'


# ///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Import data

print2('Importing', subjects_import_dt.nrows, 'into solverd_subjects....')
rd3.import_dt('solverd_subjects', subjects_import_dt)

print2('Updating metadata in staging table....')
rd3.import_dt('rd3_portal_recontact_solved', portal_dt)


rd3.logout()
