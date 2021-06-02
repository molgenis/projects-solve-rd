#'////////////////////////////////////////////////////////////////////////////
#' FILE: solved_status_mapping.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-17
#' MODIFIED: 2021-05-27
#' PURPOSE: update solved status metadata from freezes
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os  # for local testing only
import json
import molgenis.client as molgenis
import requests
from urllib.parse import quote_plus, urlparse, parse_qs


# set local token
# os.environ['molgenisToken'] = ''
env = 'prod'

class molgenisExtra(molgenis.Session):
    # deze klasse bevat alle functionaliteit die ook in de normale client.Session zit
    # + de extra functionaliteit die je zelf toevoegt:
    def batch_update_one_attr(self, entity, attr, values):
        add='No new data'
        for i in range(0, len(values), 1000):
            add='Update did tot go OK'
            """Updates one attribute of a given entity with the given values of the given ids"""
            response = self._session.put(
                self._api_url + "v2/" + quote_plus(entity) + "/" + attr,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': values[i:i+1000]})
            )
            if response.status_code == 200:
                add='Update went OK'
            else: 
                try:
                    response.raise_for_status()
                except requests.RequestException as ex:
                    self._raise_exception(ex)
                return response
        return add

# @title Process Freeze Subjects
# @description store the current contact, recontact and solved information
# @param data list of dictionaries containing `rd3_freeze*_subjects`
# @return list of named dictionaries
def process_freeze_subjects(data):
    subject = {}
    no_solved_update = 0
    for d in data:
        contactInfo = 'missing'
        date_solved=''
        recontact = 'U'
        update_solved_status = 'Y'
        if 'recontact' in d: recontact = d['recontact']['id']
        if 'contact' in d: contactInfo = d['contact']
        if 'date_solved' in d:
            if date_solved == '2019-10-01' and d['remarks'] == 'Solved before the initial start of the project':
                update_solved_status = 'N'
                no_solved_update += 1
        if d['subjectID'] not in subject:
            subject[d['subjectID']]={
                'id': [d['id']],
                'solved': d['solved'] if 'solved' in d else None,
                'date_solved': date_solved,
                'recontact': recontact,
                'contact': contactInfo,
                'update_solved_status': update_solved_status
            }
        else:
            print('ID already exists')
            ids=subject[d['subjectID']]['id']
            ids.append(d['id'])
            if d['solved'] != subject[d['subjectID']]['solved']:
               raise SystemExit('Differences in current solved status in RD3 for '+d['subjectID']+' FIRST CHECK THIS!')
            if recontact != subject[d['subjectID']]['recontact']:
                raise SystemExit('Differences in current Recontact Incidental Findings in RD3 for '+d['subjectID']+' FIRST CHECK THIS!')
            if contactInfo != subject[d['subjectID']]['contact']:
                raise SystemExit('Differences in current Contact Information in RD3 for '+d['subjectID']+' FIRST CHECK THIS!')
            subject[d['subjectID']]={
                'id': ids,
                'solved': d['solved'],
                'date_solved': date_solved,
                'recontact': recontact,
                'contact': contactInfo,
                'update_solved_status': update_solved_status
            }
    print('Number of records of which the solve status should not be updated is', no_solved_update)
    return subject


# init session
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc' : 'https://solve-rd-acc.gcc.rug.nl/api/',
        'dev' : 'https://solve-rd-acc.gcc.rug.nl/api/',
    },
    'token': {
        'prod': '${molgenisToken}',
        'acc': '${molgenisToken}',
        'dev': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
    }
}

# init session
# rd3 = molgenisExtra(url=api['host']['prod'], token=api['token'][env])
rd3 = molgenisExtra(url=api['host']['prod'], token=os.environ['molgenisToken'])

# @title Get Freeze Subjects
# @description Pull data from `rd3_freeze*_subject`
# @param freeze target freeze to pull data (i.e., 1, 2, ...)
# @return list of dicts
def get_freeze_subjects(freeze):
    attribs = 'id,subjectID,solved,remarks,recontact,contact,remarks'
    entity = 'rd3_freeze' + str(freeze) + '_subject'
    retracted = rd3.get(entity=entity, attributes=attribs, batch_size=1000, q="retracted==''")
    print('Total number of subjects where retracted is null', len(retracted))
    retracted_no_unknown = rd3.get(entity=entity, attributes=attribs, batch_size=1000, q="retracted=in=('N','U')")
    print('Total number of subjects where retracted is No or Unknown', len(retracted_no_unknown))
    retracted += retracted_no_unknown
    print('Total number of subjects where retracted is null, No, or Unknown', len(retracted))
    return retracted

# pull data
print('Fetching subject entities...')
freeze1_subjects = get_freeze_subjects(freeze="1")
freeze2_subjects = get_freeze_subjects(freeze="2")

# process freeze subject data
print("Processing subject data...")
freeze1_subjects_processed = process_freeze_subjects(data = freeze1_subjects)
freeze2_subjects_processed = process_freeze_subjects(data = freeze2_subjects)

# Read the unprocessed data from rd3_portal_recontact_solved
print('Fetching portal data...')
portal_data = rd3.get('rd3_portal_recontact_solved', q='process_status=="N"', batch_size=10000)
print('Number of new portal records', len(portal_data))

# @title Process Portal Status
def process_portal_status(data, subjects):
    updateContact = []
    updateDate = []
    updatePtlHistory = []
    updatePtlStatus = []
    updatePtlRemark = []
    updateRecontact = []
    updateRemark = []
    updateSolved = []
    for d in data:
        history='N'
        contactNew = None
        dateNew = None
        ptlRemark = None
        ptlStatus = None
        recontactNew = None
        remarkNew = None
        solvedNew = None
        status = True if d['subject'] in subjects else False
        if status:
            ids = subjects[d['subject']]['id']
            if d['solved'] == 'solved': solved = True
            elif d['solved'] == 'unsolved': solved = False
            elif d['solved'] == 'nA': solved = None
            else: raise SystemExit('Unknown Portal solved status' + d['solved'] + ' for ' + d['subject'])
            if subjects[d['subject']]['solved'] != solved:
                print('Solved Status ongelijk voor', d['subject'], 'old', subjects[d['subject']]['solved'], 'new', d['solved'])
                if subjects[d['subject']]['update_solved_status'] == 'Y':
                    if not subjects[d['subject']]['solved']:
                        solvedNew = solved
                        dateNew = d['date_solved']
                        remarkNew = None
                    else:
                        print('Solved status changed from solved to unsolved for', d['subject'])
                        solvedNew = solved
                        dateNew = None
                        remarkNew = 'Solved status changed from solved to unsolved on' + d['date_solved']
                else:
                    ptlRemark = 'New solved status, but not updated!'
                    ptlStatus='D'
            if 'recontact' in d:
                if subjects[d['subject']]['recontact'] == 'U': recontactNew = d['recontact']
                elif subjects[d['subject']]['recontact'] != d['recontact']:
                    print('New recontact information for', d['subject'], 'old', subjects[d['subject']]['recontact'], 'new', d['recontact'])
                    recontactNew = d['recontact']
            if 'contact' in d:
                if subjects[d['subject']]['contact'] == 'missing': contactNew = d['contact']
                elif subjects[d['subject']]['contact'] != d['contact']:
                    print('New contact information for', d['subject'], 'old', subjects[d['subject']]['contact'], 'new', d['contact'])
                    contactNew = d['contact']
            if contactNew != None or recontactNew != None or solvedNew != None:
                for id in ids:
                    if contactNew != None:
                        if ptlRemark == None: ptlRemark='Contact info updated'
                        else: ptlRemark = ptlRemark +  ' Contact info updated'
                        ptlStatus = 'P'
                        updateContact.append({'id': id, 'contact': contactNew})
                    if recontactNew != None:
                        if ptlRemark == None: ptlRemark='Recontact info updated'
                        else: ptlRemark = ptlRemark + ' and recontact info updated'
                        ptlStatus='P'
                        updateRecontact.append({'id': id, 'recontact': recontactNew})
                    if solvedNew != None:
                        if ptlRemark == None: ptlRemark = 'Solved Status updated'
                        else: ptlRemark = ptlRemark + ' and solved status updated'
                        ptlStatus = 'P'
                        updateSolved.append({'id': id, 'solved': solvedNew})
                        updateDate.append({'id': id, 'date_solved': dateNew})
                        updateRemark.append({'id': id, 'remarks': remarkNew})
            if ptlRemark != None:
                updatePtlHistory.append({'id': d['id'], 'history': 'Y'})
                updatePtlRemark.append({'id': d['id'], 'remark': ptlRemark})
                updatePtlStatus.append({'id': d['id'], 'process_status': ptlStatus})
            else:
                updatePtlHistory.append({'id': d['id'], 'history': 'Y'})
                updatePtlRemark.append({'id': d['id'], 'remark': 'No new information'})
                updatePtlStatus.append({'id': d['id'], 'process_status': 'P'})
    return {
        'updateSolved': updateSolved,
        'updateDate': updateDate,
        'updateContact': updateContact,
        'updateRecontact': updateRecontact,
        'updateRemark': updateRemark,
        'updatePtlHistory': updatePtlHistory,
        'updatePtlRemark': updatePtlRemark,
        'updatePtlStatus': updatePtlStatus
    }

### PICK UP HERE CONTINUE WITH UPDATING ENTITIES ###
freeze1 = process_portal_status(portal_data, freeze1_subjects_processed)
freeze2 = process_portal_status(portal_data, freeze2_subjects_processed)


# @title Describe processed freeze data
# @describe print summary info about the processed metadata
# @param data returned object of `process_portal_status`
# @return console messages
def describe_processed_freeze_data(data):
    print('Number of solved to be updated is', len(data['updateSolved']))
    print('Number of date_solved to be updated is', len(data['updateDate']))
    print('Number of contact information to be updated is', len(data['updateContact']))
    print('Number of recontact information to be updated is', len(data['updateRecontact']))
    print('Number of remarks to be updated is', len(data['updateRemark']))
    print('Number of Portal history to be updated is', len(data['updatePtlHistory']))
    print('Number of Portal remarks to be updated is', len(data['updatePtlRemark']))
    print('Number of Portal status to be updated is', len(data['updatePtlStatus']))

# print descripptives by freeze
describe_processed_freeze_data(freeze1)
describe_processed_freeze_data(freeze2)

# @title Import Data
# @describe Import processed solved status into freeze*_subject and portal recontact
# @param entity rd3_freeze*_subject entity
# @param data output of `process_portal_status`
def import_data(entity, data):
    print('Importing data into' + str(entity))
    print('Updating contact information...')
    update=rd3.batch_update_one_attr(entity=entity, attr='contact', values=data['updateContact'])
    print(update)
    print('Updating recontact information...')
    update=rd3.batch_update_one_attr(entity=entity, attr='recontact', values=data['updateRecontact'])
    print(update)
    print('Updating solved status...')
    update=rd3.batch_update_one_attr(entity=entity, attr='solved', values= data['updateSolved'])
    print(update)
    print('Updating date_solved...')
    update=rd3.batch_update_one_attr(entity=entity, attr='date_solved', values= data['updateDate'])
    print(update)
    print('Updating remarks...')
    update=rd3.batch_update_one_attr(entity=entity, attr='remarks', values= data['updateRemark'])
    print(update)
    print('Updating portal history...')
    update=rd3.batch_update_one_attr(entity='rd3_portal_recontact_solved', attr='history', values=data['updatePtlHistory'])
    print(update)
    print('Updating portal status...')
    update=rd3.batch_update_one_attr(entity='rd3_portal_recontact_solved', attr='process_status', values=data['updatePtlStatus'])
    print(update)
    print('Updating portal remarks...')
    update=rd3.batch_update_one_attr(entity='rd3_portal_recontact_solved', attr='remark', values=data['updatePtlRemark'])
    print(update)

# update appropriate freeze table and portal
import_data(entity='rd3_freeze1_subject', data=freeze1)
import_data(entity='rd3_freeze2_subject', data=freeze2)

# # Finally check if all New portal records are processed
portal_data = rd3.get('rd3_portal_recontact_solved', q='process_status=="N"')

if len(portal_data) > 0:
    raise SystemExit('Not all new portal records are processed!!!') 

print('FINISHED')