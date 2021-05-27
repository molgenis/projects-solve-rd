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
env = 'dev'

class molgenisExtra(molgenis.Session):
    # deze klasse bevat alle functionaliteit die ook in de normale client.Session zit
    # + de extra functionaliteit die je zelf toevoegt:
    def batch_update_one_attr(self, entity, attr, values):
        add='No new data'
        for i in range(0, len(values), 1000):
            add='Update did tot go OK'
            """Updates one attribute of a given entity with the given values of the given ids"""
            response = self._session.put(self._url + "v2/" + quote_plus(entity) + "/" + attr,
                                         headers=self._get_token_header_with_content_type(),
                                         data=json.dumps({'entities': values[i:i+1000]}))

            if response.status_code == 200:
                add='Update went OK'
            else: 
                try:
                    response.raise_for_status()
                except requests.RequestException as ex:
                    self._raise_exception(ex)
                return response
        return add
    def get_freeze_subjects(self, freeze):
        entity = 'rd3_freeze' + freeze + '_subject'
        retracted = self.get(entity=entity, batch_size=1000, q="retracted=''")
        print('Total number of subjects where retracted is null', len(retracted))
        retracted_no_uknown = self.get(entity=entity, batch_size=1000, q="retracted=in=('N','U')")
        print('Total number of subjects where retracted is No or Unknown', len(retracted_no_uknown))
        retracted += retracted_no_uknown
        print('Total number of subjects where retracted is null, No, or Unknown'. len(retracted))
        return retracted


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
        if data['subjectID'] not in subject:
            subject[d['subjectID']]={
                'id': [d['id']],
                'solved': d['solved'],
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
rd3 = molgenisExtra(url=api['host'][env], token=api['token'][env])


# pull data
print('Fetching subject entities...')
freeze1_subjects = molgenisExtra.get_freeze_subjects(entity='rd3_freeze1_subject', freeze="1")
freeze2_subjects = molgenisExtra.get_freeze_subjects(entity='rd3_freeze2_subject', freeze="2")

# process freeze subject data
print("Processing subject data...")
freeze1_subjects_processed = process_freeze_subjects(data = freeze1_subjects)
freeze2_subjects_processed = process_freeze_subjects(data = freeze2_subjects)

# Read the unprocessed data from rd3_portal_recontact_solved
print('Fetching portal data...')
portal_data = rd3.get('rd3_portal_recontact_solved', q='process_status=="N"')
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
                        updateRemark.appen({'id': id, 'remarks': remarkNew})
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


# Store and process the portal data
# for data in portal_data:
#     history='N'
#     ids=subject[data['subject']]['id']
#     contactNew=None
#     dateNew=None
#     ptlRemark=None
#     ptlStatus=None
#     recontactNew=None
#     remarkNew=None
#     solvedNew=None
    
#     if data['solved'] == 'solved': solved = True
#     elif data['solved'] == 'unsolved': solved = False
#     elif data['solved'] == 'nA': solved = None
#     else: raise SystemExit('Unknown Portal solved status '+  data['solved'] + ' for '+ data['subject'])    

#     if subject[data['subject']]['solved'] != solved:
#         print('Solved status ongelijk voor', data['subject'], 'old', subject[data['subject']]['solved'], 'new', data['solved'])
#         if subject[data['subject']]['update_solved_status'] == 'Y':
#             if not subject[data['subject']]['solved']:
#                 solvedNew = solved
#                 dateNew =  data['date_solved']
#                 remarkNew = None
#             else:
#                 print('Solved status changed from solved to unsolved for', data['subject'])
#                 solvedNew = solved
#                 dateNew = None
#                 remarkNew = 'Solved status changed from solved to unsolved on '+data['date_solved']
#         else:
#             ptlRemark='New solved status, but not updated!'
#             ptlStatus='D'
#     if 'recontact' in data:
#         if subject[data['subject']]['recontact'] == 'U': recontactNew = data['recontact'] 
#         elif subject[data['subject']]['recontact'] != data['recontact']:
#             print('New recontact information for', data['subject'], 'old', subject[data['subject']]['recontact'], 'new', data['recontact'])
#             recontactNew = data['recontact']
#     if 'contact' in data:
#         if subject[data['subject']]['contact'] == 'missing': contactNew = data['contact']
#         elif subject[data['subject']]['contact'] != data['contact']:
#             print('New contact information for', data['subject'], 'old', subject[data['subject']]['contact'], 'new', data['contact'])
#             contactNew = data['contact']
#     if contactNew != None or recontactNew != None or solvedNew != None:
#         for id in ids:
#             if contactNew != None:
#                 if ptlRemark == None: ptlRemark='Contact info updated'
#                 else: ptlRemark = ptlRemark + ' Contact info updated'
#                 ptlStatus = 'P'
#                 updateContact.append({'id': id, 'contact': contactNew})
#             if recontactNew != None:
#                 if ptlRemark == None: ptlRemark='Recontact info updated'
#                 else: ptlRemark=ptlRemark+' and recontact info updated'
#                 ptlStatus = 'P'
#                 updateRecontact.append({'id': id, 'recontact': recontactNew})
#             if solvedNew != None:
#                 if ptlRemark == None: ptlRemark = 'Solved status updated'
#                 else: ptlRemark = ptlRemark+' and solved status updated'
#                 ptlStatus = 'P'
#                 updateSolved.append({'id': id, 'solved': solvedNew})
#                 updateDate.append({'id': id, 'date_solved': dateNew})
#                 updateRemark.append({'id': id, 'remarks': remarkNew})
#     if ptlRemark != None:
#         updatePtlHistory.append({'id': data['id'], 'history': 'Y'})
#         updatePtlRemark.append({'id': data['id'], 'remark': ptlRemark})
#         updatePtlStatus.append({'id': data['id'], 'process_status': ptlStatus})
#     else:
#         # No new information
#         updatePtlHistory.append({'id': data['id'], 'history': 'N'})
#         updatePtlRemark.append({'id': data['id'], 'remark': 'No new information'})
#         updatePtlStatus.append({'id': data['id'], 'process_status': 'P'})

# print('Number of solved to be updated is', len(updateSolved))
# print('Number of date_solved to be updated is', len(updateDate))
# print('Number of contact information to be updated is', len(updateContact))
# print('Number of recontact information to be updated is', len(updateRecontact))
# print('Number of remarks to be updated is', len(updateRemark))

# print('Number of records of which the solve status should not be updated is', no_solved_update)

# print('Number of Portal history to be updated is', len(updatePtlHistory))
# print('Number of Portal remarks to be updated is', len(updatePtlRemark))
# print('Number of Portal status to be updated is', len(updatePtlStatus))

# # Update the columns contact, reconcact, solved and date_solved in rd3_freeze1_subject
# print('Update contact information')
# update=rd3.batch_update_one_attr(entity='rd3_freeze1_subject', attr='contact', values=updateContact)
# print(update)
# print('Update recontact information')
# update=rd3.batch_update_one_attr(entity='rd3_freeze1_subject', attr='recontact', values=updateRecontact)
# print(update)
# print('Update solved status')
# update=rd3.batch_update_one_attr(entity='rd3_freeze1_subject', attr='solved', values=updateSolved)
# print(update)
# print('Update date_solved')
# update=rd3.batch_update_one_attr(entity='rd3_freeze1_subject', attr='date_solved', values=updateDate)
# print(update)
# print('Update remarks')
# update=rd3.batch_update_one_attr(entity='rd3_freeze1_subject', attr='remarks', values=updateRemark)
# print(update)

# # Update the portal table:
# print('Update portal history')
# update=rd3.batch_update_one_attr(entity='rd3_portal_recontact_solved', attr='history', values=updatePtlHistory)
# print(update)
# print('Update portal history')
# update=rd3.batch_update_one_attr(entity='rd3_portal_recontact_solved', attr='process_status', values=updatePtlStatus)
# print(update)
# print('Update portal history')
# update=rd3.batch_update_one_attr(entity='rd3_portal_recontact_solved', attr='remark', values=updatePtlRemark)
# print(update)

# # Finally check if all New portal records are processed
# portal_data = rd3.get('rd3_portal_recontact_solved', q='process_status=="N"')

# if len(portal_data) > 0:
#     raise SystemExit('Not all new portal records are processed!!!') 

# print('FINISHED')