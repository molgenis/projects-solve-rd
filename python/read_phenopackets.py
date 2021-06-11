"""
    Revision history:
    20200319 Dieuwke Roelofs-Prins: Created

    Description:
    With this Python script phenopacket information can be pushed into the RD3 database:
    - rd3_subject
    - rd3_subject_info

    Phenopacket elements that are defined are:
    - id
    - subject
    - phenotypes
    - metaData
    - MME

    => Missing ones will be logged

"""

# Import modules
import csv
import datetime
import json
import molgenis.client as molgenis_data_api
import os
import subprocess
import sys

import requests
from urllib.parse import quote_plus

# Define variables
os.environ['molgenisToken'] = ''
disease_codes=[]
hpo_codes=[]
known_disease_items=['term', 'classOfOnset']
known_file_elements=['resolutionStatus', 'phenopacket']
known_phenopacket_elements=['id', 'subject', 'phenotypicFeatures', 'genes', 'variants', 'diseases', 'metaData']
known_phenotypicFeature_items=['type', 'negated']
known_subject_items=['sex', 'dateOfBirth']
n_files_processed=0
n_removed_diseases=0
n_removed_hpo=0
pheno_files=[]
rd3_sex_codes={'MALE' : 'M', 'FEMALE': 'F', 'OTHER_SEX': 'UD'}
rd3_subject_All=[]
rd3_subjectInfo_All=[]
sep=','
subject_attributes=['identifier', 'sex1', 'phenotype', 'hasNotPhenotype', 'phenopacketsID', 'disease']
subjectInfo_attributes=['id', 'subjectnr', 'dateofBirth', 'ageOfOnset']

rd3_session = molgenis_data_api.Session('https://solve-rd.gcc.rug.nl/api/',token=os.environ['molgenisToken'])

# Directory and file definitions
# If the script runs on the same system as where the files are located
output_folder='./'
disease_error_file=open(output_folder+'disease_errors.txt', 'w')
hpo_error_file=open(output_folder+'hpo_errors.txt', 'w')
md5_file=''
#phenopacket_files=os.scandir(phenopacket_folder)

# Files are located remotely
phenopacket_folder='/groups/solve-rd/tmp10/releases/freeze2/phenopacket/'
phenopacket_files= subprocess.Popen(['ssh', 'corridor+fender', 'ls', phenopacket_folder],
                       stdout=subprocess.PIPE, universal_newlines=True)

# Output files #Used until API works OK, in the end not used as data is pushed into RD3 database using Data API
rd3_subject_file=open(output_folder+'rd3_freeze2_subject.csv', 'w')
rd3_subjectInfo_file=open(output_folder+'rd3_freeze2_subjectinfo.csv', 'w')

rd3_subject_csv = csv.DictWriter(rd3_subject_file, fieldnames=subject_attributes, delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
rd3_subject_csv.writeheader()

rd3_subjectInfo_csv = csv.DictWriter(rd3_subjectInfo_file, fieldnames=subjectInfo_attributes, delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
rd3_subjectInfo_csv.writeheader()

# Loop through the list with phenopacket files that are on Gearshift
print ("Start processing phenopacket files at", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
start_script = datetime.datetime.now()

i=0
print('Store all files names in a temporary list otherwise connection closed by remote host can occur in case of many files')
for file in phenopacket_files.stdout:
    if os.path.splitext(file)[1].strip() == '.md5' : md5_file = file.strip() # strip to remove newline
    if os.path.splitext(file)[1].strip() == '.json': pheno_files.append(file.strip())
phenopacket_files.stdout.close()    

print('Number of phenopackets files on Gearshift is', len(pheno_files))

if len(md5_file) == 0:
    print('No checksum file available quit processing')
    raise Exception('No checksum file available quit processing')
else:
    print('md5 file is', md5_file)

######## Get all HPO codes to check if the ones in the phenopackets are in RD3, missing ones will be logged in a separate file
rd3_phenotypes=rd3_session.get('rd3_phenotype', batch_size=10000)

for phenotype in rd3_phenotypes:
    hpo_codes.append(phenotype['id'])

######## Get all Disease codes to check if the ones in the phenopackets are in RD3, missing ones will be logged in a separate file
rd3_diseases=rd3_session.get('rd3_disease', batch_size=10000)

for disease in rd3_diseases:
    disease_codes.append(disease['id'])

####### Get the checksums of the files to check if files on Gearshift are complete ####
rd3_checksums=rd3_session.get('rd3_freeze2_file', batch_size=10000, attributes='name,md5', q='typeFile=="json"')
print('Number of phenopacket files in RD3 is', len(rd3_checksums))
EGA_checksum={}
for checksum in rd3_checksums:
    EGA_checksum[checksum['name'].split('/')[-1].replace('.cip','')]=checksum['md5']

# Store the checksum of the files on Gearshift
checksum_file = subprocess.Popen(['ssh', 'corridor+fender', 'cat', phenopacket_folder+md5_file],
                   stdout=subprocess.PIPE, universal_newlines=True)
file_checksum={}
for file_content in checksum_file.stdout:
    file_checksum[file_content.split()[1]] = file_content.split()[0]
checksum_file.stdout.close()

for file in pheno_files:
    #    i += 1
    #    if i==10: break
    # Check if the stop-file exists, this is to quit the process in a normal way
    # in stead of killing the process
    if os.path.isfile(output_folder+'stop_process.txt'):
        print ("\nSTOP: Stop file exists, so stop processing data")
        rd3_session.logout
        rd3_subject_file.close()
        rd3_subject_info_file.close()        
        exit()  
    rd3_subject={} # Empty the dictionary
    rd3_subjectInfo={} # Empty the dictionary
    # Define some of the attributes in the dictionary
    rd3_subjectInfo['ageOfOnset']=[]
    rd3_subject['disease']=[]
    rd3_subject['hasNotPhenotype']=[]
    rd3_subject['phenotype']=[]
    ##    i += 1
    ##    if i==6:
    ##        print(rd3_subject_All)
    ##        print(rd3_subjectInfo_All)
    ##        print(rd3_session._get_token_header_with_content_type())
    ###        response = requests.put('https://molgenis124.gcc.rug.nl/api/' + "v2/" + quote_plus('rd3_subject') + "/sex1",
    ##        response = requests.put('https://molgenis124.gcc.rug.nl/api/v2/rd3_subject/sex1',
    ##                        headers=rd3_session._get_token_header_with_content_type(),
    ##                                     data=json.dumps({'entities' : [{'identifier': 'P0000037', 'sex1': "F"}]}))
    ##                                #data=json.dumps({'entities': rd3_subject_All}))
    ##        print(response.status_code)
    ##        print(response.text)
    ##
    ##        try:
    ##            response.raise_for_status()
    ##        except:    
    ##            print(json.loads(response.content.decode("utf-8"))['errors'][0]['message'])    
    ##
    ####        try:
    ####            response.raise_for_status()
    ####        except requests.RequestException as ex:
    ####            print(ex)        
    ##        exit()
    start_file=datetime.datetime.now()        
    rd3_subject['phenopacketsID'] = os.path.basename(file)
    print('\nProcess file', rd3_subject['phenopacketsID'])
#   Check if the checksum of the file on Gearshift is the same as the EGA checksum (stored in RD3)
    # if file not in EGA_checksum:
    #     print('EGA checksum of file {0} is not available, phenopacket file information is not processed'.format(file))
    #     continue
    # if file not in file_checksum:
    #     print('Gearshift checksum of file {0} is not available, phenopacket file information is not processed'.format(file))
    #     continue
    # if EGA_checksum[file] != file_checksum[file]:
    #     print('Checksum {0} of the file {1} is not equal to the EGA checksum {2} => Phenopacket file information not added!'.format(file_checksum[file],file,EGA_checksum[file]))
    #     continue
    n_files_processed += 1
    #        In the script runs on the same system as where the files are
    #        with open(file, 'r') as pp_file:
    #            phenopacket = json.load(pp_file)
    #       In case the files are remotely located
    #       Get the content of the phenopacket file
    phenopacket_file = subprocess.Popen(['ssh', 'corridor+fender', 'cat', phenopacket_folder+file],
                   stdout=subprocess.PIPE, universal_newlines=True)
    phenopacket_file.wait()
    for file_content in phenopacket_file.stdout: phenopacket=json.loads(file_content)
    for file_element, content in phenopacket.items():
        # Check if there are any new file elements
        if file_element not in known_file_elements: raise Exception('Undefined file element {}'.format(file_element))
        # ResolutionStatus information, but that one is already filled in RD3
        # if file_element == 'resolutionStatus': rd3_subject['solved']=content
        # Phenopacket information (elements id, subject, phenotypicFeatures, genes, variants and metaData
        if file_element == 'phenopacket':
            if type(content) is not dict: raise Exception('Phenopacket element is not a dictionary')
            for phenopacket_element in content.keys():
                if phenopacket_element not in known_phenopacket_elements: print('WARNING: Undefined phenopacket element {}'.format(phenopacket_element)) # raise Exception('Undefined phenopacket element {}'.format(phenopacket_element))
                if phenopacket_element == 'id': rd3_subject['id']=content['id']; rd3_subjectInfo['id']=content['id']; rd3_subjectInfo['subjectID']=content['id']
                if phenopacket_element == 'subject':
                    if type(content['subject']) is not dict: raise Exception('Subject element is not a dictionary')
                    for subject_item in content['subject']:
                        if subject_item not in known_subject_items: print('WARNING: Undefined subject item {}'.format(subject_item)) #raise Exception('Undefined subject item {}'.format(subject_item))  
                        try: rd3_subject['sex1'] = content['subject']['sex']
                        except: print('Sex is not in the phenopacket of subject {}'.format(rd3_subject['identifier']))
                        rd3_subject['sex1']=rd3_subject['sex1'].replace(rd3_subject['sex1'], rd3_sex_codes[rd3_subject['sex1']])
                        
                        try:  rd3_subjectInfo['dateofBirth'] = content['subject']['dateOfBirth']
                        except: print('Date of birth not in the phenopacket of subject {}'.format(rd3_subject['identifier']))
                # PhenotypicFeatures (items type and negated)
                if phenopacket_element == 'phenotypicFeatures':
                    if type(content['phenotypicFeatures']) is not list: raise Exception('PhenotypicFeatures element is not a list!!!')
                    for phenotype in content['phenotypicFeatures']:
                        # Check if there are any unknown PhenotypicFeatures items
                        for phenotype_item in phenotype.keys():
                            if phenotype_item not in known_phenotypicFeature_items: print('WARNING: Undefined phenotypicFeatures item {}'.format(phenotype_item)) #raise Exception('Undefined phenotypicFeatures item {}'.format(phenotype_item))
                        try:
                            if phenotype['negated']:
                                if phenotype['type']['id'].replace(':', '_') not in rd3_subject['hasNotPhenotype']:
                                    rd3_subject['hasNotPhenotype'].append(phenotype['type']['id'].replace(':','_'))
                                else:
                                    print('WARNING DUPLICATE hasNotPhenotype {0} in phenopacket of subject {1}'.format(phenotype['type']['id'], rd3_subject['identifier']), 'removed')
                                    hpo_error_file.write('%s %s %s %s %s\n' % ('WARNING DUPLICATE hasNotPhenotype', phenotype['type']['id'], 'in phenopacket of subject',  rd3_subject['identifier'], 'removed'))
                           # print('hasnot', rd3_subject['hasNotPhenotype'])
                            elif not phenotype['negated']:
                                rd3_subject['phenotype'].append(phenotype['type']['id'].replace(':','_'))
                           # print('negated phenotype false', rd3_subject['phenotype'])
                        except:
                            if phenotype['type']['id'].replace(':', '_') not in rd3_subject['phenotype']:
                                rd3_subject['phenotype'].append(phenotype['type']['id'].replace(':', '_'))
                            else:
                                print('WARNING DUPLICATE phenotype {0} in phenopacket of subject {1}'.format(phenotype['type']['id'], rd3_subject['identifier']), 'removed')
                                hpo_error_file.write('%s %s %s %s %s\n' % ('WARNING DUPLICATE Phenotype', phenotype['type']['id'], 'in phenopacket of subject',  rd3_subject['identifier'], 'removed'))
                # Disease information
                if phenopacket_element == 'diseases':
                    if type(content['diseases']) is not list: raise Exception('Diseases element is not a list!!!')
                    for disease in content['diseases']:
                        get_next=False
                        for disease_item in disease.keys():
                            if get_next: break
                            if disease_item not in known_disease_items: print('WARNING: Undefined disease item {}'.format(disease_item)) #raise Exception('Undefined disease item {}'.format(disease_item)
                            # term is required
                            if disease_item == 'term':
                                try:
                                    if disease['term']['id'].replace(':','_') not in rd3_subject['disease']:
                                        rd3_subject['disease'].append(disease['term']['id'].replace(':','_'))
                                    else:
                                        print('WARNING DUPLICATE disease code {0} in phenopacket of subject {1}'.format(disease['term']['id'], rd3_subject['identifier']), 'removed')
                                        disease_error_file.write('%s %s %s %s %s\n' % ('WARNING DUPLICATE disease code', disease['term']['id'], 'in phenopacket of subject',  rd3_subject['identifier'], 'removed'))
                                except:
                                    print('ERROR: Disease item available, but ID is missing in the phenopacket of subject {}'.format(rd3_subject['identifier']))
                            # classOfOnset
                            if disease_item == 'classOfOnset' :
                                try:
                                    if disease['classOfOnset']['id'].replace(':','_') not in rd3_subjectInfo['ageOfOnset']:
                                        rd3_subjectInfo['ageOfOnset'].append(disease['classOfOnset']['id'].replace(':','_'))
                                    else:
                                        print('WARNING DUPLICATE classOfOnset {0} in phenopacket of subject {1}'.format(disease['classOfOnset']['id'], rd3_subject['identifier']), 'removed')
                                        hpo_error_file.write('%s %s %s %s %s\n' % ('WARNING DUPLICATE classOfOnset', disease['classOfOnset']['id'], 'in phenopacket of subject',  rd3_subject['identifier'], 'removed'))
                                except:
                                    print('ERROR: classOfOnset ID is missing in the phenopacket of subject {}'.format(rd3_subject['identifier']))
            # Genes information
            if phenopacket_element == 'genes': print('WARNING: Genes information available for subject {}'.format(rd3_subject['identifier'])) #raise Exception('Genes information available for subject {}'.format(rd3_subject['identifier'])) 
            # Variant information
            if phenopacket_element == 'variant': print('WARNING: Variant information available for subject {}'.format(rd3_subject['identifier'])) #raise Exception('Variant information available for subject {}'.format(rd3_subject['identifier'])) 
            # MetaData information
            # if phenopacket_element == 'metaData':
            # rd3_subject['organisation'] and rd3_subject['ERN'], but these ones are already filled in RD3       
    # Check if all necessary columns exist
    for attribute in subject_attributes :
        if attribute not in rd3_subject:
            raise Exception('Missing attribute {0} for subject {1}'.format(attribute, os.path.splitext(file)[0]))
        if len(rd3_subject[attribute])==0: print('Attribute {} of subject {}'.format(attribute, rd3_subject['identifier']), 'has no values')
    # Check if all phenotype, hasNotPhenotype and ageOfOnset HPO codes exist
    # Iterate through a copy ([:]) of the list otherwise you miss elements if ones are removed
    for code in rd3_subject['phenotype'][:] :
        if code not in hpo_codes:
            n_removed_hpo += 1
            hpo_error_file.write('%s %s %s %s %s\n' % ('HPO code', code, 'of subject', rd3_subject['identifier'], 'does not exist in RD3'))
            rd3_subject['phenotype'].remove(code)
    for code in rd3_subject['hasNotPhenotype'][:] :
        if code not in hpo_codes:
            n_removed_hpo += 1
            hpo_error_file.write('%s %s %s %s %s\n' % ('HPO code', code, 'of subject', rd3_subject['identifier'], 'does not exist in RD3'))
            rd3_subject['hasNotPhenotype'].remove(code)
    for code in rd3_subjectInfo['ageOfOnset'][:] :
        if code not in hpo_codes:
            n_removed_hpo += 1
            hpo_error_file.write('%s %s %s %s %s\n' % ('HPO code age of onset', code, 'of subject', rd3_subject['identifier'], 'does not exist in RD3'))
            rd3_subjectInfo['ageOfOnset'].remove(code)
    # Check if all disease codes exist
    for code in rd3_subject['disease'][:] :
        if code not in disease_codes :
            n_removed_diseases += 1
            disease_error_file.write('%s %s %s %s %s\n' % ('Disease code', code, 'of subject', rd3_subject['identifier'], 'does not exist in RD3'))
            rd3_subject['disease'].remove(code)   
    # Remove list brackets
    rd3_subjectInfo['ageOfOnset']=','.join(rd3_subjectInfo['ageOfOnset'])
    rd3_subject['disease']=','.join(rd3_subject['disease'])
    rd3_subject['hasNotPhenotype']=','.join(rd3_subject['hasNotPhenotype'])
    rd3_subject['phenotype']=','.join(rd3_subject['phenotype'])
    # Write the content to the subject and subject_info file      
    rd3_subject_csv.writerow(rd3_subject)
    rd3_subjectInfo_csv.writerow(rd3_subjectInfo)
    # Write the dictionary to the overall rd3_subject and rd3_subjectInfo list
    rd3_subject_All.append({"identifier": ''.join(rd3_subject['identifier']), "phenotype": rd3_subject['phenotype']})
    #rd3_subject_All.append(rd3_subject)
    rd3_subjectInfo_All.append(rd3_subjectInfo)
    phenopacket_file.stdout.close()
    print('Throughput time of the file is', (datetime.datetime.now()-start_file))

#print(rd3_subject_All)
#print(rd3_subjectInfo_All)


### Use the Molgenis Data API to push the data into the RD3 database
##
#### RD3_SUBJECT ##
##response = requests.put('https://molgenis124.gcc.rug.nl/api/' + "v2/" + quote_plus('rd3_subject'),
##                        headers=rd3_session._get_token_header_with_content_type(),
##                        data=json.dumps({'entities': rd3_subject_All}))
##if response.status_code == '200':
##    print('rd3_subject successfully updated!')
##else:    
##    print('Updating rd3_subject failed')
##    print(json.loads(response.content.decode("utf-8"))['errors'][0]['message'])
##
##    
#### RD3_SUBJECTINFO ##
##response = requests.put('https://molgenis124.gcc.rug.nl/api/' + "v2/" + quote_plus('rd3_subjectinfo'),
##                        headers=rd3_session._get_token_header_with_content_type(),
##                        data=json.dumps({'entities': rd3_subjectInfo_All}))
##if response.status_code == '200':
##    print('rd3_subject successfully updated!')
##else:    
##    print('Updating rd3_subject failed')
##    print(json.loads(response.content.decode("utf-8"))['errors'][0]['message'])    

    
print('\nNumber of files that are processed', n_files_processed)
print('Number of HPO codes that are removed', n_removed_hpo)
print('Number of disease codes that are removed', n_removed_diseases)
print('Processing files has finished at', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print('Total throughput time is', (datetime.datetime.now() - start_script))  

disease_error_file.close()
hpo_error_file.close()
rd3_session.logout()
rd3_subject_file.close()
rd3_subjectInfo_file.close()
