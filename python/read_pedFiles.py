"""
    Revision history:
    20200319 Dieuwke Roelofs-Prins: Created
    20200511 Dieuwke Roelofs-Prins: Added checksum check

    Description:
    With this Python script Ped file information can be pushed into the RD3 database:
    - rd3_subject

    Ped file information that is defined is:
    - Family Id (fid)
    - Mother Id (mid)
    - Father Id (fid)

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

# set token
os.environ['molgenisToken'] = ''

# Define variables
family_folders=[]
n_files_processed=0
n_expected_columns=6
rd3_sex_codes={'1' : 'M', '2': 'F', 'OTHER': 'U'}
rd3_phenotype_codes={'-9': 'N/A', '0': 'N/A', '1' : 'No', '2': 'Yes'}
rd3_subject_All=[]
sep=','
rd3_session = molgenis_data_api.Session('', token=os.environ['molgenisToken'])
subject_attributes=['identifier', 'sex1', 'fid', 'mid', 'pid', 'clinical_status', 'family_folder', 'upload', 'error_mid', 'error_pid']
subject_IDs=[]

# Directory and file definitions
# If the script runs on the same system as where the files are located
output_folder=''
#ped_files=os.scandir(phenopacket_folder)

# Files are located remotely
ped_folder=''
ped_folders= subprocess.Popen(['ssh', 'corridor+fender', 'ls', ped_folder], stdout=subprocess.PIPE, universal_newlines=True)

#ped_folders.wait()

# Output files, in the end not used as data is pushed into RD3 database using Data API
rd3_Ped_file=open(output_folder+'rd3_ped.csv', 'w')

rd3_ped_csv = csv.DictWriter(rd3_Ped_file, fieldnames=subject_attributes, delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
rd3_ped_csv.writeheader()

# Loop through the list with phenopacket files that are on Gearshift
print ("Start processing ped files at", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
start_script = datetime.datetime.now()

i=0
print('Store all the family folder names in a temporary list otherwise connection closed by remote host can occur in case of many files')
for family_folder in ped_folders.stdout:
    family_folders.append(family_folder.strip())    

ped_folders.stdout.close()

print('Number of family ped folders', len(family_folders))

######## Get all subjects IDs to check if mother and father IDs exist
rd3_subjectIDs=rd3_session.get('rd3_freeze2_subject', batch_size=10000)

for subjectID in rd3_subjectIDs:
    subject_IDs.append(subjectID['id'])

####### Get the checksums of the files to check if files on Gearshift are complete ####
rd3_checksums=rd3_session.get('rd3_freeze2_file', batch_size=10000, attributes='name,md5', q='typeFile=="ped"')

i=0
EGA_checksum={}
print('Number of Ped files in RD3 is', len(rd3_checksums))
for checksum in rd3_checksums:
    #print(checksum['name'].split('/')[-1].replace('.cip',''))
    EGA_checksum[checksum['name'].split('/')[-1].replace('.cip','')]=checksum['md5']

for family_folder in family_folders:
    print('\n',ped_folder+'/'+family_folder+'/*.ped')
    # Check if the stop-file exists, this is to quit the process in a normal way
    # in stead of killing the process
    if os.path.isfile(output_folder+'stop_process.txt'):
        print ("\nSTOP: Stop file exists, so stop processing data")
        rd3_session.logout
        exit()    
    #    i += 1
    #    if i==3: break
    #   Get all the content of all files in the family folder
    # ped_file = subprocess.Popen(['ssh', 'corridor+fender', 'grep . ', ped_folder+'/'+family_folder+'/*.ped*'],
    #                    stdout=subprocess.PIPE, universal_newlines=True)
    ped_file = subprocess.Popen(['ssh', 'corridor+fender', 'grep . ', ped_folder+'/'+family_folder],
                       stdout=subprocess.PIPE, universal_newlines=True)
    ped_file.wait()
    folder_content={}
    for output in ped_file.stdout:
        # output = output.strip().replace(ped_folder+'/'+family_folder,'')
        folder_content['family']=output.split(' ')[0]
        folder_content.setdefault(output.split(' ')[0],[])
        folder_content[output.split(' ')[0]].append(output.split(' ')[1])
    ped_file.stdout.close()
    #   Check if the checksum of the *.ped file is OK
    if EGA_checksum[folder_content['family']+'.ped'] != ''.join(folder_content[folder_content['family']+'.ped.md5']):
        print('Checksum {0} of the file {1} is not equal to the EGA checksum {2} => Ped file information not added!'.format(''.join(folder_content[folder_content['family']+'.ped.md5']),folder_content['family']+'.ped',EGA_checksum[folder_content['family']+'.ped']))
        continue   
    else: print('checksum ok', EGA_checksum[folder_content['family']+'.ped'], ''.join(folder_content[folder_content['family']+'.ped.md5']))
    #   So far so good, now store the family information
    start_file=datetime.datetime.now()
    n_files_processed += 1 
    rd3_subject={} # Empty the dictionary
    # Define some of the attributes in the dictionary    
    for line in folder_content[folder_content['family']+'.ped']:
        print(line)
        rd3_subject={} # Empty the dictionary
        # Check whether there are six columns in the file otherwise discard the file
        if len(line.split('\t')) == n_expected_columns:
            rd3_subject['identifier']=line.split('\t')[1]
            rd3_subject['sex1']=line.split('\t')[4]
            try:
                rd3_subject['sex1']=rd3_subject['sex1'].replace(rd3_subject['sex1'], rd3_sex_codes[rd3_subject['sex1']])
            except KeyError as key_error:
                print ('ERROR sex code {} could not be decoded: {}'.format(rd3_subject['sex1'], key_error))
            rd3_subject['fid']=line.split('\t')[0]
            rd3_subject['mid']=line.split('\t')[3]
            rd3_subject['pid']=line.split('\t')[2]
            rd3_subject['clinical_status']=line.split('\t')[5]
            try:
                rd3_subject['clinical_status']=rd3_subject['clinical_status'].replace(rd3_subject['clinical_status'], rd3_phenotype_codes[rd3_subject['clinical_status']])
            except KeyError as key_error:
                print ('ERROR phenotype {} could not be decoded: {}'.format(rd3_subject['clinical_status'], key_error.message))
            rd3_subject['family_folder']=family_folder
            rd3_subject['upload']='Y'
            if ('FAM' in rd3_subject['identifier']) or (rd3_subject['identifier'] not in subject_IDs):
                rd3_subject['upload']='N';
                print('ERROR Subject ID {} in family folder {} starts with FAM or does not exist, don''t upload'.format(rd3_subject['identifier'], family_folder))
            if ('FAM' in rd3_subject['mid']) or (rd3_subject['mid'] == '0') or (rd3_subject['mid'] not in subject_IDs) :
                rd3_subject['error_mid']=rd3_subject['mid']
                print('ERROR Mother ID {} in family folder {} starts with FAM is 0 or is not a valid subject ID, mother ID removed'.format(rd3_subject['mid'], family_folder))
                rd3_subject['mid']=None
            if ('FAM' in rd3_subject['pid']) or (rd3_subject['pid'] == '0') or (rd3_subject['pid'] not in subject_IDs):
                rd3_subject['error_pid']=rd3_subject['pid']
                print('ERROR Father ID {} in family folder {} starts with FAM is 0 or is not a valid subject ID, father ID removed'.format(rd3_subject['pid'], family_folder))
                rd3_subject['pid']=None
            # Write the content to the subject and subject_info file      
            rd3_ped_csv.writerow(rd3_subject)
        else:
            print ('ERROR Ped file in folder {} does not contain six columns but {}'.format(family_folder, len(file_content.split('\t'))))
            break
    print('Throughput time of the file is', (datetime.datetime.now()-start_file))

print('\nNumber of files that are processed', n_files_processed)
print('Processing files has finished at', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print('Total throughput time is', (datetime.datetime.now() - start_script))

rd3_Ped_file.close()
