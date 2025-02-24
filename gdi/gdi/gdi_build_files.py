"""Collate files and extract information
FILE: gdi_build_files.py
AUTHOR: David Ruvolo
CREATED: 2023-04-21
MODIFIED: 2024-02-20
PURPOSE: compile file metadata dataset
STATUS: stable
PACKAGES: **see below**
COMMENTS: This script is designed to run on the cluster. Make changes and push to the client
"""

import sys
import re
import json
from os import listdir, path, stat
import pandas as pd


ENTRY_DIR = '/groups/umcg-gdi/prm03/rawdata/ngs/EGAD00001008392/EGAD00001008392'
OUTPUT_DIR = '/home/umcg-druvolo/data/gdi'

sys.setrecursionlimit(100000)


def list_files(_dir: str = None):
    """List files recursively
    :param _dir: entry point to begin looking for files
    :param _dir: string

    :returns: all files at a given path
    :rtype: recordset
    """
    dir_files = []
    dir_contents = listdir(_dir)
    for item in dir_contents:
        item_path = _dir + '/' + item
        if path.isdir(item_path):
            print('Processing files subdirectory', item_path)
            subdir_contents = list_files(item_path)
            dir_files = dir_files + subdir_contents
        if path.isfile(item_path):
            file = {
                'inode': stat(item_path, follow_symlinks=False).st_ino,
                'name': path.basename(item),
                'path': item_path,
                'extension': path.splitext(item)[1],
                'size': path.getsize(item_path)
            }
            dir_files.append(file)
    return dir_files


def read_ped(file: str = None, name: str = None):
    """Read and extract the contents of a PED file

    :param file: the location of a file
    :type file: string

    :param name: the name of the file
    :param name: string

    :return: object containg the extract pedigree information
    :type: object
    """
    with open(file, mode="r", encoding="UTF-8") as f:
        ped_contents = f.readlines()
        for line in ped_contents:
            values = line.replace('\n', '').strip().split(' ')
            data = {
                'file': name,
                'family_id': values[0],
                'subject_id': values[1],
                'maternal_id': values[2],
                'paternal_id': values[3],
                'sex': values[4],
                'affected_status': values[5]
            }
    f.close()
    return data


def read_phenopacket(file: str = None, name: str = None):
    """Read and extract the contents of a phenopacket file

    :param file: the location of a file
    :param file: string

    :param name: the name of the file
    :param name: string

    :returns: data stored in the phenopacket file
    :rtype: json object
    """
    with open(file, mode='r', encoding='UTF-8') as f:
        content = f.read()
        data = json.loads(content)
        data['name'] = name
        f.close()
    return data


def read_checksum(file: str = None):
    """Read and extract the contents of a checksum file

    :param file: the location of a file
    :param file: string

    :return checksum stored in a file
    :rtype: string
    """
    with open(file, mode='r', encoding='UTF-8') as f:
        md5 = f.readline()
    f.close()
    return md5


if __name__ == "__main__":

    print('Compiling a list of files at entry....')
    raw_files = list_files(_dir=ENTRY_DIR)

    # ///////////////////////////////////////

    # process checksum files
    print('Processing checksum files.....')

    checksum_files = [
        file for file in raw_files if re.search(r'(md5)$', file['name'])
    ]

    # For md5 files, read and extract the checksum
    for row in checksum_files:
        row['md5'] = read_checksum(file=row['path'])

    checksumDF = pd.DataFrame(checksum_files)
    checksumDF.to_csv(f'{OUTPUT_DIR}/gdi_checksums.csv', index=False)

    # ///////////////////////////////////////

    # subset to files of interest and extract relevant metadata
    print('Processing files of interest....')
    files = [
        file for file in raw_files
        if re.search(r'(json|ped|fastq.gz|vcf.gz.cip)$', file['name'])
    ]

    ped = []
    phenopacket = []

    print('Extracting contents from ped and phenopacket files....')
    for row in files:
        if row['extension'] == '.ped':
            ped_data = read_ped(file=row['path'], name=row['name'])
            ped.append(ped_data)

        if row['extension'] == '.json':
            json_data = read_phenopacket(file=row['path'], name=row['name'])
            phenopacket.append(json_data)

    # ///////////////////////////////////////

    # save datasets
    print('Saving datasets....')
    ped_df = pd.DataFrame(ped)
    phenopacket_df = pd.DataFrame(phenopacket)
    files_dt = pd.DataFrame(files)

    ped_df.to_csv(
        f"{OUTPUT_DIR}/gdi_ped.csv",
        index=False
    )

    phenopacket_df.to_csv(
        f"{OUTPUT_DIR}/gdi_phenopacket.csv",
        index=False
    )

    files_dt.to_csv(f"{OUTPUT_DIR}/gdi_files.csv", index=False)
