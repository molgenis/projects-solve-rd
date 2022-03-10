#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3tools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-17
#' MODIFIED: 2021-09-24
#' PURPOSE: collection of methods used across scripts
#' STATUS: working / ongoing
#' PACKAGES: *see imports below*
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
from datetime import datetime
import numpy as np
import subprocess
# import mimetypes
import requests
import json
import pytz
import yaml
import os
import re


def status_msg(*args):
    """Status Message
    Print a log-style message, e.g., "[16:50:12.245] Hello world!"

    @param *args one or more strings containing a message to print
    @return string
    """
    message = ' '.join(map(str, args))
    time = datetime.now(tz=pytz.timezone('Europe/Amsterdam')).strftime('%H:%M:%S.%f')[:-3]
    print(f'[{time}] {message}')

class Molgenis(molgenis.Session):
    def __init__(self, *args, **kwargs):
        super(Molgenis, self).__init__(*args, **kwargs)
        self.__getApiUrl__()
    
    def __getApiUrl__(self):
        """Find API endpoint regardless of version"""
        props = list(self.__dict__.keys())
        if '_url' in props:
            self._apiUrl = self._url
        if '_api_url' in props:
            self._apiUrl = self._api_url
    
    def _checkResponseStatus(self, response, label):
        if (response.status_code // 100) != 2:
            err = response.json().get('errors')[0].get('message')
            status_msg(f'Failed to import data into {label} ({response.status_code}): {err}')
        else:
            status_msg(f'Imported data into {label}')
    
    def _POST(self, url: str = None, data: list = None, label: str=None):
        try:
            response = self._session.post(
                url = url,
                headers = self._get_token_header_with_content_type(),
                data = json.dumps({'entities': data})
            )
            
            self._checkResponseStatus(response, label)
            response.raise_for_status()
            
        except requests.exceptions.HTTPError as e:
            raise SystemError(e)
            
    def _PUT(self, url: str=None, data: list=None, label: str=None):
        try:
            response = self._session.put(
                url = url,
                headers = self._get_token_header_with_content_type(),
                data = json.dumps({'entities': data})
            )
            
            self._checkResponseStatus(response, label)
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            raise SystemError(e)
            
    
    def importData(self, entity: str, data: list):
        """Import Data
        Import data into a table. The data must be a list of dictionaries that
        contains the 'idAttribute' and one or more attributes that you wish
        to import.
        
        @param entity (str) : name of the entity to import data into
        @param data (list) : data to import (a list of dictionaries)
        
        @return a status message
        """
        url = '{}v2/{}'.format(self._apiUrl, entity)
        # single push
        if len(data) < 1000:
            self._POST(url=url, data=data, label=str(entity))
            
        # batch push
        if len(data) >= 1000:    
            for d in range(0, len(data), 1000):
                self._POST(
                    url = url,
                    data = data[d:d+1000],
                    label = '{} (batch {})'.format(str(entity), str(d))
                )
    
    
    def updateRows(self, entity: str, data: list):
        """Update Rows
        Update rows in a table. The data must be a list of dictionaries that
        contains the 'idAttribute' and must contain values for all attributes
        in addition to the one that you wish to update. This is ideal for
        updating rows. To update an attribute, use `updateColumn`.
        
        @param entity (str) : name of the entity to import data into
        @param data (list) : data to import (list of dictionaries)
        
        @return a status message
        """
        url = '{}v2/{}'.format(self._apiUrl, entity)
        # single push
        if len(data) < 1000:
            self._PUT(url=url, data=data, label=str(entity))
            
        # batch push
        if len(data) >= 1000:    
            for d in range(0, len(data), 1000):
                self._PUT(
                    url = url,
                    data = data[d:d+1000],
                    label = '{} (batch {})'.format(str(entity), str(d))
                )

    def updateColumn(self, entity: str, attr: str, data: list):
        """Update Column
        Update values of an single column in a table. The data must be a list of
        dictionaries that contain the `idAttribute` and the value of the
        attribute that you wish to update. As opposed to the `updateRows`, you
        do not need to supply values for all columns.
        
        @param entity (str) : name of the entity to import data into
        @param attr (str) : name of the attribute to update
        @param data (list) : data to import (list of dictionaries)
        
        @retrun status message
        """
        url = '{}v2/{}/{}'.format(self._apiUrl, str(entity), str(attr))
        
        # single push
        if len(data) < 1000:
            self._PUT(url=url, data=data, label=f'{entity}/{attr}')
        
        # batch push
        if len(data) >= 1000:
            for d in range(0, len(data), 1000):
                self._PUT(
                    url = url,
                    data = data[d:d+1000],
                    label = '{}/{} (batch {})'.format(
                        str(entity),
                        str(attr),
                        str(d)
                    )
                )



def recodeValue(
    mappings: None,
    value: str=None,
    label:str=None,
    warn=True
):
    """Recode value
    Recode values using new mappings. It is recommended to define all
    mappings using lowercase letters and the the input for value should
    also be lowered.
    
    @param mappings a datatable object containing where each key
        corresponds to a new value
    @param value string containing a value to recode
    @param label string that indicates the mapping type for error messages
    @param warn If True (default), a message will be displayed when a value
        cannot be mapped
    
    @return string or NoneType
    """
    try:
        return mappings[value]
    except KeyError:
        if bool(value) and warn:
            status_msg('Error in {} recoding: "{}" not found'.format(label, value))
        return None
    except AttributeError:
        if bool(value) and warn:
            status_msg('Error in {} recoding: "{}" not found'.format(label, value))
        return None
        

def to_keypairs(data, keyAttr='sourceValue', valueAttr='newValue'):
    """To Key pairs
    Convert a list of dictionaries into a key-value dictionary. This method
    is useful for creating objects for mapping tables.
    
    @param data list of dictionaries that you wish to convert
    @param keyAttr attribute that will be key
    @param valueAttr attribute that contains the value
    
    @return a dictionary
    """
    maps = {}
    for d in data:
        maps[d[keyAttr]] = d.get(valueAttr)
    return maps


def to_records(data):
    """Datatable object to records
    @param data : datatable object
    @return list of dictionaries
    """
    return data.to_pandas().replace({np.nan: None}).to_dict('records')


# add_forward_slash
def __add__forward__slash(path: str = None):
    """Add Forward Slash
    
    Adds forward slash to the end of a path if missing
    
    @param path a string containing a path to a given location (file, url, etc.)
    
    @returns a string
    """
    return path + '/' if path[len(path)-1] != '/' else path

# build paths
def build_rd3_paths(freeze, patch, baseFilePath):
    """Build all RD3 Entity IDs and Paths
    @param freeze       (str) : name of the freeze (i.e., "freeze1")
    @param patch        (str) : name of the patch (i.e., "patch1")
    @param baseFilePath (str) : base file path of the cluster
    @return dict of entity IDs and file paths
    """
    rd3EntityBase = f'rd3_{freeze}'
    clusterBasePath = f'{baseFilePath}/{freeze}-{patch}'
    
    return {
        'rd3_subjects': f'{rd3EntityBase}_subject',
        'rd3_subjectinfo': f'{rd3EntityBase}_subjectinfo',
        'rd3_sample': f'{rd3EntityBase}_sample',
        'rd3_labInfo': f'{rd3EntityBase}_labInfo',
        'rd3_file': f'{rd3EntityBase}_file',
        'cluster_ped': f'{clusterBasePath}/ped/',
        'cluster_phenopacket': f'{clusterBasePath}/phenopacket/'
    }

# cluster_list_files
def cluster_list_files(path: str = None, filter: str = None):
    """List Cluster Files

    Return a list of dictionaries containing available files
    
    @param path location to directory
    @param filter a string containing a file type used to filter results
           (can also be a pattern)
    
    @return a list of dictionaries
    """
    available_files = subprocess.Popen(
        ['ssh', 'corridor+fender', 'ls', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr= subprocess.PIPE,
        universal_newlines=True
    )
    path = __add__forward__slash(path)
    data = []
    for f in available_files.stdout:
        data.append({
            'filename': f.strip(),
            'filepath': path + f.strip()
        })
    available_files.kill()
    if filter:
        filtered = []
        for d in data:
            q = re.compile(filter)
            if re.search(q, d['filename']):
                filtered.append(d)
        status_msg('Found {} files'.format(len(filtered)))
        return filtered
    else:
        status_msg('Found {} files'.format(len(data)))
        return data



def cluster_read_file(path: str = None):
    """Read file from the cluster
    
    Read contents of a file on the cluster; ideally for ped files
    
    @param path location of a file; use output from cluster_list_files
    
    @return a list
    """
    proc = subprocess.Popen(
        ['ssh', 'corridor+fender', 'cat', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    proc.wait()
    data= []
    for line in proc.stdout:
        data.append(line.strip())
    proc.kill()
    return data



def cluster_read_json(path: str = None):
    """Read Json file from the cluster
    
    Read a json file located on the cluster (ideal for phenopacket files)
    
    @param path location of the file; use output from cluster_list_files
    
    @return a list
    """
    proc = subprocess.Popen(
        ['ssh', 'corridor+fender', 'cat', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    try:
        raw = proc.communicate(timeout=15)
        data = json.loads(raw[0])
        proc.kill()
        return data
    except subprocess.TimeoutExpired:
        status_msg('Error: unable to fetch file {}'.format(str(os.path.basename(path))))
        proc.kill()
        return ''




def cluster_run_checksum(path: str = None):
    """Run checksum on a file
    
    Run md5 on a file and return the value
    
    @param path location of the file run the checksum
    
    @return a string
    """
    proc = subprocess.Popen(
        ['ssh', 'corridor+fender', 'md5sum', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    value = proc.stdout.read().split()[0]
    proc.kill()
    return value



def distinct_dict(data: list = None, key: str = None):
    """Distinct Dictionaries
    
    In a list of dictionnaires, return distinct dictionaries by a given key
    
    @param data a list containing one or more dictionaries 
    @param key one or more keys to filter by
    
    @return a list of dictionaries
    """
    if key is None:
        key = lambda x: x
    seen = set()
    for d in data:
        k = key(d)
        if k in seen:
            continue
        yield d
        seen.add(k)
    return seen




def extract_nested_attr(data: list = None, attr: str = None):
    """Extract nested attribute
    
    Extract attribute from nested dictionary
    
    @param data input dataset a list of dictionaries
    @param attr value to extract
    
    @return a string of values
    """
    value = None
    if len(data) == 1:
        value = data[0].get(attr)
    if len(data) > 1:
        joined_att = []
        for d in data:
            joined_att.append(d.get(attr))
        value = ','.join(map(str, joined_att))
    return value




def flatten_attr(data: list = None, attr: str = None, distinct: bool = False):
    """Flatten attribute
    
    In a list of dictionaries, pull values by key
    
    @param data list of dictionaries
    @param name of attribute to flatten
    @param distinct if TRUE, return unique cases only
    
    @return a list of values
    """
    out = []
    for d in data:
        tmp_attr = d.get(attr)
        out.append(tmp_attr)
    if distinct:
        return list(set(out))
    else:
        return out



def find_dict(data: list = None, attr: str = None, value: str = None):
    """Filter list of dictionaries
    
    @param data object to search
    @param attr variable find match
    @param value value to filter for
    
    @return a list of a dictionaries

    """
    return list(filter(lambda d: d[attr] in value, data))




def load_yaml_config(path: str = None):
    """Load YAML file
    
    Load yaml file
    
    @param path path to yaml file
    
    @return list
    """
    p = os.path.abspath(path)
    with open(p, 'r') as f:
        d = yaml.safe_load(f)
        f.close()
    return d




def load_json(path: str = None):
    """Load JSON
    
    Read and extract the contents of a JSON file
    
    @param path: location of the file
    """
    with open(path, 'r') as file:
        data = json.load(file)
        file.close()
    return data



def lookups_find_new(lookup, lookup_attr, new):
    """Find new lookup values
    
    Given a list of new values and old values, determine if there are any
    new values.
    
    @param lookup_attr the attribute to look into
    @param lookup RD3 lookup table
    @param new a list of unique values
    
    @return a list of dictionaries of 
    """
    refs = flatten_attr(lookup, lookup_attr)
    out = []
    for n in new:
        if (n in refs) == False:
            out.append({'id': n, 'label': n})
    return out



def lookups_prep_new(data, id_name='identifier', label_name='label', clean = False):
    """Prepare Reference Types for Import
    
    Prepare data for import into Molgenis
    
    @param data list containing one or more dictionaries of new references
    @param id_name label to apply to the ID variable (id => identifier)
    @param label_name label to map to 'label' key (i.e., name => label)
    @param clean if TRUE, the ID attribute will be cleaned (spaces => '-'; text => lowered)
    
    @return a list of dictionaries
    """
    out = []
    for d in data:
        new = {}
        value  = d.get('id')
        label = d.get('label')
        if clean:
            value = '-'.join(d.get('id').split()).lower()
            label = value
        new[id_name] = value
        new[label_name] = label
        out.append(new)
    return out




def missing_count(data):
    """Calculate Missing Count
    
    In a list of dictionaries, calculate how many keys are missing by dictionary
    
    @param data list of dictionaries
    
    @return dictionary containing the count of missing values by key
    """
    keys = {k: 0 for k in data[0].keys()}
    for d in data:
        for i in d:
            if d[i] == None:
                keys[i] += 1
    return keys




def __ped__recode__affectedstatus(value):
    """Recode Affected Status
    
    Recode PED affected status into RD3 terminology
    
    @param value string containing a PED affected_status code (0,1,2,-9,..)
    
    @return bool
    """
    if value in ['-9', '0']: return None
    elif value == '1': return False
    elif value == '2': return True
    else: status_msg('ERROR: unable to recode {}'.format(value))


def __ped__recode__sex(value):
    """Recode Sex Values
    
    Recode PED coding into RD3 terminology
    
    @param value a string containing PED sex code (0,1,...)

    @return a string
    """
    if value == '1': return 'M'
    elif value == '2': return 'F'
    elif value.lower() == 'other': return "U"
    else: status_msg('ERROR: unable to recode {}'.format(value))



def __ped__extract__line(line):
    """Extract a single line from a PED file
    
    @param line a dict containing a single line of PED file
        To be used inside ped_process_file
    
    @return a dictionary
    """
    return {
        'id': line[1],
        'subjectID': line[1],
        'fid': line[0],
        'mid': line[3],
        'pid': line[2],
        'sex1': __ped__recode__sex(value=line[4]),
        'clinical_status': __ped__recode__affectedstatus(value=line[5]),
        'upload': True
    }
    


def __ped__validate__line(data, ids):
    """Validate PED Line Data
    
    Validate a single line of data extracted from a PED file
    
    @param data a dict containing a single line of data from a PED file.
        This is the output from `__ped__extract__line`
    @param ids a list of IDs to compare to
    @param a dictionary containing validated data
    
    @return a dictionary
    """
    line = data
    if ('FAM' in line['id']) or (line['id'] not in ids):
        status_msg(
            'ID {} from family {} does not exist'
            .format(line['id'], line['fid'])
        )
        line['upload'] = False
    if ('FAM' in line['mid']) or (line['mid'] == '0') or (line['mid'] not in ids) :
            line['error_mid'] = line['mid']
            status_msg(
                'removed mid {} as it starts with `FAM`, is `0`, or it does not exist'
                .format(line['mid'])
            )
            line['mid']=None
    if ('FAM' in line['pid']) or (line['pid'] == '0') or (line['pid'] not in ids):
            line['error_pid'] = line['pid']
            status_msg(
                'removed pid {} as it starts with `FAM`, is `0`, or it does not exist'
                .format(line['pid'])
            )
            line['pid']=None
    return line



def ped_extract_contents(contents, ids, filename):
    """Extract contents from a PED file
    
    Extract the contents of a pedigree file
    
    @param contents output from `cluster_read_file`
    @param ids a list of reference IDs to check against
    @param filename string containing the name of the file (for validation)
    
    @return list of dictionaries
    """
    data = []
    for line in contents:
        d = line.split()
        if len(d) == 6:
            raw_line_data = __ped__extract__line(line = d)
            proc_line_data = __ped__validate__line(
                data = raw_line_data,
                ids = ids
            )
            data.append(proc_line_data)
        else:
            status_msg(
                'Line in {} does not have 6 columns. Has {}'
                .format(filename, len(d))
            )
    return data



def __pheno__recode__sex(value):
    """Recode Phenopacket set value

    Record phenopackets sex values into RD3 terminology
    
    # @param value string containing a sex
    
    # @return string
    """
    if value.lower() == 'female':
        return 'F'
    elif value.lower() == 'male':
        return 'M'
    elif value.lower() == 'unknown_sex':
        return 'U'
    else:
        return value



def __pheno__recode__date(value):
    """Format Date
    
    Format date as yyyy-mm-dd
    
    @param value string containing date to recode
    
    @return string
    
    """
    if value != '':
        return re.sub(r'(T00:00:00Z)', '', value).split('-')[0]
    else:
        return value



def __pheno__unpack__phenotypicfeatures(phenotypicFeatures):
    """Upackage PhenotypicFeatures

    Extract `phenotypicFeatures` from and separate into observed and unobserved phenotypes

    @param phenotypicFeatures list of dictionaries from data['phenopacket']['phenotypicFeatures']
    
    @return a dictionary with two lists of HPO IDs

    """
    phenotype = []
    hasNotPhenotype = []
    for pheno in phenotypicFeatures:
        if pheno:
            hpo_id = re.sub(r'^(HP:)', 'HP_', pheno['type']['id'])
            if not (hpo_id in phenotype) and not (hpo_id in hasNotPhenotype):
                if 'negated' in pheno:
                    if pheno['negated']:
                        hasNotPhenotype.append(hpo_id)
                    if not pheno['negated']:
                        phenotype.append(hpo_id)
                else:
                    phenotype.append(hpo_id)
    return {'phenotype': phenotype, 'hasNotPhenotype': hasNotPhenotype}


def __pheno__unpack__diseases(data):
    """Extract Phenopacket Disease Codes
    
    Extract disease IDs and Onset codes

    @param data list of dictionaries from data['phenopacket']['diseases]
    
    @return a dictionary of disease IDs and onset code(s)
    """
    dx = []
    onset = []
    for d in data:
        if 'term' in d:
            if 'id' in d['term']:
                code = d['term']['id']
                if re.search(r'^((Orphanet:)|(ORDO:))', code):
                    code = re.sub(r'^((Orphanet:)|(ORDO:))', 'ORDO_', code)
                if re.search(r'^((OMIM:)|(MIM:))', code):
                    code = re.sub(r'^((OMIM:)|(MIM:))', 'MIM_', code)
                dx.append(code)
        if 'classOfOnset' in d:
            if 'id' in d['classOfOnset']:
                code = d['classOfOnset']['id']
                code = re.sub(r'^(HP:)', 'HP_', code)
                onset.append(code)
    return {'dx': dx, 'onset': onset}



def __pheno__recode__diseases(codes, collapse = False):
    """Recode Phenotypic Disease Codes
    
    If a disease code has changed, update it
    
    @param codes list of disease codes
    @param if True, codes will be collapsed into a comma separated string
    @param a list of values or string
    
    @return a list of values
    """
    ids_to_recode = {
        'MIM_159000': {'old':'MIM_159000','new':'MIM_609200'},
        'MIM_159001': {'old':'MIM_159001','new':'MIM_181350'},
        'MIM_607569': {'old':'MIM_607569','new':'MIM_603689'},
        'ORDO_856': {'old': 'ORDO_856', 'new': ''}
    }
    out = []
    for code in codes:
        if code in ids_to_recode:
            out.append(ids_to_recode[code]['new'])
        else:
            out.append(code)
    if collapse:
        return ','.join(out)
    else:
        return out


def pheno_extract_contents(contents, filename, recode_dx_codes=False):
    """Extract Phenopacket File Contents
    
    Extract the contents of phenopacket file into dictionary
    
    @param contents returned object from `cluster_read_json`
    @param filename name of the phenopackets file
    @param recode_dx_codes if True, invalid diagnostic codes will be recoded
    
    @return a dictionary
    """
    pheno = {
        'id': contents['phenopacket']['id'],
        'dateofBirth': None,
        'sex1': None,
        'phenotype': [],
        'hasNotPhenotype': [],
        'disease': [],
        'phenopacketsID': filename
    }
    # process `subject` metdata:
    if 'subject' in contents['phenopacket']:
        if 'dateOfBirth' in contents['phenopacket']['subject']:
            pheno['dateofBirth'] = __pheno__recode__date(
                value  = contents['phenopacket']['subject']['dateOfBirth']
            )
        if 'sex' in contents['phenopacket']['subject']:
            pheno['sex1'] = __pheno__recode__sex(
                value = contents['phenopacket']['subject']['sex']
            )
    # process `phenotypicFeatures`
    if 'phenotypicFeatures' in contents['phenopacket']:
        phenotypic_features = __pheno__unpack__phenotypicfeatures(
            phenotypicFeatures = contents['phenopacket']['phenotypicFeatures']
        )
        pheno['phenotype'] = phenotypic_features['phenotype']
        pheno['hasNotPhenotype'] = phenotypic_features['hasNotPhenotype']
    # process `diseases`: append if results exist
    if 'diseases' in contents['phenopacket']:
        dx_data =  __pheno__unpack__diseases(contents['phenopacket']['diseases'])
        if len(dx_data['dx']) > 0:
            dx_codes = dx_data['dx']
            if recode_dx_codes:
                dx_codes = __pheno__recode__diseases(dx_codes)
            pheno['disease'] = ','.join(dx_codes)
        if len(dx_data['onset']) > 0:
            pheno['ageOfOnset'] = ','.join(dx_data['onset'])
    return pheno



def select_keys(data, keys):
    """Select Keys

    Reduce list of dictionaries to named keys
    
    @param data list of dictionaries to select
    @param keys an array of values
    
    @return a list of dictionaries
    """
    return list(map(lambda x: {k: v for k, v in x.items() if k in keys}, data))



# def timestamp():
#     """Timestamp
#     Print a timestamp as yyyy-mm-dd HH:MM:SS:ms
#     """
#     return datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]

def timestamp(tz='Europe/Amsterdam', timeFormat='%Y-%m-%d'):
    """Timestamp
    Return a timestamp specific to user's tz and desired time format
    @param tz string containing a timezone
    @param timeFormat string indicating how to format the output
    @return datetime object
    """
    return datetime.now(tz=pytz.timezone(tz)).strftime(timeFormat)

def write_csv(path, data):
    """Write CSV
    
    Save object to csv file
    
    @param path location of output file
    @param data dat ato write to file
    
    """
    import csv
    headers = list(data[0].keys())
    with open(path, 'w') as file:
        writer = csv.DictWriter(file, fieldnames = headers)
        writer.writeheader()
        writer.writerows(data)
    file.close()