#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3tools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-17
#' MODIFIED: 2021-06-28
#' PURPOSE: collection of methods used across scripts
#' STATUS: working / ongoing
#' PACKAGES: *see imports below*
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
import subprocess
import mimetypes
import requests
import json
import yaml
import os

from urllib.parse import quote_plus
from datetime import datetime

# @title molgenis
# @description extend molgenis class
class molgenis(molgenis.Session):
    # @title Update Table
    # @name update_table
    # @description batch update a molgenis entity
    # @param self required class param
    # @param data object containing data to import
    # @param entity ID of the target entity written as 'package_entity'
    # @return a response code
    def update_table(self, data, entity):
        if len(data) < 1000:
            response = self._session.post(
                url = self._url + 'v2/' + quote_plus(entity),
                headers = self._get_token_header_with_content_type(),
                data = json.dumps({'entities' : data})
            )
            if response.status_code == 201:
                status_msg(
                    'Successfully imported data (response: {})'
                    .format(response.status_code)
                )
            else:
                status_msg(
                    'Failed to import data (response: {}): \nReason:{}'
                    .format(response.status_code, response.content)
                )
        else:    
            for d in range(0, len(data), 1000):
                response = self._session.post(
                    url=self._url + 'v2/' + entity,
                    headers=self._get_token_header_with_content_type(),
                    data=json.dumps({'entities': data[d:d+1000]})
                )
                if response.status_code == 201:
                    status_msg(
                        'Successfuly imported batch {} (response: {})'
                        .format(d, response.status_code)
                    )
                else:
                    status_msg(
                        'Failed to import data (response: {}): \nReason:{}'
                        .format(response.status_code, response.content)
                    )
    # @title Batch Update Entity Attribute
    # @name batch_update_one_attr
    # @description import data for an attribute in groups of 1000
    # @param self required class param
    # @param entity ID of the target entity written as `package_entity`
    # @param values data to import, a list of dictionaries where each dictionary
    #       is structured with two keys: the ID attribute and the attribute
    #       that you wish to update. E.g. [{'id': 'id123", 'x': 1},...]
    # @return a response code
    def batch_update_one_attr(self, entity, attr, values):
        add = 'No new data'
        for i in range(0, len(values), 1000):
            add = 'Update did tot go OK'
            """Updates one attribute of a given entity with the given values of the given ids"""
            response = self._session.put(
                self._url + "v2/" + quote_plus(entity) + "/" + attr,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': values[i:i+1000]})
            )
            if response.status_code == 200:
                add = 'Update went OK'
            else:
                try:
                    response.raise_for_status()
                except requests.RequestException as ex:
                    self._raise_exception(ex)
                return response
        return add
    # @title Batch Remove Data
    # @name batch_remove
    # @description remove data from an entity using a list of row IDs
    # @param selef required param
    # @param entity ID of the target entity written as `package_entity`
    # @param data a list row IDs (must contain values of the idAttribute)
    # @return a response code
    def batch_remove(self, entity, data):
        if len(data) < 1000:
            self.delete_list(entity = entity, entities = data)
        else:
            for d in range(0, len(data), 1000):
                self.delete_list(
                    entity = entity,
                    entities = data[d+d:1000]
                )
    # @title Upload File
    # @name upload_file
    # @description upload file (pdf, word, etc.) into Molgenis
    # @param self required molgenis param
    # @param name name of the file to use in Molgenis
    # @param path location to the file
    # @return a response code
    def upload_file(self, name, path):
        filepath = os.path.abspath(path)
        url = self._url + 'files/'
        header = {
            'x-molgenis-token': self._token,
            'x-molgenis-filename': name,
            'Content-Length': str(os.path.getsize(filepath)),
            'Content-Type': str(mimetypes.guess_type(filepath)[0])
        }
        with open(filepath,'rb') as f:
            data = f.read()
        f.close()
        response = requests.post(url, headers=header, data=data)
        if response.status_code == 201:
            print(
                'Successfully imported file:\nFile Name: {}\nFile ID: {}'
                .format(
                    response.json()['id'],
                    response.json()['filename']
                )
            )
        else:
            response.raise_for_status()

# @title cluster_list_files
# @description return a list of dictionaries containing available files
# @param path location to directory
# @return a list of dictionaries
def cluster_list_files(path):
    available_files = subprocess.Popen(
        ['ssh', 'corridor+fender', 'ls', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr= subprocess.PIPE,
        universal_newlines=True
    )
    data = []
    for f in available_files.stdout:
        data.append({
            'file_name': f.strip(),
            'file_path': path + f.strip()
        })
    available_files.kill()
    print('Found', len(data), 'files')
    return data

# @title cluster_read_file
# @description read contents of a file on the cluster; ideally for ped files
# @param path location of a file; use output from cluster_list_files
# @return list object
def cluster_read_file(path):
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

# @title cluster_read_json
# @description read contents of json file
# @param path location of the file; use output from cluster_list_files
# @return a list object
def cluster_read_json(path):
    proc = subprocess.Popen(
        ['ssh', 'corridor+fender', 'cat', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    proc.wait()
    try:
        raw = proc.communicate(timeout=15)
        data = json.loads(raw[0])
        proc.kill()
        return data
    except subprocess.TimeoutExpired:
        print('Error: unable to fetch file ' + str(os.path.basename(path)))
        proc.kill()
        return ''


# @title cluster_run_checksum
# @description run md5 on a file and return the value
# @param path location of the file run the checksum
# @return a string containing an md5 value
def cluster_run_checksum(path):
    proc = subprocess.Popen(
        ['ssh', 'corridor+fender', 'md5sum', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    proc.wait()
    value = proc.stdout.read().split()[0]
    proc.kill()
    return value

# @title distinct_dict
# @description get distinct dictionnaires only
# @param data a list containing one or more dictionaries 
# @param key one or more keys to filter by
# @return a list containing distinct dictionaries
def distinct_dict(data, key):
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

# @title Extract Nested Attribute
# @description extract attribute from nested dictionary
# @param data input dataset a list of dictionaries
# @param attr value to extract
# @return a string of values
def extract_nested_attr(data, attr):
    value = None
    if len(data) == 1:
        value = data[0].get(attr)
    if len(data) > 1:
        joined_att = []
        for d in data:
            joined_att.append(d.get(attr))
        value = ','.join(map(str, joined_att))
    return value


# @title flatten attribute
# @description pull values from a specific attribute
# @param data list of dict
# @param name of attribute to flatten
# @param distinct if TRUE, return unique cases only
# @return a list of values
def flatten_attr(data, attr, distinct=False):
    out = []
    for d in data:
        tmp_attr = d.get(attr)
        out.append(tmp_attr)
    if distinct:
        return list(set(out))
    else:
        return out

# @title filter list of dictionaries
# @param data object to search
# @param attr variable find match
# @param value value to filter for
# @return a list of a dictionary
def find_dict(data, attr, value):
    return list(filter(lambda d: d[attr] in value, data))


# @title load_config
# @description load yaml configuration file
# @param path path to yaml configuration file
def load_yaml_config(path):
    p = os.path.abspath(path)
    with open(p, 'r') as f:
        d = yaml.safe_load(f)
        f.close()
    return d

# @title load_json
# @description read json file
# @param path location to the file
def load_json(path):
    with open(path, 'r') as file:
        data = json.load(file)
        file.close()
    return data

# @title Identitfy new lookup values
# @description using a list of new values, determine if there are
#       new values to update
# @param lookup RD3 lookup table
# @param lookup_attr the attribute to look into
# @param new a list of unique values
# @return a list of dictionaries of 
def lookups_find_new(lookup, lookup_attr, new):
    refs = flatten_attr(lookup, lookup_attr)
    out = []
    for n in new:
        if (n in refs) == False:
            out.append({'id': n, 'label': n})
    return out

# @title Prepare Reference Types for Import
# @descrition prepare new reference data for import
# @param data list containing one or more dictionaries of new references
# @param id_name label to apply to the ID variable (id => identifier)
# @param label_name label to map to 'label' key (i.e., name => label)
# @param clean if TRUE, the ID attribute will be cleaned (spaces => '-'; text => lowered)
# @return a list of dictionaries
def lookups_prep_new(data, id_name='identifier', label_name='label', clean = False):
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

# @title Get missing value count by key
# @description get number of missing values by key
# @param data list of dictionaries to evaluate
# @return dictionary of counts by key
def missing_count(data):
    keys = {k: 0 for k in data[0].keys()}
    for d in data:
        for i in d:
            if d[i] == None:
                keys[i] += 1
    return keys

# @title recode_affected_status
# @description recode PED affected status into RD3 terminology
# @param value a string containing a PED affected_status code
# @return a string
def __ped__recode__affectedstatus(value):
    if value in ['-9', '0']: return None
    elif value == '1': return False
    elif value == '2': return True
    else: status_msg('ERROR: unable to recode {}'.format(value))

# @title recode_sex
# @description recode PED coding into RD3 terminology
# @param value a string containing PED sex code
# @return a string
def __ped__recode__sex(value):
    if value == '1': return 'M'
    elif value == '2': return 'F'
    elif value.lower() == 'other': return "U"
    else: status_msg('ERROR: unable to recode {}'.format(value))

# @title Extract contents from a PED file by line
# @name __ped__extract__line
# @description Extract contents of a ped file by line
# @param line a dict containing a single line of PED file
#       To be used inside ped_process_file
def __ped__extract__line(line):
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

# @title Validate PED Line Data
# @name __ped__validate__line
# @description Validate a single line of data extracted from a PED file
# @param data a dict containing a single line of data from a PED file.
#       This is the output from `__ped__extract__line`
# @param ids a list of IDs to compare to
# @param a dictionary containing validated data
def __ped__validate__line(data, ids):
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


# @title Process Pedgree File Contents
# @name ped_extract_contents
# @description Read contents of pedigree file
# @param contents output from `cluster_read_file`
# @param ids a list of reference IDs to check against
# @param filename string containing the name of the file (for validation)
# @return as list of dictionaries containing processed PED data
def ped_extract_contents(contents, ids, filename):
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

# @title select keys
# @describe reduce list of dictionaries to named keys
# @param data list of dictionaries to select
# @param keys an array of values
# @return a list of dictionaries
def select_keys(data, keys):
    return list(map(lambda x: {k: v for k, v in x.items() if k in keys}, data))


# @title timestamp
# @description generate a timestamp in H:M:S.ms format
def timestamp():
    return datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]

# @title Status Message
# @description Prints a message with a timestamp
def status_msg(msg):
    print('[' + timestamp() + '] ' + str(msg))