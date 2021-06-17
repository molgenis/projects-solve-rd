#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_tools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-17
#' MODIFIED: 2021-06-17
#' PURPOSE: collection of methods used across scripts
#' STATUS: in.progress
#' PACKAGES: *see below*
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# declare imports
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
        for d in range(0, len(data), 1000):
            response = self._session.post(
                url=self._api_url + 'v2/' + entity,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': data[d:d+1000]})
            )
            if response.status_code == 201:
                print("Imported batch " + str(d) +
                      " successfully (" + str(response.status_code) + ")")
            else:
                print("Failed to import batch " + str(d) +
                      " (" + str(response.status_code) + ")")
    # @title Batch Update Entity Attribute
    # @name batch_update_one_attr
    # @description import data for an attribute in groups of 1000
    # @param self required class param
    # @param entity ID of the target enttiy written as `package_entity`
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