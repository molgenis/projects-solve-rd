#'////////////////////////////////////////////////////////////////////////////
#' FILE: pedtools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-04
#' MODIFIED: 2022-08-04
#' PURPOSE: PED file tools
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import statusMsg

def _parseFileRow(row: dict = None):
  """Parse Pedigree File Row
  Transform a row from a PED file into RD3 terminology and shape

  @param row dictionary that is a row from a PED file
  @return dictionary
  """
  data = {
    'id': row[1],
    'subjectID': row[1],
    'fid': row[0],
    'mid': row[3],
    'pid': row[2],
    'sex1': row[4],
    'clinical_status': row[5],
    'unknownSexCode': None,
    'unknownClinicalStatus': None,
    'upload': True
  }
  
  sexCodeMappings = {'1': 'M', '2': 'F', 'OTHER': 'U', 'other': 'U'}
  clinicalStatusMappings = {'-9': None, '0': None, '1': False, '2': True}
  
  if data['sex1'] in sexCodeMappings:
    data['sex1'] = sexCodeMappings[data['sex1']]
  else:
    data['unknownSexCode'] = data['sex1']
    data['sex1'] = None
  
  if data['clinical_status'] in clinicalStatusMappings:
    data['clinical_status'] = clinicalStatusMappings[data['clinical_status']]
  else:
    data['unknownClinicalStatus'] = data['clinical_status']
    data['clinical_status'] = None
  
  return data

def _validateFileRow(row: dict = None, ids: list = None):
  """Validate PED Row
  Validate a single line of data extracted from a PED file

  @param data a dict containing a single line of data from a PED file.
      This is the output from `pedtools.parseFileRow`
  @param ids a list of IDs to compare to
  @param a dictionary containing validated data

  @return a dictionary
  """
  line = row
  if ('FAM' in line['id']) or (line['id'] not in ids):
    statusMsg('ID', line['id'],'from family',line['fid'],'does not exist')
    line['error_id'] = line['id']
    line['upload'] = False

  if ('FAM' in line['mid']) or (line['mid'] == '0') or (line['mid'] not in ids):
    statusMsg('removed mid',line['mid'],'as it starts with `FAM`, is `0`, or it does not exist')
    line['error_mid'] = line['mid']
    line['mid'] = None

  if ('FAM' in line['pid']) or (line['pid'] == '0') or (line['pid'] not in ids):
    statusMsg('removed pid',line['pid'],'as it starts with `FAM`, is `0`, or it does not exist')
    line['error_pid'] = line['pid']
    line['pid'] = None
  return line

def parseFileContents(contents, ids, filename):
  """Extract contents from a PED file

  Extract the contents of a pedigree file

  @param contents output from `cluster_read_file`
  @param ids a list of reference IDs to check against
  @param filename string containing the name of the file (for validation)

  @return list of dictionaries
  """
  data = []
  for line in contents:
    row = line.split()
    if len(row) == 6:
      row_data = _parseFileRow(row=row)
      processed_row_data = _validateFileRow(row=row_data, ids=ids)
      data.append(processed_row_data)
    else:
      statusMsg('Line in ', filename, 'has',len(row),'columns instead of 6')
  return data
