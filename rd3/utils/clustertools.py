#'////////////////////////////////////////////////////////////////////////////
#' FILE: clustertools.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-04-25
#' MODIFIED: 2022-09-19
#' PURPOSE: methods for interacting with files on the cluster
#' STATUS: stable
#' PACKAGES: os, re, json, io, csv, rd3.utils
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import addForwardSlash, statusMsg
from os.path import basename
import subprocess
import re
import json
import io
import csv

class clustertools:
  def __init__(self, connectionMethod):
    """Cluster Tools"""
    self.method = connectionMethod

  def listFiles(self, path, filter: str=None, quietly: bool=False):
    """List Files
    List all files at a given path
    
    @param path location to directory
    @param filter a string containing a file type used to filter results
        (can also be a pattern)
    
    @return a list of dictionaries
    """
    available_files = subprocess.Popen(
      ['ssh', self.method, 'ls', path],
      stdout = subprocess.PIPE,
      stdin = subprocess.PIPE,
      stderr= subprocess.PIPE,
      universal_newlines=True
    )
    path = addForwardSlash(path)
    data = []
    for f in available_files.stdout:
      data.append({ 'filename': f.strip(), 'filepath': path + f.strip() })
    available_files.kill()
    if filter:
      filtered = []
      for d in data:
          q = re.compile(filter)
          if re.search(q, d['filename']):
            filtered.append(d)
      if not quietly:
        statusMsg('Found {} files'.format(len(filtered)))
      return filtered
    else:
      if not quietly:
        statusMsg('Found {} files'.format(len(data)))
      return data

  def readTextFile(self, path: str = None):
      """Read text file
      Read the contents of a text file (i.e., PED)
      
      @param path location of a file
      @return list of dictionaries, contents of file 
      """
      proc = subprocess.Popen(
          ['ssh', self.method, 'cat', path],
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
      
  def readCsv(self, path: str = None):
    """Read CSV file
    Read the contents of a csv files
    @param path location of the file
    @return list of dictionaries where each dict is a row in the csv file
    """
    proc = subprocess.Popen(
      ['ssh', self.method, 'cat', path],
      stdout = subprocess.PIPE
    )
    procWrapper = io.TextIOWrapper(proc.stdout)
    csvReader = csv.DictReader(procWrapper)
    data = []
    for line in csvReader:
      data.append(dict(line))
    proc.kill()
    return data

  def readJson(self, path: str = None):
    """Read Json file
    Read a json file located on the cluster
    
    @param path location of the file
    @return list containing contents of a json file
    """
    proc = subprocess.Popen(
      ['ssh', self.method, 'cat', path],
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
      statusMsg('Error: unable to fetch file {}'.format(str(basename(path))))
      proc.kill()
      return ''
          
  def md5sum(self,path: str = None):
    """Run checksum on a file
    Run md5 on a file and return the value
    
    @param path location of the file run the checksum
    @return string containing checksum value of a file
    """
    proc = subprocess.Popen(
      ['ssh', self.method, 'md5sum', path],
      stdout = subprocess.PIPE,
      stdin = subprocess.PIPE,
      stderr = subprocess.PIPE,
      universal_newlines=True
    )
    value = proc.stdout.read().split()[0]
    proc.kill()
    return value
