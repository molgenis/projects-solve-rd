#'////////////////////////////////////////////////////////////////////////////
#' FILE: cli.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-04
#' MODIFIED: 2023-05-05
#' PURPOSE: colored terminal messages
#' STATUS: stable
#' PACKAGES: NONE
#' COMMENTS: NONE
#'////////////////////////////////////////////////////////////////////////////

class cli:
  def __init__(self):
    self._start = '\u001b'
    self._stop = '\u001b[0m'
    self._blue = '[34;1m'
    self._green = '[32:1m'
    self._red = '[31;1m'
    self._white = '[37;1m'
    self._yellow = '[33;1m'
    self.error = self._setStatus(self._red, '⨉')
    self.success = self._setStatus(self._green, '✓')
    self.warning = self._setStatus(self._yellow, '!')
      
  def _setStatus(self, color, text):
    return self._start + color + text + self._stop
  
  def _c(self, *args):
    """Concatenate"""
    return ' '.join(map(str, args))
  
  def alert_success(self, *args):
    """Print a success message"""
    print(f"{self.success} {self._c(*args)}")

  def alert_error(self, *args):
    """Print an error message"""
    print(f'{self.error} {self._c(*args)}')

  def alert_warning(self, *args):
    """Print a warning message"""
    print(f'{self.warning} {self._c(*args)}') 
      
  def text_value(self, *args):
    """Style text as a value"""    
    return self._start + self._blue + self._c(*args) + self._stop
  
  def text_success(self, *args):
    """Style text as a success message"""
    return self._start + self._green + self._c(*args) + self._stop
      
  def text_error(self, *args):
    """Style text as an error message"""
    return self._start + self._red + self._c(*args) + self._stop
  
  def text_warning(self, *args):
    """Style text as an warning message"""
    return self._start + self._yellow + self._c(*args) + self._stop
