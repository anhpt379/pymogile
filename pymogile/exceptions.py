# -*- coding: utf-8 -*-

__all__ = ['MogileFSError', 'MogileFSHTTPError', 'MogileFSTrackerError']

class MogileFSError(Exception):
  pass

class MogileFSTrackerError(MogileFSError):
  def __init__(self, errstr, err=None):
    self.errstr = errstr
    self.err = err

  def __str__(self):
    return self.errstr

class MogileFSHTTPError(MogileFSError):
  def __init__(self, code, content):
    self.code = code
    self.content = content

  def __repr__(self):
    return '<mogilefs.exceptions.MogileFSHTTPError status:%s>' % self.code

  def __str__(self):
    return 'HTTP Error %d, %s' % (self.code, self.content)
