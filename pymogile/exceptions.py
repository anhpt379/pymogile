#! coding: utf-8


class MogileFSError(Exception):
    def __init__(self, errstr, err=None):
        self.errstr = errstr
        self.err = err

    def __str__(self):
        return self.errstr

    def __repr__(self):
        return '<pymogile.exceptions.MogileFSError: %s>' % self.errstr


class HTTPError(Exception):
    def __init__(self, code, content):
        self.code = code
        self.content = content

    def __repr__(self):
        return '<pymogile.exceptions.HTTPError status:%s>' % self.code

    def __str__(self):
        return 'HTTP Error %d, %s' % (self.code, self.content)
