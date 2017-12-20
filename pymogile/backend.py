#! coding: utf-8

import re
import time
import select
import socket
import signal
import random
import urllib
import logging
from cgi import parse_qs
from errno import EINPROGRESS, EISCONN

from pymogile.exceptions import MogileFSError

CONSOLE_HANDLER = logging.StreamHandler()

LOG = logging.getLogger("MogileFS Backend")
LOG.setLevel(logging.INFO)
LOG.addHandler(CONSOLE_HANDLER)

PROTO_TCP = socket.getprotobyname('tcp')
MSG_NOSIGNAL = 0x4000
FLAG_NOSIGNAL = MSG_NOSIGNAL

ERR_RE = re.compile(r'^ERR\s+(\w+)\s*(\S*)')
OK_RE = re.compile(r'^OK\s+\d*\s*(\S*)')


def _encode_url_string(args):
    if not args:
        return ''
    buf = []
    for k, v in args.items():
        buf.append(
            '%s=%s' % (urllib.quote_plus(str(k)), urllib.quote_plus(str(v)))
        )
    return '&'.join(buf)


def _decode_url_string(arg):
    params = {}
    for k, values in parse_qs(arg).items():
        params[k] = values[0]
    return params


class Backend(object):
    def __init__(self, trackers, timeout=None):
        self.last_host_connected = None
        self._hosts = []
        self._sock_cache = None
        for tracker in trackers:
            try:
                addr, port = tracker.split(':', 1)
            except ValueError:
                raise ValueError(
                    "trackers argument must be of form: 'tracker:port'"
                )

            try:
                port = int(port)
            except (ValueError, TypeError):
                raise ValueError('port must be an integer: %r given' % port)

            self._hosts.append((addr, port))

        if timeout is None:
            self._timeout = 3
        else:
            try:
                self._timeout = int(timeout)
            except ValueError:
                raise ValueError("timeout argument must be a number")

        self._host_dead = {}
        self._pref_ip = {}

    def set_pref_ip(self, pref_ip):
        if not isinstance(pref_ip, dict):
            try:
                pref_ip = dict(pref_ip)
            except ValueError:
                raise ValueError("argument pref_ip must a dict")
        self._pref_ip = pref_ip

    def _wait_for_readability(self, fileno, timeout):
        if not fileno or not timeout:
            return 0
        return not not (select.select([fileno], [], [], timeout)[0])

    def do_request(self, cmd, args=None):
        req = '%s %s\r\n' % (cmd, _encode_url_string(args))
        reqlen = len(req)

        if FLAG_NOSIGNAL:
            try:
                signal.signal(signal.SIGPIPE, signal.SIG_IGN)
            except:
                pass

        rv = 0
        sock = self._sock_cache
        if sock:
            self.run_hook('do_request_start', cmd, self.last_host_connected)
            LOG.debug("SOCK: cached = %r, REQ: %r" % (sock, req))

            # send FLAG_NOSIGNAL
            try:
                rv = sock.send(req, FLAG_NOSIGNAL)
                if rv != reqlen:
                    self.run_hook(
                        'do_request_length_mismatch', cmd,
                        self.last_host_connected
                    )
                    raise MogileFSError(
                        """
                        send() didn't return expected length (%s, not %s)
                        """ % (rv, reqlen)
                    )
            except socket.error, e:
                self.run_hook(
                    'do_request_send_error', cmd, self.last_host_connected
                )
                self._sock_cache = None

        if not rv:
            ## may cause an exception
            sock = self._get_sock()
            if sock is None:
                raise MogileFSError(
                    """
                    couldn't connect to any mogilefs backends: %s
                    """ % self._hosts
                )

            self.run_hook('do_request_start', cmd, self.last_host_connected)
            LOG.debug("SOCK: %r, REQ: %r" % (sock, req))

            # send FLAG_NOSIGNAL
            try:
                rv = sock.send(req, FLAG_NOSIGNAL)
            except socket.error, e:
                self.run_hook(
                    'do_request_send_error', cmd, self.last_host_connected
                )
                raise MogileFSError(
                    """
                    couldn't send command: [%s]. reason: %s
                    """ % (req, e)
                )

            if rv != reqlen:
                self.run_hook(
                    'do_request_length_mismatch', cmd, self.last_host_connected
                )
                raise MogileFSError(
                    """
                    send() didn't return expected length (%s,MogileFSError not %s)
                    """ % (rv, reqlen)
                )

        ## wait up to 3 seconds for the socket to come to life
        if not self._wait_for_readability(sock.fileno(), self._timeout):
            sock.close()
            self.run_hook(
                'do_request_read_timeout', cmd, self.last_host_connected
            )
            raise MogileFSError("""
                tracker socket never became readable (%s) when sending command: [%s]
                """ % (self.last_host_connected, req))

        sockfile = sock.makefile()
        line = sockfile.readline()

        self.run_hook('do_request_finished', cmd, self.last_host_connected)
        LOG.debug('RESPONSE: %r' % line)

        matcher = OK_RE.match(line)
        if matcher:
            args = _decode_url_string(matcher.group(1))
            LOG.debug("RETURN_VARS: %r" % args)
            return args

        matcher = ERR_RE.match(line)
        if matcher:
            self.lasterr, self.lasterrstr = map(
                urllib.unquote_plus, matcher.groups()
            )
            LOG.debug("LASTERR: %s %s" % (self.lasterr, self.lasterrstr))
            raise MogileFSError(self.lasterrstr, self.lasterr)

        raise MogileFSError('invalid response from server: [%s]' % line)

    def run_hook(self, hookname, *args):
        pass

    def add_hook(self, hookname, *args):
        pass

    def _sock_to_host(self, tracker):
        # try preferred ips
        if tracker[0] in self._pref_ip:
            prefip = self._pref_ip[tracker[0]]
            LOG.debug("using preferred ip %s over %s" % (tracker[0], prefip))
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, PROTO_TCP)
            prefhost = (prefip, tracker[1])
            if self._connect_sock(sock, prefhost, 0.1):
                self.last_host_connected = prefhost
                # successfully connected so return this socket
                return sock
            else:
                LOG.debug(
                    "failed connect to preferred tracker %s" % str(prefhost)
                )
                sock.close()

        # now try the original ip
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, PROTO_TCP)
        if self._connect_sock(sock, tracker):
            self.last_host_connected = tracker
            return sock
        else:
            return None

    def _connect_sock(self, sock, sin, timeout=0.25):
        if timeout:
            # make the socket non-blocking for the connection if wanted, but
            # unconditionally set it back to blocking mode at the endut:
            sock.settimeout(timeout)
        else:
            sock.setblocking(1)

        try:
            err = sock.connect_ex(sin)
        except socket.gaierror:
            err = 'bogus error'

        if err:
            connected = False
        else:
            connected = True

        if err and timeout and err == EINPROGRESS:
            ret = select.select([], [sock.fileno()], [], timeout)[1]
            if ret:
                err = sock.connect_ex(sin)
                if err == EISCONN:
                    # EISCONN means connected & won't re-connect, so success.
                    connected = True

        if timeout:
            #turn blocking back on, as we expect to do blocking IO on our sockets
            sock.setblocking(1)

        return connected

    def _get_sock(self):
        size = len(self._hosts)
        tries = size > 15 and 15 or size
        idx = random.randint(0, tries)
        now = time.time()

        for _ in xrange(1, tries + 1):
            tracker = self._hosts[idx % size]
            idx += 1
            # try dead trackers every 5 seconds
            if tracker in self._host_dead:
                if self._host_dead[tracker] > now - 5:
                    continue
            sock = self._sock_to_host(tracker)
            if sock:
                break
            # mark sock as dead
            LOG.debug("marking tracker dead: %s @ %d" % (tracker, now))
            self._host_dead[tracker] = now
        else:
            sock = None
        return sock
