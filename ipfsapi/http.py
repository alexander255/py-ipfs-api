# -*- encoding: utf-8 -*-
"""
Common base functionality of all IPFS-API HTTP backends
"""
from __future__ import absolute_import

import abc
import functools

from . import encoding


def pass_defaults(func):
    """Decorator that returns a function named wrapper.

    When invoked, wrapper invokes func with default kwargs appended.

    Parameters
    ----------
    func : callable
        The function to append the default kwargs to
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        merged = {}
        merged.update(self.defaults)
        merged.update(kwargs)
        return func(self, *args, **merged)
    return wrapper


def _notify_stream_iter_closed():
    pass  # Mocked by unit tests to determine check for proper closing


class HTTPClientBase(object):
    """An HTTP client for interacting with the IPFS daemon.

    Parameters
    ----------
    host : str
        The host the IPFS daemon is running on
    port : int
        The port the IPFS daemon is running at
    base : str
        The path prefix for API calls
    defaults : dict
        The default parameters to be passed to
        :meth:`~ipfsapi.http.HTTPClient.request`
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, host, port, base, **defaults):
        self.host = host
        self.port = port
        #API05: Replace this with multiaddr
        if not host.startswith("http://") or host.startswith("https://"):
            host = 'http://' + host

        self.base = '%s:%s/%s' % (host, port, base)

        self.defaults = defaults
        self._session = None

    @staticmethod
    def _build_params(*args, **opts):
        def value_to_str(value):
            if isinstance(value, bool):
                return "true" if value is True else "false"
            elif value is None:
                return None
            else:
                return str(value)
        
        # Set mandatory parameters
        opts["stream-channels"] = True
        
        params = []
        for name, value in opts.items():
            # Make sure that all parameters are strings (or `yarl` complains)
            name = str(name)
            value = value_to_str(value)
            if value is not None:
                params.append((name, value))
        for value in args:
            value = value_to_str(value)
            if value is not None:
                params.append(('arg', value))
        return params

    @abc.abstractmethod
    def _request(self, method, url, params, parser, stream=False, headers={},
                 data=None):
        pass

    @abc.abstractmethod
    def _download(self, method, url, path, compressed, params=[], headers={}):
        pass

    @pass_defaults
    def request(self, path, args=[], opts={}, stream=False,
                decoder=None, headers={}, data=None):
        """Makes an HTTP request to the IPFS daemon.

        This function returns the contents of the HTTP response from the IPFS
        daemon.

        Raises
        ------
        ~ipfsapi.exceptions.ErrorResponse
        ~ipfsapi.exceptions.ConnectionError
        ~ipfsapi.exceptions.ProtocolError
        ~ipfsapi.exceptions.StatusError
        ~ipfsapi.exceptions.TimeoutError

        Parameters
        ----------
        path : str
            The REST command path to send
        args : collections.abc.Iterable
            Positional parameters to be sent along with the HTTP request
        files : :class:`io.RawIOBase` | :obj:`str` | :obj:`list`
            The file object(s) or path(s) to stream to the daemon
        opts : collections.abc.Mapping
            Query string paramters to be sent along with the HTTP request
        stream : bool
            Return the request payload as an iterator over the received chunks
            instead of returning the entire result once everything has been
            downloaded?
        decoder : str
            The encoder to use to parse the HTTP response
        headers : collections.abc.Mapping
            Extra headers to send along with the request
        data : io.RawIOBase
            File-like object yielding the data to upload
        """
        url = self.base + path

        method = 'POST' if data else 'GET'
        params = self._build_params(*args, **opts)

        parser = encoding.get_encoding(decoder if decoder else "none")

        return self._request(method, url, params=params, parser=parser,
                             stream=stream, headers=headers, data=data)

    @pass_defaults
    def download(self, path, args=[], filepath=None, opts={}, compress=True,
                 headers={}):
        """Makes a request to the IPFS daemon to download a file.

        Downloads a file or files from IPFS into the current working
        directory, or the directory given by ``filepath``.

        Raises
        ------
        ~ipfsapi.exceptions.ErrorResponse
        ~ipfsapi.exceptions.ConnectionError
        ~ipfsapi.exceptions.ProtocolError
        ~ipfsapi.exceptions.StatusError
        ~ipfsapi.exceptions.TimeoutError

        Parameters
        ----------
        path : str
            The REST command path to send
        filepath : str
            The local path where IPFS will store downloaded files

            Defaults to the current working directory.
        args : list
            Positional parameters to be sent along with the HTTP request
        opts : dict
            Query string paramters to be sent along with the HTTP request
        compress : bool
            Whether the downloaded file should be GZip compressed by the
            daemon before being sent to the client
        """
        url = self.base + path
        path = filepath or '.'

        # Set default and mandatory parameters
        opts["archive"] = True
        opts.setdefault("compress", compress)

        method = 'GET'
        params = self._build_params(*args, **opts)
        return self._download(method, url, path, compress,
                              params=params, headers=headers)
