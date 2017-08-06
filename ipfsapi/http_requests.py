"""
HTTP client for synchronous API requests based on `requests`
"""
from __future__ import absolute_import

import tarfile
from six.moves import http_client

import requests
import six

from . import encoding
from . import exceptions
from . import http


class StreamDecodeIterator(object):
	"""
	Wrapper around `Iterable` that allows the iterable to be used in a
	context manager (`with`-statement) allowing for easy cleanup.
	"""
	def __init__(self, response, parser):
		self._response = response
		self._parser   = parser
		self._response_iter = response.iter_content(chunk_size=None)
		self._parser_iter   = None

	def __iter__(self):
		return self

	def __next__(self):
		while True:
			# Try reading for current parser iterator
			if self._parser_iter is not None:
				try:
					return next(self._parser_iter)
				except StopIteration:
					self._parser_iter = None

					# Forward exception to caller if we do not expect any
					# further data
					if self._response_iter is None:
						raise

			try:
				data = next(self._response_iter)

				# Create new parser iterator using the newly recieved data
				self._parser_iter = iter(self._parser.parse_partial(data))
			except StopIteration:
				# No more data to receive – destroy response iterator and
				# iterate over the final fragments returned by the parser
				self._response_iter = None
				self._parser_iter   = iter(self._parser.parse_finalize())

	#PY2: Old iterator syntax
	def next(self):
		return self.__next__()

	def __enter__(self):
		return self

	def __exit__(self, *a):
		self.close()

	def close(self):
		# Clean up any open iterators first
		if self._response_iter is not None:
			self._response_iter.close()
		if self._parser_iter is not None:
			self._parser_iter.close()
		self._response_iter = None
		self._parser_iter   = None

		# Clean up response object and parser
		if self._response is not None:
			self._response.close()
		self._response = None
		self._parser   = None

		http._notify_stream_iter_closed()


def stream_decode_full(response, parser):
	with StreamDecodeIterator(response, parser) as response_iter:
		result = list(response_iter)
		if len(result) == 1:
			return result[0]
		else:
			return result


class HTTPClient(http.HTTPClientBase):
	"""An HTTP client for interacting with the IPFS daemon.
	"""
	
	def open(self):
		if self._session is None:
			self._session = requests.session()
	
	def close(self):
		if self._session is not None:
			self._session.close()
			self._session = None
	
	def _do_request(self, method, url, **kwargs):
		# Even if we return only one aggregate result we want to be to build that
		# result incrementally for performance reasons
		kwargs["stream"] = True
		
		try:
			if self._session:
				response = self._session.request(method, url, **kwargs)
			else:
				response = requests.request(method, url, **kwargs)
		except requests.ConnectionError as error:
			six.raise_from(exceptions.ConnectionError(error), error)
		except http_client.HTTPException as error:
			six.raise_from(exceptions.ProtocolError(error), error)
		except requests.Timeout as error:
			six.raise_from(exceptions.TimeoutError(error), error)
		
		# Handle error responses
		if response.status_code >= 400 and response.status_code < 600:
			# Usually error responses include a message as JSON
			try:
				result = stream_decode_full(response, encoding.Json())
			except exceptions.DecodingError:
				result = None
			
			# Call the normal `response.raise_for_status`
			try:
				response.raise_for_status()
			except requests.exceptions.HTTPError as error:
				# If we have decoded an error response from the server,
				# use that as the exception message; otherwise, just pass
				# the exception on to the caller.
				if isinstance(result, dict) and 'Message' in result:
					six.raise_from(exceptions.ErrorResponse(result['Message'], error), error)
				else:
					six.raise_from(exceptions.StatusError(error), error)
			raise AssertionError("Response status should have raised exception")
		
		return response
	
	# Overrides required by `http.HTTPClientBase`
	def _request(self, method, url, params, parser, stream=False, headers={},
	             data=None):
		# Do HTTP request (synchronously)
		response = self._do_request(method, url, params=params, headers=headers,
		                            data=data)
		
		if stream:
			# Decode each item as it is read
			return StreamDecodeIterator(response, parser)
		else:
			# Decode everything before returning
			return stream_decode_full(response, parser)
	
	def _download(self, method, url, path, compressed, params=[], headers={}):
		response = self._do_request(method, url, params=params, headers=headers)
		
		# Try to stream download as a tar file stream
		mode = 'r|gz' if compressed else 'r|'
		with tarfile.open(fileobj=response.raw, mode=mode) as tf:
			tf.extractall(path=path)
