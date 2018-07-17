"""
HTTP client for asynchronous API requests (Python 3-only)
"""
import asyncio
import os
import threading
import tarfile

from async_generator import async_generator, yield_, yield_from_
import aiohttp

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
		self._response_iter = response.content.iter_any()
		self._parser_iter   = None

	def __aiter__(self):
		return self

	async def __anext__(self):
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
						raise StopAsyncIteration()

			try:
				data = await self._response_iter.__anext__()

				# Create new parser iterator using the newly recieved data
				self._parser_iter = iter(self._parser.parse_partial(data))
			except StopIteration:
				# No more data to receive – destroy response iterator and
				# iterate over the final fragments returned by the parser
				self._response_iter = None
				self._parser_iter   = iter(self._parser.parse_finalize())

	async def __aenter__(self):
		return self

	async def __aexit__(self, *a):
		
		self.close()

	def close(self):
		# Clean up any open iterators first
		if self._parser_iter is not None:
			self._parser_iter.close()
		self._response_iter = None
		self._parser_iter   = None

		# Clean up response object and parser
		if self._response is not None:
			#XXX: Run synchronously release assuming that it will not block on
			#     I/O ever (since we cannot have any real AIO in `.close()`)
			release_gen = self._response.release()
			if hasattr(release_gen, "__await__"):
				release_gen = release_gen.__await__()
			for _ in release_gen:
				pass
		self._response = None
		self._parser   = None

		http._notify_stream_iter_closed()


async def stream_decode_full(response, parser):
	async with StreamDecodeIterator(response, parser) as response_iter:
		#PY35: Cannot use: result = [i async for i in response_iter]
		result = []
		async for item in response_iter:
			result.append(item)
		if len(result) == 1:
			return result[0]
		else:
			return result


class HTTPClient(http.HTTPClientBase):
	"""An HTTP client for interacting with the IPFS daemon.
	"""
	
	def open(self):
		if self._session is None:
			self._session = aiohttp.ClientSession(loop=self.loop)
	
	def close(self):
		if self._session is not None:
			self._session.close()
			self._session = None
	
	async def _do_request(self, method, url, **kwargs):
		try:
			#TODO: Loop parameter for session
			if self._session:
				response = await self._session.request(method, url, **kwargs)
			else:
				async with aiohttp.ClientSession(loop=self.loop) as session:
					response = await session.request(method, url, **kwargs)
		except (aiohttp.ClientConnectorError, aiohttp.ServerDisconnectedError) as error:
			raise exceptions.ConnectionError(error) from error
		except aiohttp.ClientResponseError as error:
			raise exceptions.ProtocolError(error) from error
		except asyncio.TimeoutError as error:
			raise exceptions.TimeoutError(error) from error
		
		# Handle error responses
		if response.status >= 400 and response.status < 600:
			# Usually error responses include a message as JSON
			try:
				result = await stream_decode_full(response, encoding.Json())
			except exceptions.DecodingError:
				result = None
			
			# Call the normal `response.raise_for_status`
			try:
				response.raise_for_status()
			except aiohttp.ClientResponseError as error:
				# If we have decoded an error response from the server,
				# use that as the exception message; otherwise, just pass
				# the exception on to the caller.
				if isinstance(result, dict) and 'Message' in result:
					raise exceptions.ErrorResponse(result['Message'], error) from error
				else:
					raise exceptions.StatusError(error) from error
			raise AssertionError("Response status should have raised exception")
		
		return response
	
	# Stream and non-streaming mode implementations
	#PY35: `async def …: yield y` not supported natively
	@async_generator
	async def _request_iter(self, method, url, parser, **kwargs):
		# Do HTTP request
		response = await self._do_request(method, url, **kwargs)
		
		# Pass through parsed response datasets
		await yield_from_(StreamDecodeIterator(response, parser))
	
	async def _request_full(self, method, url, parser, **kwargs):
		# Do HTTP request
		response = await self._do_request(method, url, **kwargs)
		
		# Return fully parsed response
		return await stream_decode_full(response, parser)
	
	# Overrides required by `http.HTTPClientBase`
	def _request(self, method, url, params, parser, stream=False, headers={},
	             data=None):
		# Do actual processing in sub-routines so that we can return an
		# async-generator for the `stream=True` case, a coroutine otherwise
		if stream:
			return self._request_iter(method, url, parser, params=params,
			                          headers=headers, data=data)
		else:
			return self._request_full(method, url, parser, params=params,
			                          headers=headers, data=data)
	
	async def _download(self, method, url, path, compressed,
	                    params=[], headers={}):
		res = await self._do_request(method, url, params=params, headers=headers)
		
		# Open pipe to stream the received data to another thread
		(fd_r, fd_w) = os.pipe()
		file_r = os.fdopen(fd_r, "rb")
		file_w = os.fdopen(fd_w, "wb")
		
		try:
			# Start background thread for parsing the TAR data synchronously
			task = _TarThread(file_r, path, compressed)
			task.start()
			
			# Copy all TAR data from the asynchronous to the synchronous world
			async for data in res.content.iter_any():
				file_w.write(data)
			file_w.close()
			
			# Wait for the background thread to exit after receiving our EOF
			task.join()
		finally:
			# Make sure no FDs are left standing
			file_w.close()
			file_r.close()


#XXX: Replace this by a streaming TAR/PAX parser implementation … sometime
#     For some reason a micro-benchmark shows that this is still about 1.5x
#     faster than using `requests` and blocking `tarfile` in the same thread.
class _TarThread(threading.Thread):
	def __init__(self, fileobj, path, compressed):
		super().__init__()
		
		self.fileobj    = fileobj
		self.path       = path
		self.compressed = compressed
	
	def run(self):
		# Try to stream download as a tar file stream
		mode = 'r|gz' if self.compressed else 'r|'
		with tarfile.open(fileobj=self.fileobj, mode=mode) as tf:
			tf.extractall(path=self.path)
