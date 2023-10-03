
# tagoma Oct23
# NASS package https://nass.readthedocs.io/en/latest/ is not any longer available fomr install
# Below is appending of the 3 .py files from the original package
# All credits go to original author Nick Fros


# api.py

# -*- coding: utf-8 -*-
"""
This module contains the high-level API object
"""

import requests
#from . import exceptions
#from .query import Query


class NassApi(object):
    """NASS API wrapper class

    :param key: API key

    Usage::
      >>> from nass import NassApi
      >>> api = NassApi('api key')

    """

    BASE_URL = 'http://quickstats.nass.usda.gov/api'

    def __init__(self, key):
        self.key = key
        self.http = requests.Session()

    def _api_request(self, url, params, field_name):
        """Make the HTTP request

        The API key is added to the params dictionary. If there is an
        error connecting, or if the response isn't valid JSON, raises an
        exception.

        :param url: The url (appended to API base url)
        :param params: Values to be encoded as the query string
        :param field_name: Key of result in JSON response to be returned
        :return: The decoded value of field_name in the response

        """
        api_url = self.BASE_URL + url
        params.update({'key': self.key})

        try:
            resp = self.http.get(api_url, params=params)
        except requests.RequestException:
            raise exceptions.NetworkException()

        try:
            data = resp.json()
        except ValueError:
            raise exceptions.InvalidJson(resp)

        return self._handle_response_data(data, resp, field_name)

    @classmethod
    def _handle_response_data(cls, data, response, field_name):
        """Parses response object

        Expects the response text to be a dictionary containing a key
        with the name field_name.
            1. Makes sure response contains no errors
            2. Checks that the status code was 200
            3. Returns the desired value from the JSON object
        If any of the above steps fail, raises an exception

        :param data: The decoded JSON data
        :param response: :class:`requests.Response` object
        :param field_name: Key of result in data to be returned
        :return: The value of field_name in data

        """

        try:
            errors = data['error']
        except (KeyError, TypeError):
            pass
        else:
            cls._raise_for_error_message(data['error'], response)

        if response.status_code != 200:
            raise exceptions.NassException(response)

        try:
            result = data[field_name]
        except (KeyError, TypeError):
            raise exceptions.UnexpectedResponseData(data, response)

        return result

    @classmethod
    def _raise_for_error_message(cls, errors, response):
        """Raises an exception from an error message

        Will attempt to raise some subclass of ApiException

        :param errors: The list of error messages
        :param response: :class:`requests.Response` object

        """
        if isinstance(errors, list):
            if len(errors) > 1:
                raise exceptions.ExceptionList(errors, response)
            elif len(errors) == 1:
                message = errors[0]
                error_classes = {
                    'unauthorized': exceptions.Unauthorized,
                    'bad request - invalid query': exceptions.InvalidQuery,
                    'bad request - unsupported media type':
                        exceptions.BadMediaType,
                }
                if message in error_classes:
                    exc_class = error_classes[message]
                    raise exc_class(message, response)
                elif message.startswith('exceeds limit'):
                    try:
                        rows = int(message.split('=')[1])
                    except (IndexError, ValueError):
                        rows = None
                    raise exceptions.ExceedsRowLimit(rows, message, response)
                else:
                    raise exceptions.ApiException(message, response)

    def param_values(self, param):
        """Returns all possible values of the given parameter

        :param param: The parameter name
        :return: Possible values
        :rtype: list

        Usage::

          >>> from nass import NassApi
          >>> api = NassApi('api key')
          >>> api.param_values('source_desc')
          >>> ['CENSUS', 'SURVEY']

        """
        return self._api_request('/get_param_values/', {'param': param}, param)

    def query(self):
        """Creates a query used for filtering

        :return: :class:`Query <nass.query.Query>` object
        :rtype: nass.query.Query

        Usage::

          >>> from nass import NassApi
          >>> api = NassApi('api key')
          >>> q = api.query()
          >>> q.filter('commodity_desc', 'CORN').filter('year', 2012)
          >>> q.count()
          141811

        """
        return Query(self)

    def count_query(self, query):
        """Returns the row count of a given query

        This is called internally by :meth:`Query.count()
        <nass.query.Query.count>`, try not to call it directly.

        :param query: the :class:`Query <nass.query.Query>` object
        :return: The number of rows in the result
        :rtype: int

        """
        count = self._api_request('/get_counts/', query.params, 'count')
        return int(count)

    def call_query(self, query):
        """Returns the result of a given query

        This is called internally by :meth:`Query.execute()
        <nass.query.Query.execute>`, try not to call it directly.

        :param query: the :class:`Query <nass.query.Query>` object
        :return: The results of the query
        :rtype: list

        """
        return self._api_request('/api_GET/', query.params, 'data')



# exception.py

# -*- coding: utf-8 -*-
"""
This module contains exceptions raised by this package

All exceptions subclass NassException, you can use it to catch all
exceptions
"""


class NassException(Exception):
    """Base exception class for an erroneous response

    :param response: :class:`requests.Response` object

    """

    def __init__(self, response):
        super(NassException, self).__init__()
        self.response = response


class NetworkException(NassException):
    """Something went wrong making the request to the server

    Raised when a :class:`requests.exceptions.RequestException` is
    caught.

    """

    def __init__(self):
        super(NetworkException, self).__init__(None)


class InvalidJson(NassException):
    """Server returned malformed JSON"""


class UnexpectedResponseData(NassException):
    """Server returned different response data than we expected

    :param data: The data the server did return
    :param response: :class:`requests.Response` object

    """

    def __init__(self, data, response):
        super(UnexpectedResponseData, self).__init__(response)
        self.data = data


class ApiException(NassException):
    """Base exception class for error messages returned by NASS

    :param message: Error message
    :param response: :class:`requests.Response` object

    """

    def __init__(self, message, response):
        super(ApiException, self).__init__(response)
        self.message = message

    def __str__(self):
        """Return the error message"""
        return 'Server returned error message \"%s\"' % self.message


class ExceptionList(NassException):
    """Raised when we get more than one error message

    :param errors: The list of error messages
    :param response: :class:`requests.Response` object

    """

    def __init__(self, errors, response):
        super(ExceptionList, self).__init__(response)
        self.errors = errors

    def __str__(self):
        """Return the comma-separated list of errors"""
        return 'Server returned error messages %s' % ', '.join(self.errors)


class ExceedsRowLimit(ApiException):
    """The request would return more than 50,000 records/rows

    :param rows: The row limit
    :param message: Error message
    :param response: :class:`requests.Response` object

    """

    def __init__(self, rows, message, response):
        super(ExceedsRowLimit, self).__init__(message, response)
        self.rows = rows


class Unauthorized(ApiException):
    """There is no key or invalid key parameter"""


class InvalidQuery(ApiException):
    """There is an error in the query string"""


class BadMediaType(ApiException):
    """The request format parameter is not JSON or CSV or XML"""



# query.py

# -*- coding: utf-8 -*-
"""
This module contains the Query object
"""
class Query(object):
    """The Query class constructs the URL params for a request

    :param api: The :class:`NassApi <nass.api.NassApi>` object

    """

    def __init__(self, api):
        self.api = api
        self.params = {}

    def filter(self, param, value, op=None):
        """Apply a filter to the query

        Returns the :class:`Query <nass.query.Query>` object, so this
        method is chainable.

        The following code:
          >>> q.filter('commodity_desc', 'CORN')
          >>> q.filter('year', 2012)
        is equivalent to this code:
          >>> q.filter('commodity_desc', 'CORN').filter('year', 2012)

        :param param: Parameter name to filter
        :param value: Value to test against
        :param op: (optional) Operator comparing param and value
        :return: :class:`Query <nass.query.Query>` object
        :rtype: nass.query.Query

        """
        if op is None:
            self.params[param] = value
        elif op in ('le', 'lt', 'ge', 'gt', 'like', 'not_like', 'ne'):
            param_key = '{param}__{op}'.format(param=param, op=op.upper())
            self.params[param_key] = value
        else:
            raise TypeError('Invalid operator: %r' % op)
        return self

    def count(self):
        """Pass count request to :class:`NassApi <nass.api.NassApi>`

        :return: The number of rows in the result
        :rtype: int

        """
        return self.api.count_query(self)

    def execute(self):
        """Pass query along to :class:`NassApi <nass.api.NassApi>`

        :return: The results of the query

        """
        return self.api.call_query(self)
