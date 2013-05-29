class Error(Exception):
    """
    Base server class exception
    """
    def __init__(self, message=None, errcode=None, exc=None):
        self.message = message if message else self.__class__.message
        self.errcode = errcode if errcode else self.__class__.errcode
        self.exc = exc if exc else '' # exc will be anything, like string and Exception class, and etc...
    
    def __str__(self):
        return "%s : %s %s" % (self.message, self.errcode, self.exc)

class PathNotExists(Error):
    message = "Server config file not found"
    errcode = "100"

class ServerNodeError(Error):
    message = "Server node has return error"
    errcode = "101"

class NodeNotResponse(Error):
    message = "Load balance node has no response"
    errcode = "1011"

class NginxPerformError(Error):
    message = "Nginx action perform error"
    errcode = "1012"

class AgentError(Error):
    message = "Agent error"
    errcode = '102'

class AgentNoHostTypeError(AgentError):
    def __init__(self):
        message = "No host type parameter found"
        errcode = '103'
        super(AgentNoHostTypeError,self).__init__(message,errcode)

class InstanceNotUniqueError(Error):
    message = "The amount of instance are more thant one."
    errcode = '1101'

class NoInstanceError(Error):
    message = "Can not found any instance, please check the loadbalance table."
    errcode = '1102'

# exception for openstack nova and keystone and etc...
class ClientException(Exception):
    """
    The base exception class for all exceptions this library raises.
    """
    def __init__(self, code, message=None, details=None):
        self.code = code
        self.message = message or self.__class__.message
        self.details = details

    def __str__(self):
        return '%s (HTTP %s)' % (self.message, self.code)


class BadRequest(ClientException):
    """
    HTTP 400 - Bad request: you sent some malformed data.
    """
    http_status = 400
    message = 'Bad request'


class Unauthorized(ClientException):
    """
    HTTP 401 - Unauthorized: bad credentials.
    """
    http_status = 401
    message = 'Unauthorized'


class Forbidden(ClientException):
    """
    HTTP 403 - Forbidden: your credentials don't give you access to this
    resource.
    """
    http_status = 403
    message = 'Forbidden'


class NotFound(ClientException):
    """
    HTTP 404 - Not found
    """
    http_status = 404
    message = 'Not found'


class OverLimit(ClientException):
    """
    HTTP 413 - Over limit: you're over the API limits for this time period.
    """
    http_status = 413
    message = 'Over limit'


class HTTPNotImplemented(ClientException):
    """
    HTTP 501 - Not Implemented: the server does not support this operation.
    """
    http_status = 501
    message = 'Not Implemented'


class SeverError(ClientException):
    """
    HTTP 500 - ServerError
    """
    http_status = 500
    message = 'Server Error'


CODE_MAP = dict((c.http_status, c) for c in [BadRequest, Unauthorized,
                   Forbidden, NotFound, OverLimit, HTTPNotImplemented])


def from_response(response, body):
    """
    Return an instance of an ClientException or subclass
    based on an httplib2 response.

    Usage::

        resp, body = http.request(...)
        if resp.status != 200:
            raise exception_from_response(resp, body)
    """
    cls = CODE_MAP.get(response.status, ClientException)
    if body:
        message = 'n/a'
        details = 'n/a'
        if hasattr(body, 'keys'):
            error = body[body.keys()[0]]
            message = error.get('message', None)
            details = error.get('details', None)
        return cls(code=response.status, message=message, details=details)
    else:
        return cls(code=response.status)


class DashboardException(Exception):
    pass


class NotAuthorized(DashboardException):
    """
    Raised whenever a user attempts to access a resource which they do not
    have role-based access to .
    """
    status_code = 401


class NotFound(DashboardException):
    status_code = 404

    
class ServiceCatalogException(DashboardException):
    """
    Raised when a requested service is not available in the ``ServiceCatalog``
    returned by Keystone.
    """
    def __init__(self, service_name):
        message = 'Invalid service catalog service: %s' % service_name
        super(ServiceCatalogException, self).__init__(message)
