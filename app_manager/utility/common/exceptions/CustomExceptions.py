from fastapi import HTTPException, status

class ConfigNotLoadedException(Exception):
    def __init__(self, message = "Configuration not loaded"):
        super().__init__(message)

class UnspecifiedError(Exception):
    def __init__(self, message = "Unspecified Error"):
        super().__init__(message)

class UnauthorisedError(HTTPException):
    def __init__(self, message = "Unspecified Error"):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED,
                detail=message,
                headers={"WWW-Authenticate": "Basic"})

class IBIError(Exception):
    def __init__(self):
        message = "Unspecified Error during iBI Connection, Please refer logs"
        super().__init__(message)

class IBIConnectionError(Exception):
    def __init__(self, message):
        message = "IBI Connection Error :" + str(message)
        super().__init__(message)

class IBIAuthenticationError(Exception):
    def __init__(self, status_code):
        message = "IBI Credentials are not valid " + str(status_code)
        super().__init__(message)

class IBIFetchError(Exception):
    def __init__(self, status_code):
        message = "IBI Dataset Fetch Url is failed " + str(status_code)
        super().__init__(message)

class DGCConnectionError(Exception):
    def __init__(self, message):
        message = "DGC Connection Error :" + str(message)
        super().__init__(message)

class DGCError(Exception):
    def __init__(self, url, status_code, response):
        message = "Unexpected response " + str(url) + " : " + str(status_code) + " : " + str(response)
        super().__init__(message)

class DGCUnspecifiedError(Exception):
    def __init__(self):
        message = "Unexpected error while connecting with DGC Please refer logs  "
        super().__init__(message)

class OkeraConnectionError(Exception):
    def __init__(self, message):
        message = "Okera Connection Error :" + str(message)
        super().__init__(message)

class OkeraUnspecifiedError(Exception):
    def __init__(self,message):
        message = "Unexpected error while connecting with Okera Please refer logs" + str(message)
        super().__init__(message)

class OkeraFetchError(Exception):
    def __init__(self, message):
        message = "Could not fetch details from Okera " + str(message)
        super().__init__(message)

class OkeraNameError(Exception):
    def __init__(self, name):
        message = "Please give proper name of DB, Table or Column, Name given is: " + str(name)
        super().__init__(message)

class OkeraCommonError(Exception):
    def __init__(self, name):
        message = "An Okera common error occured" + str(name)
        super().__init__(message)

