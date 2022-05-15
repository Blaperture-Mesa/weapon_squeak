from logging import getLogger
from asyncio import TimeoutError as ASyncTimeoutError
from starlette.exceptions import HTTPException
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
__all__ = [
    "APP"
    , "LOGGER"
    , "init"
    , "get_exception_dict"
    , "response_exception"
    , "response_busy"
    , "response_goaway"
]



APP = FastAPI()
LOGGER = getLogger( "uvicorn" )
APP_RESPONSE_BUSY_RETRYAFTER = 30


def init ():
    @APP.exception_handler( Exception )
    async def exception_generic (request: Request, exc: Exception):
        response = JSONResponse()
        response.body = response.render( response_exception(response, exc) )
        return response

    @APP.get( "/ping" )
    async def get_ping ():
        return "pong!"

    @APP.get( "/{_:path}" )
    async def get_away (_: str, response: Response):
        return response_goaway( response )


def response_exception (response: Response, exc: BaseException):
    if response:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    if isinstance( exc, (TimeoutError, ASyncTimeoutError,) ):
        if response:
            response.status_code = status.HTTP_404_NOT_FOUND
    else:
        LOGGER.critical( exc, exc_info=True, )
    return get_exception_dict( exc )


def response_busy (response: Response):
    if response:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        response.headers["Retry-After"] = str( APP_RESPONSE_BUSY_RETRYAFTER )
    return {
        "status": False
        , "retry-after": APP_RESPONSE_BUSY_RETRYAFTER
    }


def response_goaway (response: Response):
    if response:
        response.status_code = status.HTTP_418_IM_A_TEAPOT
    return response


def get_exception_dict (exc: BaseException):
    detail = str( exc )
    if isinstance( exc, HTTPException ):
        detail = exc.detail
    return {
        "status": False
        , "error": (type(exc).__name__, detail,)
    }
