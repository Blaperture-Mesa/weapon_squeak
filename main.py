from logging import getLogger
from os import environ
from time import time, sleep
from hashlib import md5
from pprint import pformat
from functools import partial
from asyncio import TimeoutError as ASyncTimeoutError
import csotools_serverquery as a2s
import csotools_serverquery.connection as a2s_con
from starlette.exceptions import HTTPException
from fastapi import FastAPI, Request, Response, Depends, status
from fastapi.responses import JSONResponse
from fastapi_etag import Etag, add_exception_handler as etag_add_exception_handler
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from weapon_squeak.threadutils import ThreadPoolManager, run_daemon_thread



BM_SQUEAK_ADDRESS = list( filter(len, (
    x
    for x
    in environ.get("BM_SQUEAK_ADDRESS", "121.14.44.33;121.14.44.34;121.14.44.35;121.14.44.36;121.14.44.37;121.14.44.38;121.14.44.39;121.14.44.40;121.14.44.41;121.14.44.42;121.14.44.43;121.14.44.44;121.14.44.45;121.14.44.46;121.14.44.47;121.14.44.48;121.14.44.49;121.14.44.50;121.14.44.51;121.14.44.52;121.14.44.53;121.14.44.54;121.14.44.55;121.14.44.56;121.14.44.57;121.14.44.58;121.14.44.59;121.14.44.60;121.14.44.61;121.14.44.62;121.14.44.63;121.14.44.64;121.14.44.65;121.14.44.66;121.14.44.67").split(";")
)) )
BM_SQUEAK_CHN_PORT_START = int( environ.get("BM_SQUEAK_CHN_PORT_START", 40000) )
BM_SQUEAK_CHN_PORT_SIZE = int( environ.get("BM_SQUEAK_CHN_PORT_SIZE", 120) )
BM_SQUEAK_MAX_THREAD = int( environ.get("BM_SQUEAK_MAX_THREAD", 236) )
BM_SQUEAK_CACHE_TIME = int( environ.get("BM_SQUEAK_CACHE_TIME", 300) )
BM_SQUEAK_SINGLE_RATELIMIT = environ.get( "BM_SQUEAK_SINGLE_RATELIMIT", "10/minute" )
A2S_DATA = {
    "ping": {"status": False, "values": {},}
    , "info": {"status": False, "values": {},}
    , "players": {"status": False, "values": {},}
}
A2S_ETAGS = dict.fromkeys( A2S_DATA.keys(), "" )
A2S_ASYNC = (
    lambda cmd, *args, **kwargs:
        getattr(a2s, f"a{cmd}")( *args, mutator_cls=a2s_con.CSOStreamMutator, **kwargs )
)
A2S_SYNC = (
    lambda cmd, *args, **kwargs:
        getattr(a2s, f"{cmd}")( *args, mutator_cls=a2s_con.CSOStreamMutator, **kwargs )
)
get_epoch = lambda: int( time() )
split_hostport = partial(
    map
    , lambda x: x.split(":")
)
app_limiter = Limiter( key_func=get_remote_address, headers_enabled=True )
app = FastAPI()
etag_add_exception_handler( app )
app.state.limiter = app_limiter
logger = getLogger( "uvicorn" )
threads_manager = ThreadPoolManager( BM_SQUEAK_MAX_THREAD, logger )


async def get_etag (request: Request):
    global A2S_DATA
    cmd = request.path_params["command"]
    if cmd not in A2S_DATA:
        return ''
    return A2S_ETAGS[cmd]


@app.on_event( "startup" )
def event_startup_run_update ():
    run_daemon_thread( target=run_update )


@app.get( "/a2s/{command}/{hostname}/{port}" )
@app_limiter.shared_limit( BM_SQUEAK_SINGLE_RATELIMIT, "single" )
async def get_a2s_single (
    command: str
    , hostname: str
    , port: int
    , request: Request
    , response: Response
):
    global A2S_DATA
    if command not in A2S_DATA:
        return response_goaway( response )
    result = {
        "status": True
        , "values": {}
    }
    try:
        result["values"][f"{hostname}:{port}"] = (
            await A2S_ASYNC( command, (hostname,port,) )
        )
    except Exception as exc:
        return response_exc( response, exc )
    return result


_des = [Depends(
    Etag(
        get_etag
        , extra_headers={"Cache-Control": f"public, max-age: {BM_SQUEAK_CACHE_TIME}"},
    )
)]
@app.head( "/a2s/{command}", dependencies=_des )
@app.get( "/a2s/{command}", dependencies=_des )
def get_a2s (command: str, request: Request, response: Response):
    global A2S_DATA
    if command not in A2S_DATA:
        return response_goaway( response )
    cmd_data = A2S_DATA[command]
    result = {
        "status": False
    }
    err = cmd_data["values"].get( "error", None )
    if err:
        exc = response_exc( response, err )
        if request.method == "HEAD":
            return response
        result.update( exc )
        return result
    if not cmd_data["status"]:
        busy = response_busy( response )
        if request.method == "HEAD":
            return response
        result.update( busy )
        return result
    if request.method == "HEAD":
        response.status_code = status.HTTP_200_OK
        return response
    result["status"] = True
    result[command] = cmd_data
    return result


@app.get( "/ping" )
async def get_ping ():
    return "pong!"


@app.get( "/{_:path}" )
async def get_a_tea (_: str, response: Response):
    return response_goaway( response )


@app.exception_handler( RateLimitExceeded )
async def exception_ratelimit (request: Request, exc: RateLimitExceeded):
    response = JSONResponse(
        _exc_dict(exc)
        , status_code=429
    )
    response = request.app.state.limiter._inject_headers(
        response, request.state.view_rate_limit
    )
    return response


def response_exc (response: Response, exc: BaseException):
    if response:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    if isinstance( exc, (TimeoutError, ASyncTimeoutError,) ):
        if response:
            response.status_code = status.HTTP_404_NOT_FOUND
    else:
        logger.critical( exc, exc_info=True )
    return _exc_dict( exc )


def response_busy (response: Response):
    delay = 30
    if response:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        response.headers["Retry-After"] = str(delay)
    return {
        "retry_after": delay
    }


def response_goaway (response: Response):
    if response:
        response.status_code = status.HTTP_418_IM_A_TEAPOT
    return response


def _update_task (cmd: str, address: tuple):
    try:
        return A2S_SYNC( cmd, address )
    except TimeoutError as exc:
        logger.debug( exc, exc_info=True )
def _update ( cmd: str, targets: tuple[tuple[str,int]] ):
    global A2S_DATA, A2S_ETAGS
    subdata = A2S_DATA[cmd]
    subdata["status"] = False
    subdata["values"].clear()
    A2S_ETAGS[cmd] = ""
    workers = [
        threads_manager.add_task( _update_task, cmd, address=(host, port,) )
        for (host,port,)
        in targets
    ]
    while not all( x.finished for x in workers ):
        pass
    for worker in workers:
        result = worker.result
        if isinstance( result, BaseException ):
            subdata["values"] = { "error": result }
            A2S_ETAGS[cmd] = "Error: One or few results are an exception"
            return subdata
    filter_factory = lambda: (filter(
        lambda x: not isinstance( x.result, type(None) )
        , workers
    ))
    items = map(
        lambda address, result: (f"{address[0]}:{address[1]}", result,)
        , (x.kwargs["address"] for x in filter_factory())
        , (x.result for x in filter_factory())
    )
    items = sorted( items )
    subdata["values"] = dict( items )
    A2S_ETAGS[cmd] = md5( pformat(subdata["values"]).encode('utf-8') ).hexdigest()
    subdata["status"] = True
    return subdata


def run_update ():
    global A2S_DATA, A2S_ETAGS
    non_ping = [x for x in A2S_DATA.keys() if x != "ping"]
    while True:
        logger.info( "Start updating..." )
        ue = get_epoch()
        for subdata in A2S_DATA.values():
            subdata["status"] = False
            subdata["values"].clear()
        for subkey in A2S_ETAGS.keys():
            A2S_ETAGS[subkey] = ""
        pings = _update(
            "ping"
            , (
                (host, BM_SQUEAK_CHN_PORT_START + port + (host_id*120),)
                for (host_id, host,)
                in enumerate( BM_SQUEAK_ADDRESS )
                for port
                in range( BM_SQUEAK_CHN_PORT_SIZE )
            )
        )
        pings: dict = pings["values"]
        if "error" in pings:
            for cmd in non_ping:
                A2S_DATA[cmd]["values"] = { "error": pings["error"] }
                A2S_ETAGS[cmd] = A2S_ETAGS["ping"]
        else:
            workers = [
                run_daemon_thread(
                    target=_update
                    , args=(
                        cmd
                        , (
                            (host, int(port))
                            for (host,port,)
                            in split_hostport( pings.keys() )
                        )
                    )
                )
                for cmd
                in non_ping
            ]
            for worker in workers:
                worker.join()
        logger.info( f"Start updating...completed! {get_epoch()-ue}s" )
        sleep( BM_SQUEAK_CACHE_TIME )


def _exc_dict (exc: BaseException):
    detail = str( exc )
    if isinstance( exc, HTTPException ):
        detail = exc.detail
    return {
        "status": False
        , "values": {"error": (type(exc).__name__, detail,)}
    }
