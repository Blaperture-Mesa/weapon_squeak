from os import environ
from time import sleep, perf_counter
from asyncio import TimeoutError as AsyncTimeoutError
from hashlib import md5
from pprint import pformat
import csotools_serverquery as a2s
import csotools_serverquery.connection as a2s_con
from fastapi import Request, Response, Depends, status
from fastapi.responses import JSONResponse
from fastapi_etag import Etag, add_exception_handler as etag_add_exception_handler
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from item_suit.threadutils import ThreadPoolManager, run_daemon_thread
from item_suit.extra import split_hostport
from item_suit.app import *



BM_SQUEAK_ADDRESS = list( filter(len, (
    x
    for x
    in environ.get("BM_SQUEAK_ADDRESS", "13.228.182.70;54.254.110.182").split(";")
)) )
BM_SQUEAK_PORT_MIN = int( environ.get("BM_SQUEAK_PORT_MIN", 40000) )
BM_SQUEAK_PORT_MAX = int( environ.get("BM_SQUEAK_PORT_MAX", 40300) )
BM_SQUEAK_MAX_THREAD = int( environ.get("BM_SQUEAK_MAX_THREAD", 100) )
BM_SQUEAK_CACHE_TIME = int( environ.get("BM_SQUEAK_CACHE_TIME", 300) )
BM_SQUEAK_SINGLE_RATELIMIT = environ.get( "BM_SQUEAK_SINGLE_RATELIMIT", "10/minute" )
_A2S_DATA_FACTORY = lambda: {"status": False, "values": {},}
A2S_COMMANDS = set([ "ping", "info", "players", "rules", ])
A2S_DATA = {
    cmd: _A2S_DATA_FACTORY()
    for cmd
    in A2S_COMMANDS
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


APP_LIMITER = Limiter( key_func=get_remote_address, headers_enabled=True )
etag_add_exception_handler( APP )
APP.state.limiter = APP_LIMITER
THREADS_MAN = ThreadPoolManager( BM_SQUEAK_MAX_THREAD, LOGGER )


async def get_etag (request: Request):
    cmd = request.path_params["command"]
    if cmd not in A2S_ETAGS:
        return None
    return A2S_ETAGS[cmd]


@APP.get( "/a2s/{command}/{hostname}/{port}" )
@APP_LIMITER.shared_limit( BM_SQUEAK_SINGLE_RATELIMIT, "single" )
async def get_a2s_single (
    command: str
    , hostname: str
    , port: int
    , request: Request
    , response: Response
):
    if command not in A2S_COMMANDS:
        return response_goaway( response )
    result = {
        "status": True
        , command: {
            "values": {}
        }
    }
    try:
        result[command]["values"][f"{hostname}:{port}"] = (
            await A2S_ASYNC( command, (hostname,port,) )
        )
    except Exception as exc:
        return response_exception( response, exc )
    return result


_DEPS = [Depends(
    Etag(
        get_etag
        , extra_headers={"Cache-Control": f"public, max-age: {BM_SQUEAK_CACHE_TIME}"},
    )
)]
@APP.head( "/a2s/{command}", dependencies=_DEPS )
@APP.get( "/a2s/{command}", dependencies=_DEPS )
def get_a2s (command: str, request: Request, response: Response):
    if command not in A2S_DATA:
        return response_goaway( response )
    cmd_data = A2S_DATA[command]
    result = {
        "status": False
    }
    err = cmd_data["values"].get( "error", None )
    if err:
        exc = response_exception( response, err )
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


@APP.exception_handler( RateLimitExceeded )
async def exception_ratelimit (request: Request, exc: RateLimitExceeded):
    response = JSONResponse(
        get_exception_dict(exc)
        , status_code=429
    )
    response = request.app.state.limiter._inject_headers(
        response, request.state.view_rate_limit
    )
    return response


def _update_task (cmd: str, address: tuple):
    try:
        return A2S_SYNC( cmd, address )
    except (TimeoutError, AsyncTimeoutError):
        LOGGER.debug( f"Timeout on {(cmd, address)}", exc_info=False )
    except Exception as exc:
        LOGGER.exception( exc, exc_info=True )
def _update (cmd: str, targets: tuple[tuple[str,int]]):
    subdata = A2S_DATA[cmd]
    subdata["status"] = False
    subdata["values"].clear()
    A2S_ETAGS[cmd] = ""
    workers = [
        THREADS_MAN.add_task(
            _update_task
            , cmd
            , address=(host, port,)
        )
        for (host,port,)
        in targets
    ]
    for worker in workers:
        worker.event.wait()
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
    cmd_update_list = [x for x in A2S_DATA.keys() if x not in ["ping",]]
    while True:
        LOGGER.info( "Start updating..." )
        ue = perf_counter()
        for subdata in A2S_DATA.values():
            subdata["status"] = False
            subdata["values"].clear()
        for subkey in A2S_ETAGS.keys():
            A2S_ETAGS[subkey] = ""
        pings = _update(
            "ping"
            , (
                (host, port,)
                for port
                in range( BM_SQUEAK_PORT_MIN, BM_SQUEAK_PORT_MAX+1 )
                for host
                in BM_SQUEAK_ADDRESS
            )
        )
        pings: dict = pings["values"]
        if "error" in pings:
            for cmd in cmd_update_list:
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
                in cmd_update_list
            ]
            for worker in workers:
                worker.join()
        LOGGER.info( f"Start updating...completed! {perf_counter()-ue}s" )
        sleep( BM_SQUEAK_CACHE_TIME )


@APP.on_event( "startup" )
def event_startup_run_update ():
    run_daemon_thread( target=run_update )


init()
