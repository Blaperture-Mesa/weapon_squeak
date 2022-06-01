import json
from os import environ
from time import sleep, perf_counter
from datetime import datetime, timezone, timedelta
from threading import Event
from asyncio import TimeoutError as AsyncTimeoutError
from hashlib import md5
from pprint import pformat
import csotools_serverquery as a2s
import csotools_serverquery.connection as a2s_con
from sse_starlette.sse import EventSourceResponse
from fastapi import Request, Response, Depends, status
from fastapi.exceptions import HTTPException
from fastapi_etag import Etag, add_exception_handler as etag_add_exception_handler
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from brotli_asgi import BrotliMiddleware
from item_suit.threadutils import ThreadPoolManager
from item_suit.extra import split_hostport
from item_suit.app import *
from . import model



BM_SQUEAK_ADDRESS = list( filter(len, (
    x
    for x
    in environ.get("BM_SQUEAK_ADDRESS", "121.14.44.33;121.14.44.34;121.14.44.35;121.14.44.36;121.14.44.37;121.14.44.38;121.14.44.39;121.14.44.40;121.14.44.41;121.14.44.42;121.14.44.43;121.14.44.44;121.14.44.45;121.14.44.46;121.14.44.47;121.14.44.48;121.14.44.49;121.14.44.50;121.14.44.51;121.14.44.52;121.14.44.53;121.14.44.54;121.14.44.55;121.14.44.56;121.14.44.57;121.14.44.58;121.14.44.59;121.14.44.60;121.14.44.61;121.14.44.62;121.14.44.63;121.14.44.64;121.14.44.65;121.14.44.66;121.14.44.67").split(";")
)) )
BM_SQUEAK_CHN_PORT_START = int( environ.get("BM_SQUEAK_CHN_PORT_START", 40000) )
BM_SQUEAK_CHN_PORT_SIZE = int( environ.get("BM_SQUEAK_CHN_PORT_SIZE", 120) )
BM_SQUEAK_CACHE_TIME = max( 2, int(environ.get("BM_SQUEAK_CACHE_TIME", 300)) )
BM_SQUEAK_SINGLE_RATELIMIT = environ.get( "BM_SQUEAK_SINGLE_RATELIMIT", "10/minute" )
BM_SQUEAK_MAX_THREAD = max( len(model.A2S_COMMANDS)+2, int(environ.get("BM_SQUEAK_MAX_THREAD", 100)) )
COMMANDS_DATA = model.AppCommandsData()
COMMANDS_ETAGS = dict.fromkeys( dict(COMMANDS_DATA).keys(), "" )
A2S_ASYNC = (
    lambda cmd, *args, **kwargs:
        getattr(a2s, f"a{cmd}")( *args, mutator_cls=a2s_con.CSOStreamMutator, **kwargs )
)
A2S_SYNC = (
    lambda cmd, *args, **kwargs:
        getattr(a2s, f"{cmd}")( *args, mutator_cls=a2s_con.CSOStreamMutator, **kwargs )
)


APP_SSE_STATE = Event()
APP_LIMITER = Limiter( key_func=get_remote_address, headers_enabled=True )
etag_add_exception_handler( APP )
APP.state.limiter = APP_LIMITER
APP.add_middleware( BrotliMiddleware )
THREADS_MAN = ThreadPoolManager( BM_SQUEAK_MAX_THREAD, LOGGER )


def generate_etag (obj):
    return md5( pformat(obj).encode('utf-8') ).hexdigest()
async def get_etag (request: Request):
    cmd = request.path_params["command"]
    if cmd not in COMMANDS_ETAGS:
        return None
    return COMMANDS_ETAGS[cmd]


@APP.get(
    "/a2s/{command}/{hostname}/{port}"
    , response_model=model.AppCommandsDataOptional
    , response_model_exclude_none=True
)
@APP_LIMITER.shared_limit( BM_SQUEAK_SINGLE_RATELIMIT, "single" )
async def a2s_retrieve_single (
    command: model.A2SCommands
    , hostname: str
    , port: int
    , request: Request
    , response: Response
):
    command: str = command.value
    result = {
        command: {
            "status": True
            , "values": {}
        }
    }
    result[command]["values"][f"{hostname}:{port}"] = (
        await A2S_ASYNC( command, (hostname,port,) )
    )
    return result


_KWARGS = {
    "path": "/a2s/{command}"
    , "dependencies": [Depends(
        Etag(
            get_etag
            , extra_headers={
                "Cache-Control": f"public, max-age: {BM_SQUEAK_CACHE_TIME}"
            },
        )
    )]
    , "response_model": model.AppCommandsDataOptional
    , "response_model_exclude_none": True
}
@APP.head( **_KWARGS )
@APP.get( **_KWARGS )
def a2s_retrieve_all (command: model.AppCommands, request: Request, response: Response):
    command: str = command.value
    subdata: model.GenericModel[model.Any] = getattr( COMMANDS_DATA, command )
    result = { command: {} }
    err: BaseException = getattr( subdata, "error", None )
    if err:
        raise err
    if not subdata.status:
        raise HTTPException( status.HTTP_503_SERVICE_UNAVAILABLE )
    result[command] = subdata
    return result


async def iter_sse_a2s_retrieve_all (request: Request, cmd: str):
    subdata: model.GenericModel[model.Any] = getattr( COMMANDS_DATA, cmd )
    subdata_type = model.A2S_MODELS[cmd]
    result = (model.create_commands_stream_model(cmd))()
    result_subdata: model.GenericModel[model.Any] = getattr( result, cmd )
    result.clear()
    while not APP_SSE_STATE.is_set():
        if await request.is_disconnected():
            break
        etag = COMMANDS_ETAGS[cmd]
        if result.etag == etag:
            continue
        result.clear()
        result.etag = etag
        exc = subdata.error
        if exc:
            result_subdata = subdata_type.parse_obj( response_exception(None, exc, False) )
        elif not subdata.status:
            result_subdata = subdata_type.parse_obj( response_busy(None) )
        else:
            result_subdata = subdata_type.parse_obj( subdata )
            result.next_update = datetime.now(timezone.utc) + timedelta(seconds=BM_SQUEAK_CACHE_TIME)
        setattr( result, cmd, result_subdata )
        yield result.json(
            exclude_none=True,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        )


@APP.get(
    f"{_KWARGS['path']}/subscribe"
    , response_model=model.StreamCommandsDataOptional
    , response_model_exclude_none=True
)
async def sse_a2s_retrieve_all (request: Request, command: model.AppCommands):
    return EventSourceResponse( iter_sse_a2s_retrieve_all(request, command.value) )


@APP.on_event( "shutdown" )
def event_shutdown_stop_sse ():
    APP_SSE_STATE.set()


def _update_clear (subkey: str, subdata: model.GenericModel):
    subdata.status = False
    COMMANDS_ETAGS[subkey] = ""
    subdata.values.clear()
def _update_exception (cmd: str, exc: BaseException):
    subdata: model.GenericModel[model.Any] = getattr( COMMANDS_DATA, cmd )
    _update_clear( subdata )
    subdata.error = exc
    COMMANDS_ETAGS[cmd] = generate_etag( subdata )
    return subdata
def _update_task (cmd: str, address: tuple):
    try:
        return A2S_SYNC( cmd, address )
    except (TimeoutError, AsyncTimeoutError):
        LOGGER.debug( f"Timeout on {(cmd, address)}", exc_info=False )
    except BaseException as exc:
        LOGGER.exception( exc, exc_info=True )
        return exc
def _update (cmd: str, targets: tuple[tuple[str,int]]):
    subdata: model.GenericModel[model.Any] = getattr( COMMANDS_DATA, cmd )
    _update_clear( cmd, subdata )
    if cmd == model.A2S_CMD_STATS_NAME:
        # Run this after all A2S queries are done.
        try:
            c_subdata: model.StatsValues = subdata.values
            c_subdata.server.sum = len( COMMANDS_DATA.ping.values )
            c_player = c_subdata.player
            c_player.sum = 0
            c_map = c_subdata.map
            c_gamemode = c_subdata.gamemode
            rules = COMMANDS_DATA.rules.values
            for addr,info in COMMANDS_DATA.info.values.items():
                if addr not in rules:
                    continue
                info: a2s.GoldSrcInfo = info
                player_sum = info.player_count - info.bot_count
                c_player.sum += player_sum
                for k,c in zip(
                    [info.map_name, str(rules[addr]["mp_gamemode"])]
                    , [c_map, c_gamemode,]
                    , strict=True
                ):
                    cc = c.get( k, model.StatsGroups() )
                    cc.server.sum += 1
                    cc.player.sum += player_sum
                    c[k] = cc
            for c in [c_map, c_gamemode]:
                tmp = dict( sorted(c.items()) )
                c.clear()
                c.update( tmp )
        except BaseException as exc:
            return _update_exception( cmd, exc )
    else:
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
                return _update_exception( cmd, result )
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
        subdata.values = dict( items )
    COMMANDS_ETAGS[cmd] = generate_etag( subdata )
    subdata.status = True
    return subdata


def run_update ():
    cmd_list = tuple(
        x.value
        for x
        in model.AppCommands.__members__.values()
        if x not in [model.AppCommands.PING,]
    )
    a2s_list = tuple( filter(lambda x:x not in [model.A2S_CMD_STATS_NAME,], cmd_list) )
    while True:
        LOGGER.info( "Start updating..." )
        ue = perf_counter()
        for subkey,subdata in COMMANDS_DATA:
            _update_clear( subkey, subdata )
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
        if pings.error:
            for cmd in cmd_list:
                getattr(COMMANDS_DATA, cmd).error = pings.error
                COMMANDS_ETAGS[cmd] = COMMANDS_ETAGS["ping"]
        else:
            workers = [
                THREADS_MAN.add_task(
                    _update
                    , cmd
                    , (
                        (host, int(port))
                        for (host,port,)
                        in split_hostport( pings.values.keys() )
                    )
                )
                for cmd
                in a2s_list
            ]
            for worker in workers:
                worker.event.wait()
            _update( model.A2S_CMD_STATS_NAME, None )
        LOGGER.info( f"Start updating...completed! {perf_counter()-ue}s" )
        sleep( BM_SQUEAK_CACHE_TIME )


@APP.on_event( "startup" )
def event_startup_run_update ():
    THREADS_MAN.add_task( run_update )


# Add our stuffs into exception handler.
async def generic_exc_hander (request: Request, exc: Exception):
    response = await handler_exception_async( request, exc )
    # Inject rate-limit headers, if any
    if hasattr( request.state, "view_rate_limit" ):
        response = request.app.state.limiter._inject_headers(
            response, request.state.view_rate_limit
        )
    # Embed root path, if any
    paths = request.path_params
    if "command" in paths:
        body: dict = json.loads( response.body.decode("utf-8") )
        body = { paths["command"]: body }
        response = replace_json_body( response, body )
    return response


@APP.exception_handler( RateLimitExceeded )
async def exception_ratelimit (request: Request, exc: RateLimitExceeded):
    response = await generic_exc_hander( request, exc )
    response.status_code = status.HTTP_429_TOO_MANY_REQUESTS
    return response


init( exc_handler=generic_exc_hander )
