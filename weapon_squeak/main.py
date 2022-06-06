import warnings
import typing
import json
from os import environ
from time import sleep, perf_counter
from datetime import datetime, timezone, timedelta
from threading import Event, Lock
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



BM_SQUEAK_TW_ADDRESS = environ.get( "BM_SQUEAK_TW_ADDRESS", "112.121.65.{:d}" )
BM_SQUEAK_TW_ADDRESS_MIN = max(  1, int(environ.get("BM_SQUEAK_TW_ADDRESS_MIN",  1)) )
BM_SQUEAK_TW_ADDRESS_MAX = min( 99, int(environ.get("BM_SQUEAK_TW_ADDRESS_MAX", 97)) )
BM_SQUEAK_TW_PORT = environ.get( "BM_SQUEAK_TW_PORT", "4{:02d}{:02d}" )
BM_SQUEAK_TW_PORT_MIN = int( environ.get("BM_SQUEAK_TW_PORT_MIN", 0) )
BM_SQUEAK_TW_PORT_MAX = min( 99, int(environ.get("BM_SQUEAK_TW_PORT_MAX", 20)) )
BM_SQUEAK_CACHE_TIME = max( 2, int(environ.get("BM_SQUEAK_CACHE_TIME", 300)) )
BM_SQUEAK_SINGLE_RATELIMIT = environ.get( "BM_SQUEAK_SINGLE_RATELIMIT", "10/minute" )
BM_SQUEAK_MAX_THREAD = max( len(model.A2S_COMMANDS)+2, int(environ.get("BM_SQUEAK_MAX_THREAD", 100)) )


A2S_ASYNC = (
    lambda cmd, *args, **kwargs:
        getattr(a2s, f"a{cmd}")( *args, mutator_cls=a2s_con.CSOStreamMutator, **kwargs )
)
A2S_SYNC = (
    lambda cmd, *args, **kwargs:
        getattr(a2s, f"{cmd}")( *args, mutator_cls=a2s_con.CSOStreamMutator, **kwargs )
)


APP_COMMANDS_DATA = model.AppCommandsData()
APP_COMMANDS_ETAGS = dict.fromkeys( dict(APP_COMMANDS_DATA).keys(), "" )
APP_COMMANDS_UPDATE_LOCK = Lock()
APP_COMMANDS_UPDATE_TIME = datetime.now( timezone.utc )
APP_SSE_STATE = Event()
APP_LIMITER = Limiter( key_func=get_remote_address, headers_enabled=True )
etag_add_exception_handler( APP )
APP.state.limiter = APP_LIMITER
APP.add_middleware( BrotliMiddleware )
APP_TPMAN = ThreadPoolManager( BM_SQUEAK_MAX_THREAD, LOGGER )


def generate_etag (obj):
    return md5( pformat(obj).encode('utf-8') ).hexdigest()
async def get_etag (request: Request):
    cmd = request.path_params["command"]
    if cmd not in APP_COMMANDS_ETAGS:
        return None
    return APP_COMMANDS_ETAGS[cmd]


def reset_update_time ():
    global APP_COMMANDS_UPDATE_TIME
    with APP_COMMANDS_UPDATE_LOCK:
        APP_COMMANDS_UPDATE_TIME = (
            datetime.now( timezone.utc )
            + timedelta( seconds=BM_SQUEAK_CACHE_TIME )
        )
    return APP_COMMANDS_UPDATE_TIME


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
    subdata: model.GenericModel[model.Any] = getattr( APP_COMMANDS_DATA, command )
    result = { command: {} }
    err: BaseException = getattr( subdata, "error", None )
    if err:
        raise err
    if not subdata.status:
        raise HTTPException( status.HTTP_503_SERVICE_UNAVAILABLE )
    result[command] = subdata
    return result


async def iter_sse_a2s_retrieve_all (request: Request, cmd: str):
    subdata: model.GenericModel[model.Any] = getattr( APP_COMMANDS_DATA, cmd )
    subdata_type = model.A2S_MODELS[cmd]
    result = (model.create_commands_stream_model(cmd))()
    result_subdata: model.GenericModel[model.Any] = getattr( result, cmd )
    result.clear()
    while not APP_SSE_STATE.is_set():
        if await request.is_disconnected():
            break
        etag = APP_COMMANDS_ETAGS[cmd]
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
            result.next_update_time = APP_COMMANDS_UPDATE_TIME
            result.cache_time = BM_SQUEAK_CACHE_TIME
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
    APP_COMMANDS_ETAGS[subkey] = ""
    subdata.values.clear()


def _update_exception (cmd: str, exc: BaseException):
    subdata: model.GenericModel[model.Any] = getattr( APP_COMMANDS_DATA, cmd )
    _update_clear( cmd, subdata )
    subdata.error = exc
    APP_COMMANDS_ETAGS[cmd] = generate_etag( subdata )
    return subdata


def _update_task_a2s (cmd: str, address: tuple):
    try:
        return A2S_SYNC( cmd, address )
    except (TimeoutError, AsyncTimeoutError):
        LOGGER.debug( f"Timeout on {(cmd, address)}", exc_info=False )
    except BaseException as exc:
        LOGGER.exception( exc, exc_info=True )
        return exc


def _sort_by_func (obj: dict, func: typing.Callable[[tuple],typing.Any], **kwargs):
    tmp = dict( sorted(obj.items(), key=func, **kwargs) )
    obj.clear()
    obj.update( tmp )
    return obj


t_server_data = dict[str,int|model.StatsNumbers]
t_population = list[int]
t_player_data = dict[str,t_population|model.StatsNumbers]
def _update_stats (subdata: model.Stats):
    sd_rules: dict[str,str] = APP_COMMANDS_DATA.rules.values
    if not sd_rules:
        warnings.warn( "update_stats: A2S_RULES is empty", RuntimeWarning )
        return subdata
    # Try to match the data length.
    sd_info: dict[str,a2s.GoldSrcInfo] = dict(
        filter(
            lambda x: x[0] in sd_rules
            , APP_COMMANDS_DATA.info.values.items()
        )
    )
    sd_rules = dict(
        filter(
            lambda x: x[0] in sd_info
            , sd_rules.items()
        )
    )
    sd_rules_len, sd_info_len = len( sd_rules ), len( sd_info )
    if sd_rules_len != sd_info_len:
        raise RuntimeError( "sd_rules_len %i != %i sd_info_len", sd_rules_len, sd_info_len )
    values = subdata.values
    values.server = model.create_stats_numbers( len(sd_info) )
    server_data = {
        "map": t_server_data()
        , "gamemode": t_server_data()
    }
    values_keys = tuple( server_data.keys() )
    player_data = {
        "values": t_population()
        , "map": t_player_data()
        , "gamemode": t_player_data()
    }
    for addr, info in sd_info.items():
        player_sum = info.player_count - info.bot_count
        player_data["values"].append( player_sum )
        for cck,ck in zip(
            [info.map_name, str(sd_rules[addr]["mp_gamemode"])]
            , values_keys
            , strict=True
        ):
            cc_s = server_data[ck]
            cc_s.setdefault( cck, 0 )
            cc_s[cck] += 1
            cc_p: t_player_data = player_data[ck]
            cc_p_c = cc_p.setdefault( cck, [] )
            cc_p_c.append( player_sum )
    values.player = model.create_stats_numbers( player_data["values"] )
    server_sorter = {
        "map": (lambda x: (x[0].lower(), x[1],))
        , "gamemode": (lambda x: (int(x[0]), x[1],))
    }
    for ck,f in server_sorter.items():
        coll = _sort_by_func( server_data[ck], f )
        for cck,v in coll.items():
            v = model.create_stats_numbers( v )
            coll[cck] = v
    player_sorter = {
        "map": (lambda x: (x[0].lower(), x[1].sum,))
        , "gamemode": (lambda x: (int(x[0]), x[1].sum,))
    }
    for ck,f in player_sorter.items():
        coll: t_player_data = player_data[ck]
        for cck,v in coll.items():
            v = model.create_stats_numbers( v )
            coll[cck] = v
        _sort_by_func( coll, f )
    for ck in values_keys:
        values_groupsmap: dict = getattr( values, ck )
        players = player_data[ck]
        servers = server_data[ck]
        for dk, dv in servers.items():
            values_groupsmap[dk] = model.StatsGroups( server=dv, player=players[dk] )
    return subdata


def _update (cmd: str, targets: tuple[tuple[str,int]]):
    subdata: model.GenericModel[model.Any] = getattr( APP_COMMANDS_DATA, cmd )
    _update_clear( cmd, subdata )
    if cmd == model.A2S_CMD_STATS_NAME:
        # Run this after all A2S queries are done.
        try:
            _update_stats( subdata )
        except BaseException as exc:
            LOGGER.critical( exc, exc_info=True )
            reset_update_time()
            return _update_exception( cmd, exc )
    else:
        workers = [
            APP_TPMAN.add_task(
                _update_task_a2s
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
                LOGGER.critical( exc, exc_info=True )
                reset_update_time()
                return _update_exception( cmd, result )
        filter_factory = lambda: (
            sorted(
                filter(
                    lambda x: not isinstance( x.result, type(None) )
                    , workers
                )
                , key=lambda x: (*x.kwargs["address"], x.result,)
            )
        )
        items = map(
            lambda address, result: (f"{address[0]}:{address[1]}", result,)
            , (x.kwargs["address"] for x in filter_factory())
            , (x.result for x in filter_factory())
        )
        subdata.values = dict( items )
    APP_COMMANDS_ETAGS[cmd] = generate_etag( subdata )
    subdata.status = True
    reset_update_time()
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
        LOGGER.debug( "Start updating..." )
        ue = perf_counter()
        for subkey,subdata in APP_COMMANDS_DATA:
            _update_clear( subkey, subdata )
        pings = _update(
            "ping"
            , (
                (BM_SQUEAK_TW_ADDRESS.format(host), int(BM_SQUEAK_TW_PORT.format(host,port)),)
                for port
                in range( BM_SQUEAK_TW_PORT_MIN, BM_SQUEAK_TW_PORT_MAX+1 )
                for host
                in range( BM_SQUEAK_TW_ADDRESS_MIN, BM_SQUEAK_TW_ADDRESS_MAX+1 )
            )
        )
        if pings.error:
            for cmd in cmd_list:
                getattr(APP_COMMANDS_DATA, cmd).error = pings.error
                APP_COMMANDS_ETAGS[cmd] = APP_COMMANDS_ETAGS["ping"]
        else:
            workers = [
                APP_TPMAN.add_task(
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
    APP_TPMAN.add_task( run_update )


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
