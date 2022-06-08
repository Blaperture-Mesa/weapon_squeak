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
from item_suit.threadutils import ThreadPoolManager, ThreadWorker
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
APP.add_middleware( BrotliMiddleware, gzip_fallback=False )
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
    subdata: model.GenericModel = getattr( APP_COMMANDS_DATA, command )
    result = { command: {} }
    err: BaseException = getattr( subdata, "error", None )
    if err:
        raise err
    if not subdata.status:
        raise HTTPException( status.HTTP_503_SERVICE_UNAVAILABLE )
    result[command] = subdata
    return result


async def iter_sse_a2s_retrieve_all (request: Request, cmd: str):
    subdata: model.GenericModel = getattr( APP_COMMANDS_DATA, cmd )
    subdata_type = model.A2S_MODELS[cmd]
    result = (model.create_commands_stream_model(cmd))()
    result_subdata: model.GenericModel = getattr( result, cmd )
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


def _update_clear (cmd: str, subdata: model.GenericModel = None):
    if subdata is None:
        subdata = getattr( APP_COMMANDS_DATA, cmd )
    subdata.clear()
    APP_COMMANDS_ETAGS[cmd] = ""
    return subdata


def _update_ok (cmd: str, subdata: model.GenericModel = None):
    if subdata is None:
        subdata = getattr( APP_COMMANDS_DATA, cmd )
    if subdata.error:
        return
    subdata.status = True
    APP_COMMANDS_ETAGS[cmd] = generate_etag( subdata )
    return subdata


def _update_exception (cmd: str, exc: BaseException):
    subdata: model.GenericModel = getattr( APP_COMMANDS_DATA, cmd )
    _update_clear( cmd, subdata )
    subdata.error = exc
    APP_COMMANDS_ETAGS[cmd] = generate_etag( subdata )
    return subdata


def _sort_by_func (obj: dict, func: typing.Callable[[tuple],typing.Any], **kwargs):
    tmp = dict( sorted(obj.items(), key=func, **kwargs) )
    obj.clear()
    obj.update( tmp )
    return obj


T_SERVER_DATA = dict[str,int|model.StatsNumbers]
T_POPULATION = list[int]
T_PLAYER_DATA = dict[str,T_POPULATION|model.StatsNumbers]
def _update_stats () -> model.Stats:
    sd_stats: model.Stats = APP_COMMANDS_DATA.stats
    sb_stats_key = model.A2S_CMD_STATS_NAME
    try:
        sd_rules: dict[str,str] = APP_COMMANDS_DATA.rules.values
        if not sd_rules:
            raise RuntimeWarning( "A2S_RULES is empty" )
        sd_info: dict[str,a2s.GoldSrcInfo] = APP_COMMANDS_DATA.info.values
        og_sd_ping_len = len( APP_COMMANDS_DATA.ping.values )
        og_sd_info_len = len( sd_info )
        og_sd_rules_len = len( sd_rules )
        values = sd_stats.values
        values.missing_from_ping = og_sd_ping_len - og_sd_info_len
        # Try to match the data length.
        sd_info: dict[str,a2s.GoldSrcInfo] = dict(
            filter(
                lambda x: x[0] in sd_rules
                , sd_info.items()
            )
        )
        sd_rules = dict(
            filter(
                lambda x: x[0] in sd_info
                , sd_rules.items()
            )
        )
        filtered_sd_rules_len, filtered_sd_info_len = len( sd_rules ), len( sd_info )
        if filtered_sd_rules_len != filtered_sd_info_len:
            raise RuntimeError(
                "sd_rules_len != sd_info_len"
                , filtered_sd_rules_len
                , filtered_sd_info_len
            )
        values.missing_from_info = og_sd_info_len - filtered_sd_info_len
        values.server = model.create_stats_numbers( filtered_sd_info_len )
        server_data = {
            "map": T_SERVER_DATA()
            , "gamemode": T_SERVER_DATA()
        }
        values_keys = tuple( server_data.keys() )
        player_data = {
            "values": T_POPULATION()
            , "map": T_PLAYER_DATA()
            , "gamemode": T_PLAYER_DATA()
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
                cc_p: T_PLAYER_DATA = player_data[ck]
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
            coll: T_PLAYER_DATA = player_data[ck]
            for cck,v in coll.items():
                v = model.create_stats_numbers( v )
                coll[cck] = v
            _sort_by_func( coll, f )
        for ck in values_keys:
            values_subdata: model.StatsValuesSubdata = getattr( values, ck )
            subdata_groupsmap = values_subdata.values
            players = player_data[ck]
            servers = server_data[ck]
            groups_cls = model.StatsGroups
            gaps = {}
            values_subdata.missing_from_ping = og_sd_ping_len
            values_subdata.missing_from_info = og_sd_info_len
            if ck == "map":
                values_subdata.missing_from_ping -= filtered_sd_info_len
                values_subdata.missing_from_info -= filtered_sd_info_len
            elif ck == "gamemode":
                values_subdata.missing_from_ping -= filtered_sd_rules_len
                values_subdata.missing_from_info -= filtered_sd_rules_len
                values_subdata.missing_from_rules = og_sd_rules_len - filtered_sd_rules_len
            for dk, dv in servers.items():
                subdata_groupsmap[dk] = groups_cls( **gaps, server=dv, player=players[dk] )
        type( sd_stats ).validate( sd_stats )
    except Warning as exc:
        warnings.warn( exc, type(exc) )
        _update_exception( sb_stats_key, exc )
    except BaseException as exc:
        LOGGER.critical( exc, exc_info=True )
        _update_exception( sb_stats_key, exc )
    else:
        _update_ok( sb_stats_key )
    finally:
        reset_update_time()
    return sd_stats


def _update_task_a2s (cmd: str, address: tuple):
    try:
        return A2S_SYNC( cmd, address )
    except (TimeoutError, AsyncTimeoutError):
        LOGGER.debug( f"Timeout on {(cmd, address)}", exc_info=False )
    except BaseException as exc:
        LOGGER.exception( exc, exc_info=True )
        return exc


T_TARGET_INFO = tuple[str,int]
T_TARGETS = tuple[T_TARGET_INFO]
def _update_roundrobin (
    cmds: typing.Iterable[str]
    , targets: T_TARGETS
) -> dict[str, model.GenericModel]:
    cmds = frozenset[str]( cmds )
    if not cmds.issubset( model.A2S_COMMANDS ):
        raise ValueError( f"One of {cmds} is not listed in A2S_COMMANDS" )
    for k in cmds:
        _update_clear( k )
    ping_len = len( APP_COMMANDS_DATA.ping.values )
    sbs = {
        k: list[ThreadWorker]()
        for k
        in cmds
    }
    for target in targets:
        for k in sbs:
            sbs[k].append(
                APP_TPMAN.add_task(
                    _update_task_a2s
                    , k
                    , address=target
                )
            )
    for k,v in sbs.items():
        subdata: model.GenericModel = getattr( APP_COMMANDS_DATA, k )
        subdata_type = model.A2S_MODELS[k]
        try:
            for worker in v:
                worker.event.wait()
                result = worker.result
                if isinstance( result, BaseException ):
                    raise result
            filter_factory = lambda: (
                sorted(
                    filter(
                        lambda x: not isinstance( x.result, type(None) )
                        , v
                    )
                    , key=lambda x: (*x.kwargs["address"], x.result,)
                )
            )
            items = map(
                lambda address, result: (f"{address[0]}:{address[1]}", result,)
                , (x.kwargs["address"] for x in filter_factory())
                , (x.result for x in filter_factory())
            )
            subdata.values.update( items )
            subdata_type.validate( subdata )
        except Warning as exc:
            warnings.warn( exc, type(exc) )
            subdata = _update_exception( k, exc )
        except BaseException as exc:
            LOGGER.critical( exc, exc_info=True )
            subdata = _update_exception( k, exc )
        else:
            if k != model.AppCommands.PING.value:
                subdata.missing_from_ping = ping_len - len( subdata.values )
            _update_ok( k, subdata )
        finally:
            sbs[k] = subdata
            reset_update_time()
    return sbs


def _update_single (cmd: str, targets: T_TARGETS) -> model.GenericModel:
    return _update_roundrobin( [cmd], targets )[cmd]


def run_update ():
    cmds_nonping: frozenset[str] = model.APP_COMMANDS.difference([model.AppCommands.PING.value,])
    cmds_roundrobin: frozenset[str] = cmds_nonping.difference([model.A2S_CMD_STATS_NAME,])
    while True:
        LOGGER.debug( "Start updating..." )
        ue = perf_counter()
        for subkey,subdata in APP_COMMANDS_DATA:
            _update_clear( subkey, subdata )
        pings = _update_single(
            model.AppCommands.PING.value
            , (
                (host, BM_SQUEAK_CHN_PORT_START + port + (host_id*120),)
                for (host_id, host,)
                in enumerate( BM_SQUEAK_ADDRESS )
                for port
                in range( BM_SQUEAK_CHN_PORT_SIZE )
            )
        )
        ping_exc = pings.error
        if ping_exc:
            for cmd in cmds_nonping:
                _update_exception( cmd, ping_exc )
        else:
            _update_roundrobin(
                cmds_roundrobin
                , (
                    (host, int(port))
                    for (host,port,)
                    in split_hostport( pings.values.keys() )
                )
            )
            _update_stats()
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


def run ():
    return init( exc_handler=generic_exc_hander )


if __name__ == "__main__":
    run()
