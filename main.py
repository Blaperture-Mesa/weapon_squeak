from logging import getLogger
from threading import Thread
from os import environ
from queue import Queue
from time import time, sleep
from datetime import datetime
from hashlib import md5
from pprint import pformat
import csotools_serverquery as a2s
import csotools_serverquery.connection as a2s_con
from fastapi import FastAPI, Request, Response, Depends, status
from fastapi_etag import Etag, add_exception_handler as etag_add_exception_handler


BM_SQUEAK_ADDRESS = [
    x
    for x
    in environ.get("BM_SQUEAK_ADDRESS", "ec2-13-228-182-70.ap-southeast-1.compute.amazonaws.com;ec2-54-254-110-182.ap-southeast-1.compute.amazonaws.com").split(";")
]
BM_SQUEAK_PORT_MIN = int( environ.get("BM_SQUEAK_PORT_MIN", 40000) )
BM_SQUEAK_PORT_MAX = int( environ.get("BM_SQUEAK_PORT_MAX", 40300) )
BM_SQUEAK_MAX_THREAD = int( environ.get("BM_SQUEAK_MAX_THREAD", 100) )
BM_SQUEAK_CACHE_TIME = int( environ.get("BM_SQUEAK_CACHE_TIME", 300) )

data = {
    "ping": {"status": False, "values": {},}
    , "info": {"status": False, "values": {},}
    , "players": {"status": False, "values": {},}
}
data_expired = datetime.now()
q = Queue()
get_epoch = lambda: int( time() )
app = FastAPI()
etag_add_exception_handler( app )
logger = getLogger( "uvicorn" )


@app.on_event( "startup" )
def event_startup_build_threads ():
    def _threader ():
        while True:
            (func, params) = q.get()
            try:
                func( *params )
            except TimeoutError:
                pass
            q.task_done()
    for _ in range( BM_SQUEAK_MAX_THREAD ):
        thread = Thread( target=_threader )
        thread.daemon = True
        thread.start()


@app.on_event( "startup" )
def event_startup_run_update ():
    thread = Thread( target=run_update )
    thread.daemon = True
    thread.start()


async def get_etag (request: Request):
    global data
    cmd = request.path_params["command"]
    if cmd not in data:
        return ''
    return md5( pformat(data[cmd]).encode('utf-8') ).hexdigest()


@app.head( "/a2s/{command}", dependencies=[Depends(
    Etag(
        get_etag
        , extra_headers={"Expires": data_expired.isoformat()},
    )
)] )
@app.get( "/a2s/{command}", dependencies=[Depends(
    Etag(
        get_etag
        , extra_headers={"Expires": data_expired.isoformat()},
    )
)] )
def get_a2s (command: str, request: Request, response: Response):
    global data, data_expired
    if command not in data:
        return response_goaway( response )
    result = {
        "status": True
    }
    if not data[command]["status"]:
        result["status"] = False
        if request.method != "HEAD":
            result.update( response_busy(response) )
        else:
            result = b''
        return result
    if request.method != "HEAD":
        result[command] = dict( sorted(data[command].items()) )
    else:
        result = b''
    return result


@app.get( "/ping" )
async def get_ping ():
    return "pong!"


@app.get( "/{_:path}" )
async def get_a_tea (_: str, response: Response):
    return response_goaway( response )


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


def _pre_process (subdata):
    subdata["status"] = False
    subdata["values"].clear()
def _post_process (data, cmd: str):
    data = data[cmd]
    data["values"] = dict( sorted(data["values"].items()) )
    data["status"] = True


def run_update ():
    global data, data_expired
    while True:
        logger.info( "Start updating..." )
        ue = get_epoch()
        for subdata in data.values():
            _pre_process( subdata )
        for host in BM_SQUEAK_ADDRESS:
            if host:
                for port in range( BM_SQUEAK_PORT_MIN, BM_SQUEAK_PORT_MAX+1 ):
                    q.put( (do_ping, [(host, port)]) )
        q.join()
        _post_process( data, "ping" )
        pings: dict[str,float] = data["ping"]["values"]
        for addr in pings.keys():
            (host, port) = (addr).split( ":" )
            q.put( (do_info, [(host, int(port))]) )
        for addr in pings.keys():
            (host, port) = (addr).split( ":" )
            q.put( (do_players, [(host, int(port))]) )
        q.join()
        _post_process( data, "info" )
        _post_process( data, "players" )
        data_expired = datetime.utcfromtimestamp( get_epoch() + BM_SQUEAK_CACHE_TIME )
        logger.info( f"Start updating...completed! {get_epoch()-ue}s" )
        sleep( BM_SQUEAK_CACHE_TIME )


def do_ping (addr: tuple):
    global data
    (host, port) = addr
    result = a2s.a2a_ping.ping( addr, mutator_cls=a2s_con.CSOStreamMutator )
    data["ping"]["values"][f"{host}:{port}"] = result


def do_info (addr: tuple):
    global data
    (host, port) = addr
    result = a2s.a2s_info.info( addr, mutator_cls=a2s_con.CSOStreamMutator )
    data["info"]["values"][f"{host}:{port}"] = result


def do_players (addr: tuple):
    global data
    (host, port) = addr
    result = a2s.a2s_players.players( addr, mutator_cls=a2s_con.CSOStreamMutator )
    data["players"]["values"][f"{host}:{port}"] = result
