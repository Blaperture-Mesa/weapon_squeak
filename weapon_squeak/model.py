from typing import Any, Optional
from enum import Enum
from functools import cache
from pydantic import BaseModel, create_model
from pydantic.types import FutureDate
from csotools_serverquery.common.datacls import DataclsBase
from csotools_serverquery import GoldSrcInfo, PlayerInfo
from item_suit.model import *



A2S_CMD_STATS_NAME = "stats"


def _datacls_to_model (cls):
    if not issubclass( cls, DataclsBase ):
        raise ValueError( f"{cls.__name__} is not a {DataclsBase.__name__} type" )
    members = {
        x: (y, None,)
        for x,y
        in cls.__annotations__.items()
    }
    return create_model( cls.__name__, **members )


def _create_generic_model (name: str, values_cls: type = Any):
    if issubclass( values_cls, DataclsBase ):
        values_cls = _datacls_to_model( values_cls )
    base_cls = GenericModel[values_cls]
    model_cls = create_model(
        name
        , values = (Optional[dict[str, values_cls]], {})
        , __base__ = base_cls
    )
    def _clear (self):
        my_values = self.values
        super( base_cls, self ).clear()
        if my_values: my_values.clear()
        self.values = my_values
    model_cls.clear = _clear
    return model_cls


A2SPing = _create_generic_model( "A2SPing", float )
A2SInfo = _create_generic_model( "A2SInfo", GoldSrcInfo )
A2SPlayers = _create_generic_model( "A2SPlayers", list[_datacls_to_model(PlayerInfo)] )
A2SRules = _create_generic_model( "A2SRules", dict[str,str] )


class StatsNumbers (BaseModel):
    sum: int = 0

    def clear (self):
        self.sum = 0
class StatsGroups (BaseModel):
    server: StatsNumbers = StatsNumbers()
    player: StatsNumbers = StatsNumbers()

    def clear (self):
        self.server.clear()
        self.player.clear()
class StatsValues (StatsGroups):
    map: dict[str, StatsGroups] = {}
    gamemode: dict[str, StatsGroups] = {}

    def clear (self):
        super().clear()
        self.map.clear()
        self.gamemode.clear()
class Stats (GenericModel):
    values: Optional[StatsValues] = StatsValues()

    def clear (self):
        my_values = self.values
        super().clear()
        if my_values: my_values.clear()
        self.values = my_values


class A2SCommandsDataOptional (BaseModel):
    ping: Optional[A2SPing]
    info: Optional[A2SInfo]
    players: Optional[A2SPlayers]
    rules: Optional[A2SRules]

    def clear (self):
        self.ping = None
        self.info = None
        self.players = None
        self.rules = None


class AppCommandsDataOptional (A2SCommandsDataOptional):
    stats: Optional[Stats]

    def clear (self):
        super().clear()
        self.stats = None


class StreamCommandsDataOptional (A2SCommandsDataOptional):
    next_update_time: Optional[FutureDate]
    cache_time: Optional[int]
    etag: Optional[str]

    def clear (self):
        super().clear()
        self.etag = None
        self.next_update_time = None
        self.cache_time = None


AppCommandsData = create_model(
    "AppCommandsData"
    , ping = A2SPing()
    , info = A2SInfo()
    , players = A2SPlayers()
    , rules = A2SRules()
    , stats = Stats()
    , __base__ = AppCommandsDataOptional
)
def _clear (self):
    self.ping.clear()
    self.info.clear()
    self.players.clear()
    self.rules.clear()
    self.stats.clear()
AppCommandsData.clear = _clear


A2S_COMMANDS = tuple[str,...]( dict(A2SCommandsDataOptional()).keys() )
A2SCommands = Enum( "A2SCommands", {x.upper():x.lower() for x in A2S_COMMANDS} )
AppCommands = Enum( "AppCommands", {x.upper():x.lower() for x in dict(AppCommandsDataOptional()).keys()} )


A2S_MODELS: dict[str, BaseModel] = {
    x: globals()[f"A2S{x.capitalize()}"]
    for x
    in A2S_COMMANDS
}
A2S_MODELS[A2S_CMD_STATS_NAME] = Stats


@cache
def create_commands_stream_model (cmd: str):
    kwargs = {
        cmd: A2S_MODELS[cmd]()
    }
    model = create_model(
        "StreamCommandsData"
        , **kwargs
        , __base__ = StreamCommandsDataOptional
    )
    def _clear (self):
        my_subdata = getattr( self, cmd )
        super( type(self), self ).clear()
        if my_subdata: my_subdata.clear()
        setattr( self, cmd, my_subdata )
    model.clear = _clear
    return model
