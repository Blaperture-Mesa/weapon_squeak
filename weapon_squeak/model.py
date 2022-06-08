from typing import Any, Optional, Collection
from enum import Enum
from functools import cache
from statistics import multimode
import numpy as np
from pydantic import create_model
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


@cache
def _clear_preserve_values (base: type):
    def method (self):
        my_values = self.values
        base.clear( self )
        if my_values: my_values.clear()
        self.values = my_values
        return self
    return method


class MissingPingOptional (BaseClearableModel):
    missing_from_ping: Optional[int]

    def clear (self):
        self.missing_from_ping = None
        return self


class MissingInfoOptional (BaseClearableModel):
    missing_from_info: Optional[int]

    def clear (self):
        self.missing_from_info = None
        return self


class MissingRulesOptional (BaseClearableModel):
    missing_from_rules: Optional[int]

    def clear (self):
        self.missing_from_rules = None
        return self


def _create_generic_model (name: str, values_cls: type = Any):
    if issubclass( values_cls, DataclsBase ):
        values_cls = _datacls_to_model( values_cls )
    base_cls = GenericModel[values_cls]
    base_cls_clear = _clear_preserve_values( base_cls )
    def _method_clear (self):
        MissingPingOptional.clear( self )
        return base_cls_clear( self )
    model_cls = create_model(
        name
        , values=(Optional[dict[str, values_cls]], {})
        , __base__= type(
            f"{name}WithMissingPing"
            , (GenericModel[values_cls], MissingPingOptional)
            , {
                "clear": _method_clear
            }
        )
    )
    return model_cls


A2SPing = _create_generic_model( "A2SPing", float )
A2SInfo = _create_generic_model( "A2SInfo", GoldSrcInfo )
A2SPlayers = _create_generic_model( "A2SPlayers", list[_datacls_to_model(PlayerInfo)] )
A2SRules = _create_generic_model( "A2SRules", dict[str,str] )


class StatsNumbers (BaseClearableModel):
    len: Optional[int] = None
    sum: Optional[int] = None
    min: Optional[int] = None
    max: Optional[int] = None
    mode: Optional[list[int]] = None
    median: Optional[float] = None
    mean: Optional[float] = None
    var: Optional[float] = None
    std: Optional[float] = None

    def clear (self):
        self.len =\
        self.sum =\
        self.min =\
        self.max =\
        self.mode =\
        self.median =\
        self.mean =\
        self.var =\
        self.std = None
        return self


def create_stats_numbers (data: Collection[int]|int):
    model_cls = StatsNumbers
    if not isinstance( data, int ):
        count = len( data )
        data_ndarray = np.fromiter( data, int, count=count )
        return model_cls(
            len=count
            , sum=data_ndarray.sum()
            , min=data_ndarray.min()
            , max=data_ndarray.max()
            , mode=multimode( data_ndarray )
            , median=np.median( data_ndarray )
            , mean=data_ndarray.mean( dtype=float )
            , var=data_ndarray.var( dtype=float )
            , std=np.std( data_ndarray, dtype=float )
        )
    return model_cls(
        sum=data
    )


class StatsGroups (BaseClearableModel):
    server: StatsNumbers = StatsNumbers()
    player: StatsNumbers = StatsNumbers()

    def clear (self):
        self.server.clear()
        self.player.clear()
        return self


class StatsValuesSubdata (MissingInfoOptional, MissingPingOptional):
    values: dict[str, StatsGroups] = {}

    def clear (self):
        MissingPingOptional.clear( self )
        MissingInfoOptional.clear( self )
        self.values.clear()
        return self


class StatsMap (StatsValuesSubdata):
    pass


class StatsGamemode (StatsValuesSubdata, MissingRulesOptional):
    def clear (self):
        MissingRulesOptional.clear( self )
        return StatsValuesSubdata.clear( self )


class StatsValues (StatsGroups, MissingInfoOptional, MissingPingOptional):
    map: StatsMap = StatsMap()
    gamemode: StatsGamemode = StatsGamemode()

    def clear (self):
        StatsGroups.clear( self )
        MissingPingOptional.clear( self )
        MissingInfoOptional.clear( self )
        self.map.clear()
        self.gamemode.clear()
        return self


class Stats (GenericModel):
    values: Optional[StatsValues] = StatsValues()
Stats.clear = _clear_preserve_values( GenericModel )


class A2SCommandsDataOptional (BaseClearableModel):
    ping: Optional[A2SPing]
    info: Optional[A2SInfo]
    players: Optional[A2SPlayers]
    rules: Optional[A2SRules]

    def clear (self):
        self.ping = None
        self.info = None
        self.players = None
        self.rules = None
        return self


class AppCommandsDataOptional (A2SCommandsDataOptional):
    stats: Optional[Stats]

    def clear (self):
        super().clear()
        self.stats = None
        return self


class StreamCommandsDataOptional (AppCommandsDataOptional):
    next_update_time: Optional[FutureDate]
    cache_time: Optional[int]
    etag: Optional[str]

    def clear (self):
        super().clear()
        self.etag = None
        self.next_update_time = None
        self.cache_time = None
        return self


def _appcmddata_clear (self):
    self.ping.clear()
    self.info.clear()
    self.players.clear()
    self.rules.clear()
    self.stats.clear()
    return self


AppCommandsData = create_model(
    "AppCommandsData"
    , ping=A2SPing()
    , info=A2SInfo()
    , players=A2SPlayers()
    , rules=A2SRules()
    , stats=Stats()
    , __base__=AppCommandsDataOptional
)
AppCommandsData.clear = _appcmddata_clear


A2S_COMMANDS = frozenset[str]( dict(A2SCommandsDataOptional()).keys() )
A2SCommands = Enum( "A2SCommands", {x.upper():x.lower() for x in A2S_COMMANDS} )
APP_COMMANDS = frozenset[str]( dict(AppCommandsDataOptional()).keys() )
AppCommands = Enum( "AppCommands", {x.upper():x.lower() for x in APP_COMMANDS} )


A2S_MODELS: dict[str, BaseClearableModel] = {
    x: globals()[f"A2S{x.capitalize()}"]
    for x
    in A2S_COMMANDS
}
A2S_MODELS[A2S_CMD_STATS_NAME] = Stats
