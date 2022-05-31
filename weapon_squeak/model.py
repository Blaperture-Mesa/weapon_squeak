from typing import Any, Optional
from pydantic import BaseModel, create_model
from csotools_serverquery.common.datacls import DataclsBase
from csotools_serverquery import GoldSrcInfo, PlayerInfo
from item_suit.model import *



def _datacls_to_model (cls):
    if not issubclass( cls, DataclsBase ):
        raise ValueError( f"{cls.__name__} is not a {DataclsBase.__name__} type" )
    members = {
        x: (y, None,)
        for x,y
        in cls.__annotations__.items()
    }
    return create_model( cls.__name__, **members )


def _create_model (name: str, values_cls: type = Any):
    if issubclass( values_cls, DataclsBase ):
        values_cls = _datacls_to_model( values_cls )
    model = create_model(
        name
        , values = (Optional[dict[str, values_cls]], {})
        , __base__ = GenericModel[values_cls]
    )
    def _clear (self):
        tmp = self.values
        if tmp:
            tmp.clear()
        super( GenericModel ).clear()
        self.values = tmp
    model.clear = _clear
    return model


A2SPing = _create_model( "A2SPing", float )
A2SInfo = _create_model( "A2SInfo", GoldSrcInfo )
A2SPlayers = _create_model( "A2SPlayers", list[_datacls_to_model(PlayerInfo)] )
A2SRules = _create_model( "A2SRules", dict[str,str] )


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
        tmp = self.values
        if tmp:
            tmp.clear()
        super().clear()
        self.values = tmp


class A2SCommandsDataOptional (BaseModel):
    ping: Optional[A2SPing]
    info: Optional[A2SInfo]
    players: Optional[A2SPlayers]
    rules: Optional[A2SRules]


class AppCommandsDataOptional (A2SCommandsDataOptional):
    stats: Optional[Stats]


AppCommandsData = create_model(
    "AppCommandsData"
    , ping = A2SPing()
    , info = A2SInfo()
    , players = A2SPlayers()
    , rules = A2SRules()
    , stats = Stats()
    , __base__ = AppCommandsDataOptional
)
