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


A2SPingModel = _create_model( "A2SPing", float )
A2SInfoModel = _create_model( "A2SInfo", GoldSrcInfo )
A2SPlayersModel = _create_model( "A2SPlayers", list[_datacls_to_model(PlayerInfo)] )
A2SRulesModel = _create_model( "A2SRules", dict[str,str] )


class StatsNumbersModel (BaseModel):
    sum: int = 0

    def clear (self):
        self.sum = 0
class StatsCommonModel (BaseModel):
    server: StatsNumbersModel = StatsNumbersModel()
    player: StatsNumbersModel = StatsNumbersModel()

    def clear (self):
        self.server.clear()
        self.player.clear()
class StatsValuesModel (StatsCommonModel):
    map: dict[str, StatsCommonModel] = {}
    gamemode: dict[str, StatsCommonModel] = {}

    def clear (self):
        super().clear()
        self.map.clear()
        self.gamemode.clear()
class StatsModel (GenericModel):
    values: Optional[StatsValuesModel] = StatsValuesModel()

    def clear (self):
        tmp = self.values
        if tmp:
            tmp.clear()
        super().clear()
        self.values = tmp


class A2SCommandsDataOptional (BaseModel):
    ping: Optional[A2SPingModel]
    info: Optional[A2SInfoModel]
    players: Optional[A2SPlayersModel]
    rules: Optional[A2SRulesModel]


class CommandsDataOptional (A2SCommandsDataOptional):
    stats: Optional[StatsModel]


CommandsData = create_model(
    "CommandsData"
    , ping = A2SPingModel()
    , info = A2SInfoModel()
    , players = A2SPlayersModel()
    , rules = A2SRulesModel()
    , stats = StatsModel()
    , __base__ = CommandsDataOptional
)
