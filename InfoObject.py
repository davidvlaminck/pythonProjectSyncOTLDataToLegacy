import abc

directional_relations = {
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftAanvullendeGeometrie",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#VoedtAangestuurd",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBeheer",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsInspectieVan",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#SluitAanOp",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Voedt",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftNetwerkProtectie",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HoortBij",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Omhult",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsNetwerkECC",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsAdmOnderdeelVan",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftBetrokkene",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftNetwerktoegang",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#HeeftToegangsprocedure",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#LigtOp",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsSWOnderdeelVan",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#IsSWGehostOp",
}
nondiretional_relations = {
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Bevestiging",
    "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Sturing"
}


def is_relation(short_type: str) -> bool:
    return short_type in directional_relations or short_type in nondiretional_relations


class InfoObject(abc.ABC):
    @abc.abstractmethod
    def __init__(self, uuid: str, short_type: str, attr_dict: dict, active: bool = True):
        self.uuid: str = uuid
        self.short_type: str = short_type
        self.active: bool = active
        self.attr_dict: dict = attr_dict
        self.is_relation: bool
        self.is_directional_relation: bool


class NodeInfoObject(InfoObject):
    def __init__(self, uuid: str, short_type: str, attr_dict: dict, active: bool = True):
        super().__init__(uuid, short_type, attr_dict, active)
        self.is_relation: bool = False
        self.is_directional_relation: bool = False


class RelationInfoObject(InfoObject):
    def __init__(self, uuid: str, short_type: str, attr_dict: dict, active: bool = True):
        super().__init__(uuid, short_type, attr_dict, active)
        self.is_relation: bool = True
        self.is_directional_relation: bool = (self.is_relation and short_type not in nondiretional_relations)
