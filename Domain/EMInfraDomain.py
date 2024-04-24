from datetime import datetime
from typing import Optional, Sequence, Union, List

from pydantic import BaseModel, Field

from Domain.Enums import Toestand, Bron, Formaat, Precisie


class Link(BaseModel):
    href: Optional[str] = None
    rel: Optional[str] = None


class ResourceRefDTO(BaseModel):
    links: Optional[Sequence[Link]] = None
    uuid: Optional[str] = None


class EventContextDTO(ResourceRefDTO):
    links: Optional[Sequence[Link]] = None
    uuid: Optional[str] = None
    omschrijving: Optional[str] = None
    createdOn: Optional[datetime] = None
    modifiedOn: Optional[datetime] = None


class AggregateIdObject(BaseModel):
    _type: Optional[str] = None
    uuid: Optional[str] = None


class AtomValueObject(BaseModel):
    type: str = Field(alias='_type')
    typeVersion: str = Field(alias='_typeVersion')
    contextId: Optional[str] = None
    from_: Optional[object] = Field(alias='from')
    to: Optional[object] = None
    aggregateId: Optional[AggregateIdObject] = None


class ContentObject(BaseModel):
    type: Optional[str] = None
    value: Optional[AtomValueObject] = None


class EntryObject(BaseModel):
    content: Optional[ContentObject]
    id: Optional[str]
    links: Optional[Sequence[Link]] = None
    updated: Optional[datetime] = None


class FeedProxyContentValue(BaseModel):
    event_type: Optional[str] = Field(alias='event-type')
    asset_type: Optional[str] = Field(alias='asset-type')
    event_id: Optional[str] = Field(alias='event-id')
    context_id: Optional[str] = Field(alias='context-id', default=None)

    uuids: Optional[Sequence[str]] = None
    aim_ids: Optional[Sequence[str]] = Field(alias='aim-ids')


class FeedProxyContent(BaseModel):
    value: FeedProxyContentValue


class ProxyEntryObject(BaseModel):
    id: str
    type: Optional[str] = Field(alias='_type')
    updated: datetime
    content: Optional[FeedProxyContent] = None


class FeedProxyPage(BaseModel):
    id: str
    base: Optional[str] = None
    title: Optional[str] = None
    updated: Optional[datetime] = None
    links: Optional[Sequence[Link]] = None
    entries: Optional[Sequence[ProxyEntryObject]] = None


class FeedPage(BaseModel):
    id: str
    base: Optional[str] = None
    title: Optional[str] = None
    updated: Optional[datetime] = None
    links: Optional[Sequence[Link]] = None
    entries: Optional[Sequence[EntryObject]] = None


class EigenschapTypedValueDTO(BaseModel):
    type: Optional[str] = Field(..., alias='_type')
    value: Optional[Union[object, Sequence[object]]] = None


class KenmerkEigenschapValueUpdateDTO(BaseModel):
    eigenschap: ResourceRefDTO = None
    kenmerkType: ResourceRefDTO = None
    typedValue: EigenschapTypedValueDTO = None


class DatatypeTypeDTO(BaseModel):
    type: Optional[str] = Field(..., alias='_type')


class EigenschapTypeDTO(BaseModel):
    actief: Optional[bool] = None
    links: Optional[Sequence[Link]] = None
    uuid: Optional[str] = None
    kardinaliteitMin: Optional[int] = None
    kardinaliteitMax: Optional[int] = None
    uri: Optional[str] = None
    definitie: Optional[str] = None
    naam: Optional[str] = None
    type: Optional[DatatypeTypeDTO] = None


class EigenschapTypeDTOType(BaseModel):
    datatype: Optional[EigenschapTypeDTO] = None
    type: Optional[str] = Field(..., alias='_type')


class EigenschapDTO(BaseModel):
    actief: Optional[bool] = None
    links: Optional[Sequence[Link]] = None
    uuid: Optional[str] = None
    kardinaliteitMin: Optional[int] = None
    kardinaliteitMax: Optional[int] = None
    uri: Optional[str] = None
    naam: Optional[str] = None
    type: Optional[EigenschapTypeDTOType] = None


class KenmerkTypeDTO(ResourceRefDTO):
    actief: Optional[bool] = None
    createdOn: Optional[str] = None
    modifiedOn: Optional[str] = None
    definitie: Optional[str] = None
    korteUri: Optional[str] = None
    predefined: Optional[bool] = None
    naam: Optional[str] = None
    standard: Optional[bool] = None
    uri: Optional[str] = None


class KenmerkEigenschapValueDTO(BaseModel):
    eigenschap: EigenschapDTO = None
    kenmerkType: KenmerkTypeDTO = None
    typedValue: EigenschapTypedValueDTO = None


class KenmerkEigenschapValueDTOList(BaseModel):
    data: Optional[List[KenmerkEigenschapValueDTO]] = None


class ListUpdateDTOKenmerkEigenschapValueUpdateDTO(BaseModel):
    data: Optional[Sequence[KenmerkEigenschapValueUpdateDTO]] = None


class EigenschapDTOList(BaseModel):
    data: Optional[Sequence[EigenschapDTO]] = None


class InfraObjectDTO(BaseModel):
    links: Optional[Sequence[Link]] = None
    uuid: Optional[str] = None
    naam: Optional[str] = None
    createdOn: Optional[str] = None
    modifiedOn: Optional[str] = None


class AssetTypeDTO(ResourceRefDTO):
    actief: Optional[bool] = None
    afkorting: Optional[str] = None
    createdOn: Optional[str] = None
    modifiedOn: Optional[str] = None
    definitie: Optional[str] = None
    korteUri: Optional[str] = None
    label: Optional[str] = None
    naam: Optional[str] = None
    uri: Optional[str] = None


class AssetDTO(InfraObjectDTO):
    commentaar: Optional[str] = None
    toestand: Optional[Toestand] = None
    actief: Optional[bool] = None
    # kenmerken
    type: Optional[AssetTypeDTO] = None


class InstallatieDTO(AssetDTO):
    parent: Optional[InfraObjectDTO] = None


class InstallatieUpdateDTO(BaseModel):
    actief: bool = None
    commentaar: Optional[str] = None
    naam: Optional[str] = None
    toestand: Toestand = None


class AssetRefDTO(ResourceRefDTO):
    pass


class LocatieRelatieDTO(BaseModel):
    asset: AssetDTO = None


class LocatieRelatieUpdateDTO(BaseModel):
    asset: AssetRefDTO = None


class CoordinatenDTO(BaseModel):
    formaat: Formaat = None
    x: float = None
    y: float = None
    z: float = None


class AdresDTO(BaseModel):
    coordinaten: Optional[CoordinatenDTO] = None
    gemeente: Optional[str] = None
    nummer: Optional[str] = None
    bus: Optional[str] = None
    label: Optional[str] = None
    postcode: Optional[str] = None
    provincie: Optional[str] = None
    straat: Optional[str] = None


class WeglocatieDTO(BaseModel):
    coordinaten: Optional[CoordinatenDTO] = None
    gemeente: Optional[str] = None
    ident2: Optional[str] = None
    ident8: Optional[str] = None
    straatnaam: Optional[str] = None
    referentiepaalAfstand: Optional[float] = None
    referentiepaalOpschrift: Optional[float] = None


class WegsegmentPuntLocatieDTO(BaseModel):
    projectie: Optional[CoordinatenDTO] = None
    afstand: Optional[float] = None
    gidn: Optional[str] = None
    oidn: Optional[int] = None
    uidn: Optional[str] = None
    opschrift: Optional[str] = None
    verwijderd: Optional[bool] = None
    wegnummer: Optional[str] = None


class LocatieDTO(BaseModel):
    adres: Optional[AdresDTO] = None
    bron: Optional[Bron] = None
    coordinaten: Optional[CoordinatenDTO] = None
    geometrie: Optional[dict] = None  # incorrect shortcut
    hernieuwdOp: Optional[str] = None
    precisie: Optional[Precisie] = None
    weglocatie: Optional[WeglocatieDTO] = None
    wegsegmentPuntLocatie: Optional[WegsegmentPuntLocatieDTO] = None
    type: Optional[str] = Field(..., alias='_type')


class LocatieKenmerkUpdateLocatieDTO(BaseModel):
    locatie: Optional[LocatieDTO] = None
    relatie: Optional[LocatieRelatieUpdateDTO] = None


class LocatieKenmerkDTO(BaseModel):
    eigenschapWaarden: Optional[KenmerkEigenschapValueDTOList] = None
    locatie: Optional[LocatieDTO] = None
    relatie: Optional[LocatieRelatieDTO] = None
    geometrie: Optional[str] = None
    links: Optional[Sequence[Link]] = None
    omschrijving: Optional[str] = None
    type: Optional[KenmerkTypeDTO] = None

