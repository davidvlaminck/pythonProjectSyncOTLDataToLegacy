from typing import Optional, Sequence

from pydantic import BaseModel

from Domain.Enums import AanleveringStatus, AanleveringSubstatus


class Link(BaseModel):
    href: Optional[str] = None
    rel: Optional[str] = None


class Onderneming(BaseModel):
    naam: str
    ondernemingsnummer: str


class AanleveringInfo(BaseModel):
    bestekOmschrijving: Optional[str] = None
    dossiernummer: Optional[str] = None
    besteknummer: Optional[str] = None
    dienstbevelId: Optional[str] = None
    dienstbevelOmschrijving: Optional[str] = None
    ondernemingInfo: Optional[Onderneming] = None


class Aanlevering(BaseModel):
    id: str
    nummer: str = None
    status: AanleveringStatus = None
    substatus: Optional[AanleveringSubstatus] = None
    info: AanleveringInfo = None


class AanleveringResultaatLinksDict(BaseModel):
    afkeuren: Optional[Link] = None
    annuleren: Optional[Link] = None
    asisaanvragen: Optional[Link] = None
    bijlageopladen: Optional[Link] = None
    finaliseren: Optional[Link] = None
    goedkeuren: Optional[Link] = None
    hoofdbestandopladen: Optional[Link] = None
    self: Link
    wijzigen: Optional[Link] = None
    wijzigenbestanden: Optional[Link] = None


class AanleveringResultaat(BaseModel):
    aanlevering: Aanlevering
    links: AanleveringResultaatLinksDict = None

class ZoekTerm(BaseModel):
    vrijeZoekterm: Optional[str] = None

    def to_dict(self):
        return self.dict()
