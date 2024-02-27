from typing import Optional, Sequence

from pydantic import BaseModel

from Domain.Enums import AanleveringStatus, AanleveringSubstatus


class Link(BaseModel):
    href: Optional[str] = None
    rel: Optional[str] = None


class Aanlevering(BaseModel):
    links: Optional[Sequence[Link]] = None
    id: str
    isStudie: Optional[bool] = None
    aanvrager: Optional[str] = None
    referentie: Optional[str] = None
    dossierNummer: Optional[str] = None
    besteknummer: Optional[str] = None
    aanleveringnummer: Optional[str] = None
    status: AanleveringStatus
    substatus: Optional[AanleveringSubstatus] = None
    omschrijving: Optional[str] = None


class ZoekTerm(BaseModel):
    vrijeZoekterm: Optional[str] = None

    def to_dict(self):
        return self.dict()
