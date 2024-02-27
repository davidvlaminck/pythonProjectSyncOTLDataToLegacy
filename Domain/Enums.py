from enum import Enum


class Environment(Enum):
    PRD = 'prd',
    DEV = 'dev',
    TEI = 'tei',
    AIM = 'aim'


class AuthType(Enum):
    JWT = 'JWT',
    CERT = 'cert'


class Direction(Enum):
    NONE = 'None',
    WITH = 'With',
    REVERSED = 'Reversed'


class AanleveringStatus(str, Enum):
    """De status van de aanlevering. De status is altijd aanwezig."""
    GEANNULEERD = 'GEANNULEERD'
    VERVALLEN = 'VERVALLEN'
    DATA_AANGELEVERD = 'DATA_AANGELEVERD'
    DATA_AANGEVRAAGD = 'DATA_AANGEVRAAGD'
    IN_OPMAAK = 'IN_OPMAAK'


class AanleveringSubstatus(str, Enum):
    """De substatus van de aanlevering. De substatus is optioneel."""
    LOPEND = 'LOPEND'
    GEFAALD = 'GEFAALD'
    BESCHIKBAAR = 'BESCHIKBAAR'
    AANGEBODEN = 'AANGEBODEN'
    GOEDGEKEURD = 'GOEDGEKEURD'
    AFGEKEURD = 'AFGEKEURD'
    OPGESCHORT = 'OPGESCHORT'
