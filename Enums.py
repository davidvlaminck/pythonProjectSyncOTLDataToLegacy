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
