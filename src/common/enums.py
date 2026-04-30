from enum import StrEnum, auto

class EnvironmentType(StrEnum):
    LOCAL = auto()
    GCP = auto()

class DataStageType(StrEnum):
    BRONZE = auto()
    SILVER = auto()
    GOLD = auto()

class DatasetType(StrEnum):
    SOURCES = auto()
    SEEDS = auto()

class VerificationFlag(StrEnum):
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    DEVERIFIED = "deverified"


class TariffType(StrEnum):
    HIGH = auto()
    LOW = auto()

class OfferType(StrEnum):
    ON_DEMAND = auto()
    BID = auto()
    RESERVED = auto()
