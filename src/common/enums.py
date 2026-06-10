from enum import StrEnum, auto


class ExecutionType(StrEnum):
    LOCAL = auto()
    GCP = auto()


class DataStageType(StrEnum):
    BRONZE = auto()
    SILVER = auto()
    GOLD = auto()


class DatasetType(StrEnum):
    SOURCES = auto()
    SEEDS = auto()


class DatasetName(StrEnum):
    COMPUTE_OFFERS = auto()
    EXCHANGE_RATES = auto()
    ELECTRICITY_TARIFF_TIERS = auto()
    ELECTRICITY_TARIFF_SCHEDULE = auto()


class VerificationStatusType(StrEnum):
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
