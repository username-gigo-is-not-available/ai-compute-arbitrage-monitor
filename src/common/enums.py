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
    ELECTRICITY_TARIFF_FEES = auto()
    ELECTRICITY_TARIFF_BLOCKS = auto()
    ELECTRICITY_TARIFF_WINDOW_SCHEDULE = auto()


class VerificationStatusType(StrEnum):
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    DEVERIFIED = "deverified"


class ConsumerCategoryType(StrEnum):
    HOUSEHOLD = auto()
    BUSINESS = auto()

class TariffBlockType(StrEnum):
    HT = auto()
    HT1 = auto()
    HT2 = auto()
    HT3 = auto()
    HT4 = auto()
    LT = auto()

    @classmethod
    def from_str(cls, value: str):
        if value == 'ВТ':
            return TariffBlockType.HT
        elif value == 'НТ':
            return TariffBlockType.LT
        elif value == 'ВТ 1':
            return TariffBlockType.HT1
        elif value == 'ВТ 2':
            return TariffBlockType.HT2
        elif value == 'ВТ 3':
            return TariffBlockType.HT3
        elif value == 'ВТ 4':
            return TariffBlockType.HT4
        else:
            raise ValueError

class TariffWindowType(StrEnum):
    HIGH = auto()
    LOW = auto()


class OfferType(StrEnum):
    ON_DEMAND = auto()
    BID = auto()
    RESERVED = auto()
