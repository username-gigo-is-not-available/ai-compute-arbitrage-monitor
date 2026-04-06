from enum import StrEnum, auto


class TariffCodeType(StrEnum):
    HT1 = auto()
    HT2 = auto()
    HT3 = auto()
    HT4 = auto()
    LT = auto()
    HT = auto()
    DF = auto()


class ConsumerType(StrEnum):
    HOUSEHOLD = auto()
    BUSINESS = auto()
    SHARED = auto()


class HardwareComponentType(StrEnum):
    CPU = auto()
    GPU = auto()

    @classmethod
    def from_str(cls, value: str):
        if value == "CPU":
            return HardwareComponentType.CPU
        elif value == "GPU":
            return HardwareComponentType.GPU
        else:
            raise ValueError(f"Invalid HardwareComponentType {value}")


class DataStageType(StrEnum):
    BRONZE = auto()
    SILVER = auto()
    GOLD = auto()


class VerificationFlag(StrEnum):
    VERIFIED = "verified"
    UNVERIFIED = "unverified"
    DEVERIFIED = "deverified"


class TariffType(StrEnum):
    HIGH = auto()
    LOW = auto()

class DayType(StrEnum):
    WEEKDAY = auto()
    WEEKEND = auto()

class OfferType(StrEnum):
    ON_DEMAND = auto()
    BID = auto()
    RESERVED = auto()