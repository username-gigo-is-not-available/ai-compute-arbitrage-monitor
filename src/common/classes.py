from pydantic import BaseModel, ConfigDict

from common.enums import DatasetName, DatasetType


class Dataset(BaseModel):
    dataset_name: DatasetName
    dataset_type: DatasetType


class AskContractIdRange(BaseModel):
    """Vast.ai offers whose ask_contract_id is in [start, end); no end means no upper bound (ADR-020)."""
    model_config = ConfigDict(frozen=True)

    start: int
    end: int | None = None

    def query(self) -> dict[str, int]:
        return {"gte": self.start} if self.end is None else {"gte": self.start, "lt": self.end}

    def split(self, sample_ids: list[int], parts: int) -> list["AskContractIdRange"]:
        # Cut at the sample's quantiles: a random sample is densest where offers are, so the parts hold similar
        # numbers of offers. Cuts start after the sample's smallest id, so every part is non-empty and narrower.
        ids = sorted(sample_ids)
        cuts = sorted({ids[len(ids) * i // parts] for i in range(1, parts)})
        bounds = [self.start, *cuts, self.end]
        return [AskContractIdRange(start=start, end=end) for start, end in zip(bounds, bounds[1:])]

    def __str__(self) -> str:
        return f"[{self.start}, {self.end})"
