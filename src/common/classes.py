from pydantic import BaseModel

from common.enums import DatasetName, DatasetType


class Dataset(BaseModel):
    dataset_name: DatasetName
    dataset_type: DatasetType

