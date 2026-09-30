from pydantic import BaseModel


class GCSBucketConfig(BaseModel):
    bucket_name: str
