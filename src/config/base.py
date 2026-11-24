from pydantic import BaseModel, ConfigDict


# For models built from a whole settings.yaml block: a misspelled key fails instead of being dropped (#125)
class SettingsBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")
