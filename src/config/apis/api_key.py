# Checked where the key is used, not when the config is built: setup builds these configs too, without the keys (#113)
def required_api_key(value: str, variable: str) -> str:
    if not value:
        raise RuntimeError(f"{variable} is not set: add it to .env locally, or to Secret Manager on GCP")
    return value
