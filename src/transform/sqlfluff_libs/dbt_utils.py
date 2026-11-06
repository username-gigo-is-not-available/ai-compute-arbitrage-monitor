# Stand-ins for the dbt_utils macros the models call, so sqlfluff's jinja templater can render them without dbt.
def generate_surrogate_key(columns):
    return "to_hex(md5(concat(" + ", ".join(f"coalesce(cast({c} as string), '')" for c in columns) + ")))"
