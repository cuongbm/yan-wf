def guard_not_null(val):
    if not val:
        raise ValueError(f"Value {val} cannot be null")
