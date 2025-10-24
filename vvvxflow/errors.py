class TableNotInCatalogError(Exception):
    pass


class InconsistentRowCountError(Exception):
    pass


class InconsistentSchemaError(Exception):
    pass


class BucketingError(Exception):
    pass
