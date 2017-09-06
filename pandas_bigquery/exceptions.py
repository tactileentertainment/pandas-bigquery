class InvalidPrivateKeyFormat(ValueError):
    """
    Raised when provided private key has invalid format.
    """
    pass


class AccessDenied(ValueError):
    """
    Raised when invalid credentials are provided, or tokens have expired.
    """
    pass


class DatasetCreationError(ValueError):
    """
    Raised when the create dataset method fails
    """
    pass


class GenericGBQException(ValueError):
    """
    Raised when an unrecognized Google API Error occurs.
    """
    pass


class InvalidColumnOrder(ValueError):
    """
    Raised when the provided column order for output
    results DataFrame does not match the schema
    returned by BigQuery.
    """
    pass


class InvalidIndexColumn(ValueError):
    """
    Raised when the provided index column for output
    results DataFrame does not match the schema
    returned by BigQuery.
    """
    pass


class InvalidPageToken(ValueError):
    """
    Raised when Google BigQuery fails to return,
    or returns a duplicate page token.
    """
    pass


class InvalidSchema(ValueError):
    """
    Raised when the provided DataFrame does
    not match the schema of the destination
    table in BigQuery.
    """
    pass


class NotFoundException(ValueError):
    """
    Raised when the project_id, table or dataset provided in the query could
    not be found.
    """
    pass


class QueryTimeout(ValueError):
    """
    Raised when the query request exceeds the timeoutMs value specified in the
    BigQuery configuration.
    """
    pass


class StreamingInsertError(ValueError):
    """
    Raised when BigQuery reports a streaming insert error.
    For more information see `Streaming Data Into BigQuery
    <https://cloud.google.com/bigquery/streaming-data-into-bigquery>`__
    """


class TableCreationError(ValueError):
    """
    Raised when the create table method fails
    """
    pass
