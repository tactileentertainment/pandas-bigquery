import pytest
from datetime import datetime
import pytz
import os
from random import randint
import numpy as np
from pandas.compat import range
from pandas import DataFrame
from pandas_bigquery import Bigquery
from pandas_bigquery import gbq

TABLE_ID = 'new_test'
DATASET_PREFIX_ROOT = 'pandas_bigquery_'


def _skip_if_no_project_id():
    if not _get_project_id():
        pytest.skip(
            "Cannot run integration tests without a project id")


def _skip_if_no_private_key_path():
    if not _get_private_key_path():
        pytest.skip("Cannot run integration tests without a "
                    "private key json file path")


def _get_dataset_prefix_random():
    return ''.join([DATASET_PREFIX_ROOT, str(randint(1, 100000))])


def _get_project_id():
    return os.environ.get('GBQ_PROJECT_ID')


def _get_private_key_path():
    return os.environ.get('GBQ_GOOGLE_APPLICATION_CREDENTIALS')


def _get_private_key_contents():
    key_path = _get_private_key_path()
    if key_path is None:
        return None

    with open(key_path) as f:
        return f.read()


def _test_imports():
    try:
        import pkg_resources  # noqa
    except ImportError:
        raise ImportError('Could not import pkg_resources (setuptools).')

    gbq._test_google_api_imports()


def _setup_common():
    try:
        _test_imports()
    except (ImportError, NotImplementedError) as import_exception:
        pytest.skip(str(import_exception))


def _cleanup():
    bq_dataset = gbq._Dataset(_get_project_id(), private_key=_get_private_key_path())
    datasets = bq_dataset.datasets()
    for dataset_id in datasets:
        if DATASET_PREFIX_ROOT in dataset_id:
            try:
                bq_dataset.delete(dataset_id, delete_contents=True)
            except:
                pass


def setup_module():
    _cleanup()


def teardown_module():
    _cleanup()


def make_mixed_dataframe_v2(test_size):
    # create df to test for all BQ datatypes except RECORD
    bools = np.random.randint(2, size=(1, test_size)).astype(bool)
    flts = np.random.randn(1, test_size)
    ints = np.random.randint(1, 10, size=(1, test_size))
    strs = np.random.randint(1, 10, size=(1, test_size)).astype(str)
    times = [datetime.now(pytz.timezone('US/Arizona'))
             for t in range(test_size)]
    return DataFrame({'bools': bools[0],
                      'flts': flts[0],
                      'ints': ints[0],
                      'strs': strs[0],
                      'times': times[0]},
                     index=range(test_size))


class TestTableOperations(object):
    @classmethod
    def setup_class(cls):
        _skip_if_no_project_id()
        _skip_if_no_private_key_path()

        _setup_common()

    def setup_method(self, method):
        self.bigquery = Bigquery(_get_project_id(), _get_private_key_path())
        self.dataset_prefix = _get_dataset_prefix_random()
        self.destination_table = "{0}.{1}".format(self.dataset_prefix, TABLE_ID)

    @classmethod
    def teardown_class(cls):
        pass

    def teardown_method(self, method):
        pass

    def test_table_create(self):
        test_id = "1"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema)

        assert self.bigquery.verify_schema(self.dataset_prefix, TABLE_ID + test_id, schema)

    def test_verify_schema_case_insensitive(self):
        test_id = "2"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        self.bigquery.upload(df, self.destination_table + test_id)

        assert self.bigquery.verify_schema(self.dataset_prefix, TABLE_ID + test_id, {'fields': [
            {'name': 'Bools', 'type': 'BOOLEAN'},
            {'name': 'flts', 'type': 'FLOAT'},
            {'name': 'ints', 'type': 'INTEGER'},
            {'name': 'strS', 'type': 'STRING'},
            {'name': 'TIMES', 'type': 'TIMESTAMP'},
        ]})

    def test_table_delete(self):
        test_id = "3"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema)

        self.bigquery.table_delete(self.dataset_prefix, TABLE_ID + test_id)

        with pytest.raises(gbq.GenericGBQException):
            self.bigquery.verify_schema(self.dataset_prefix, TABLE_ID + test_id, df)

    def test_upload_data(self):
        test_id = "4"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        self.bigquery.upload(df, self.destination_table + test_id)

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id), strict=False, local_cache=False)
        assert result['num_rows'][0] == test_size

    def test_table_copy(self):
        test_id = "5"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        self.bigquery.upload(df, self.destination_table + test_id)

        self.bigquery.query_batch(
            "SELECT * FROM {0}".format(
                self.destination_table + test_id), strict=False, configuration={
                'query': {
                    'allowLargeResults': True,
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'priority': 'BATCH',
                    'destinationTable': {
                        'projectId': self.bigquery.project_id,
                        'datasetId': self.dataset_prefix,
                        'tableId': TABLE_ID + test_id + '_intermediate'
                    }
                }
            })

        self.bigquery.table_copy(self.dataset_prefix, TABLE_ID + test_id + '_intermediate', self.dataset_prefix,
                                 TABLE_ID + test_id + "_copy")

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id + "_copy"), strict=False, local_cache=False)
        assert result['num_rows'][0] == test_size


class TestPartitionedTableOperations(object):
    @classmethod
    def setup_class(cls):
        _skip_if_no_project_id()
        _skip_if_no_private_key_path()

        _setup_common()

    def setup_method(self, method):
        self.bigquery = Bigquery(_get_project_id(), _get_private_key_path())
        self.dataset_prefix = _get_dataset_prefix_random()
        self.destination_table = "{0}.{1}".format(self.dataset_prefix, TABLE_ID)

    @classmethod
    def teardown_class(cls):
        pass

    def teardown_method(self, method):
        pass

    def test_table_create(self):
        test_id = "1"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema, body={
            'timePartitioning': {'type': 'DAY'}
        })

        actual = self.bigquery.resource(self.dataset_prefix, TABLE_ID + test_id)
        assert actual['timePartitioning']['type'] == 'DAY'

    def test_partition_delete(self):
        test_id = "2"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema, body={
            'timePartitioning': {'type': 'DAY'}
        })

        self.bigquery.upload(df, self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d"))

        self.bigquery.table_delete(self.dataset_prefix, TABLE_ID + test_id + '$' + datetime.today().strftime("%Y%m%d"))

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d")), dialect='legacy',
            local_cache=False)

        assert result['num_rows'][0] == 0

    def test_upload_data_to_partition(self):
        test_id = "3"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema, body={
            'timePartitioning': {'type': 'DAY'}
        })

        self.bigquery.upload(df, self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d"))

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d")), dialect='legacy',
            local_cache=False)

        assert result['num_rows'][0] == test_size

    def test_append_data_to_partition(self):
        test_id = "4"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema, body={
            'timePartitioning': {'type': 'DAY'}
        })

        self.bigquery.upload(df, self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d"))
        self.bigquery.upload(df, self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d"),
                             if_exists='append')

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d")), dialect='legacy',
            local_cache=False)

        assert result['num_rows'][0] == test_size * 2

    def test_replace_data_in_partition(self):
        test_id = "5"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size * 10)
        df2 = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema, body={
            'timePartitioning': {'type': 'DAY'}
        })

        self.bigquery.upload(df, self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d"))
        self.bigquery.upload(df2, self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d"),
                             if_exists='replace')

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id + '$' + datetime.today().strftime("%Y%m%d")), dialect='legacy',
            local_cache=False)

        assert result['num_rows'][0] == test_size


class TestQueries(object):
    @classmethod
    def setup_class(cls):
        _skip_if_no_project_id()
        _skip_if_no_private_key_path()

        _setup_common()

    def setup_method(self, method):
        self.bigquery = Bigquery(_get_project_id(), _get_private_key_path())
        self.dataset_prefix = _get_dataset_prefix_random()
        self.destination_table = "{0}.{1}".format(self.dataset_prefix, TABLE_ID)

    @classmethod
    def teardown_class(cls):
        pass

    def teardown_method(self, method):
        pass

    def test_run_query_interactive(self):
        test_id = "1"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)
        self.bigquery.upload(df, self.destination_table + test_id)

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id), strict=False, local_cache=False)
        assert result['num_rows'][0] == test_size

    def test_run_query_interactive_local_cache(self):
        test_id = "2"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)
        self.bigquery.upload(df, self.destination_table + test_id)

        self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id), strict=False)

        self.bigquery.upload(df, self.destination_table + test_id, if_exists='append')

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id), strict=False)

        assert result['num_rows'][0] == test_size

    def test_run_query_batch(self):
        test_id = "3"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)
        self.bigquery.upload(df, self.destination_table + test_id)

        result = self.bigquery.query_batch(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id), strict=False)
        assert result['num_rows'][0] == test_size

    def test_run_query_async(self):
        test_id = "4"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema)

        result = self.bigquery.query_async(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id), strict=False)
        assert isinstance(result, str)

    def test_run_query_strict(self):
        test_id = "5"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)

        schema = self.bigquery.generate_schema(df)
        self.bigquery.table_create(self.dataset_prefix, TABLE_ID + test_id, schema)

        with pytest.raises(Exception):
            self.bigquery.query_async(
                "SELECT COUNT(*) as num_rows FROM {0}".format(
                    self.destination_table + test_id))

        with pytest.raises(Exception):
            self.bigquery.query(
                "SELECT COUNT(*) as num_rows FROM {0}".format(
                    self.destination_table + test_id), local_cache=False)

    def test_run_query_to_table(self):
        test_id = "6"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)
        self.bigquery.upload(df, self.destination_table + test_id)

        self.bigquery.query_batch(
            "SELECT * FROM {0}".format(
                self.destination_table + test_id), strict=False, configuration={
                'query': {
                    'allowLargeResults': True,
                    'writeDisposition': 'WRITE_TRUNCATE',
                    'priority': 'BATCH',
                    'destinationTable': {
                        'projectId': self.bigquery.project_id,
                        'datasetId': self.dataset_prefix,
                        'tableId': TABLE_ID + test_id + '_copy'
                    }
                }
            })

        result = self.bigquery.query(
            "SELECT COUNT(*) as num_rows FROM {0}".format(
                self.destination_table + test_id + '_copy'), strict=False, local_cache=False)
        assert result['num_rows'][0] == test_size

    def test_run_query_batch_retry_wrong(self):
        test_id = "7"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)
        self.bigquery.upload(df, self.destination_table + test_id)

        with pytest.raises(gbq.GenericGBQException):
            Bigquery.run_with_retry(self.bigquery.query_batch, 10,
                                    query="SELECT COUNT(***) as num_rows FROM {0}".format(
                                        self.destination_table + test_id), strict=False)

    def test_run_query_batch_retry_no_error(self):
        test_id = "8"
        test_size = 10
        df = make_mixed_dataframe_v2(test_size)
        self.bigquery.upload(df, self.destination_table + test_id)

        result, attempts = Bigquery.run_with_retry(self.bigquery.query,
                                                   query="SELECT COUNT(*) as num_rows FROM {0}".format(
                                                       self.destination_table + test_id), strict=False,
                                                   local_cache=False)

        assert result['num_rows'][0] == test_size and attempts == 1
