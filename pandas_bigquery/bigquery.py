from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
import os
import logging
from pandas_bigquery.exceptions import *
from pandas_bigquery.datasets import Datasets
from pandas_bigquery.tables import Tables
from pandas_bigquery.tabledata import Tabledata
from pandas_bigquery.jobs import Jobs
from pandas import DataFrame, concat
from pandas.compat import lzip
from datetime import datetime
from random import randint
import numpy as np
from time import sleep

try:
    from googleapiclient.errors import HttpError
except:
    from apiclient.errors import HttpError

log = logging.getLogger()


class Bigquery:
    def __init__(self, project_id=os.getenv('BIGQUERY_PROJECT'), private_key_path=os.getenv('BIGQUERY_KEY_PATH')):

        if private_key_path is None:
            raise RuntimeError('Invalid bigquery key path')

        self.project_id = project_id
        self.private_key_path = private_key_path

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            private_key_path, ['https://www.googleapis.com/auth/bigquery'])

        http_auth = credentials.authorize(Http())
        self._service = build('bigquery', 'v2', http=http_auth)

        with open(private_key_path) as data_file:
            self.private_key = data_file.read()

        self._tables = Tables(self.project_id, private_key=self.private_key_path)
        self._datasets = Datasets(self.project_id, private_key=self.private_key_path)
        self._jobs = Jobs(self.project_id, private_key=self.private_key_path)
        self._tabledata = Tabledata(self.project_id, private_key=self.private_key_path)

    @staticmethod
    def _parse_data(schema, rows):
        # see:
        # http://pandas.pydata.org/pandas-docs/dev/missing_data.html
        # #missing-data-casting-rules-and-indexing
        dtype_map = {'FLOAT': np.dtype(float),
                     'TIMESTAMP': 'M8[ns]'}

        fields = schema['fields']
        col_types = [field['type'] for field in fields]
        col_names = [str(field['name']) for field in fields]
        col_dtypes = [dtype_map.get(field['type'], object) for field in fields]
        page_array = np.zeros((len(rows),), dtype=lzip(col_names, col_dtypes))
        for row_num, raw_row in enumerate(rows):
            entries = raw_row.get('f', [])
            for col_num, field_type in enumerate(col_types):
                field_value = Bigquery._parse_entry(entries[col_num].get('v', ''),
                                                    field_type)
                page_array[row_num][col_num] = field_value

        return DataFrame(page_array, columns=col_names)

    @staticmethod
    def _parse_entry(field_value, field_type):
        if field_value is None or field_value == 'null':
            return None
        if field_type == 'INTEGER':
            return int(field_value)
        elif field_type == 'FLOAT':
            return float(field_value)
        elif field_type == 'TIMESTAMP':
            timestamp = datetime.utcfromtimestamp(float(field_value))
            return np.datetime64(timestamp)
        elif field_type == 'BOOLEAN':
            return field_value == 'true'
        return field_value

    @property
    def datasets(self):
        return self._datasets

    @property
    def tables(self):
        return self._tables

    @property
    def jobs(self):
        return self._jobs

    @property
    def tabledata(self):
        return self._tabledata

    def query_async(self, query, dialect='standard', priority='BATCH', strict=True, **kwargs):

        if Bigquery._check_strict(query, strict):
            raise Exception('Strict mode error',
                            "partition reference not found in query, "
                            "please add a partitiondate, _partitiontime or _table_suffix restriction "
                            "in the where-clause or set strict = False if you are confident in what you're doing.")

        config = kwargs.get('configuration')
        if config is not None and 'query' in config:
            config['query']['priority'] = priority
        else:
            config = {
                'query': {
                    'priority': priority
                }
            }
        config['query']['useLegacySql'] = dialect == 'legacy'

        return self.jobs.query_async(query, configuration=config)

    def query(self, query, dialect='standard', priority='BATCH', strict=True, **kwargs):

        if Bigquery._check_strict(query, strict):
            raise Exception('Strict mode error',
                            "partition reference not found in query, "
                            "please add a partitiondate, _partitiontime or _table_suffix restriction "
                            "in the where-clause or set strict = False if you are confident in what you're doing.")

        if dialect not in ('legacy', 'standard'):
            raise ValueError("'{0}' is not valid for dialect".format(dialect))

        if priority not in ('BATCH', 'INTERACTIVE'):
            raise ValueError("'{0}' is not valid for priority".format(dialect))

        config = kwargs.get('configuration')
        if config is not None and 'query' in config:
            config['query']['priority'] = priority
        else:
            config = {
                'query': {
                    'priority': priority
                }
            }
        config['query']['useLegacySql'] = dialect == 'legacy'

        schema, pages = self.jobs.query(query, configuration=config)
        dataframe_list = []
        while len(pages) > 0:
            page = pages.pop()
            dataframe_list.append(Bigquery._parse_data(schema, page))

        if len(dataframe_list) > 0:
            final_df = concat(dataframe_list, ignore_index=True)
        else:
            final_df = Bigquery._parse_data(schema, [])

        # cast BOOLEAN and INTEGER columns from object to bool/int
        # if they dont have any nulls
        type_map = {'BOOLEAN': bool, 'INTEGER': int}
        for field in schema['fields']:
            if field['type'] in type_map and \
                    final_df[field['name']].notnull().all():
                final_df[field['name']] = \
                    final_df[field['name']].astype(type_map[field['type']])

        self.jobs.print_elapsed_seconds(
            'Total time taken',
            datetime.now().strftime('s.\nFinished at %Y-%m-%d %H:%M:%S.'),
            0
        )

        return final_df

    def upload(self, dataframe, destination_table, if_exists='fail', chunksize=10000):
        if if_exists not in ('fail', 'replace', 'append'):
            raise ValueError("'{0}' is not valid for if_exists".format(if_exists))

        if '.' not in destination_table:
            raise NotFoundException(
                "Invalid Table Name. Should be of the form 'datasetId.tableId' ")

        dataset_id, table_id = destination_table.rsplit('.', 1)

        table_schema = Tables.generate_schema_from_dataframe(dataframe)

        if Tables.contains_partition_decorator(table_id):
            root_table_id, partition_id = table_id.rsplit('$', 1)

            if not self.tables.exists(dataset_id, root_table_id):
                raise NotFoundException("Could not write to the partition because "
                                        "the table does not exist.")

            table_resource = self.tables.get(dataset_id, root_table_id)

            if 'timePartitioning' not in table_resource:
                raise InvalidSchema("Could not write to the partition because "
                                    "the table is not partitioned.")

            partition_exists = self.query("SELECT COUNT(*) AS num_rows FROM {0}"
                                          .format(destination_table),
                                          strict=False,
                                          priority='INTERACTIVE',
                                          dialect='legacy')['num_rows'][0] > 0

            if partition_exists:
                if if_exists == 'fail':
                    raise TableCreationError("Could not create the partition "
                                             "because it already exists. "
                                             "Change the if_exists parameter to "
                                             "append or replace data.")
                elif if_exists == 'append':
                    if not self.tables.schema_is_subset(dataset_id,
                                                        root_table_id,
                                                        table_schema):
                        raise InvalidSchema("Please verify that the structure and "
                                            "data types in the DataFrame match "
                                            "the schema of the destination table.")
                    self.tabledata.insert_all(dataframe, dataset_id, table_id, chunksize)

                elif if_exists == 'replace':
                    if not self.tables.schema_is_subset(dataset_id,
                                                        root_table_id,
                                                        table_schema):
                        raise InvalidSchema("Please verify that the structure and "
                                            "data types in the DataFrame match "
                                            "the schema of the destination table.")

                    temporary_table_id = '_'.join(
                        [root_table_id + '_' + partition_id,
                         str(randint(1, 100000))])
                    self.tables.insert(dataset_id, temporary_table_id, table_schema)
                    self.tabledata.insert_all(dataframe, dataset_id, temporary_table_id,
                                              chunksize)
                    sleep(30)  # <- Curses Google!!!
                    self.jobs.query('select * from {0}.{1}'
                                    .format(dataset_id, temporary_table_id),
                                    configuration={
                                        'query': {
                                            'destinationTable': {
                                                'projectId': self.project_id,
                                                'datasetId': dataset_id,
                                                'tableId': table_id
                                            },
                                            'createDisposition':
                                                'CREATE_IF_NEEDED',
                                            'writeDisposition':
                                                'WRITE_TRUNCATE',
                                            'allowLargeResults': True
                                        }
                                    })
                    self.tables.delete(dataset_id, temporary_table_id)

            else:
                self.tabledata.insert_all(dataframe, dataset_id, table_id, chunksize)

        else:
            if self.tables.exists(dataset_id, table_id):
                if if_exists == 'fail':
                    raise TableCreationError(
                        "Could not create the table because it "
                        "already exists. "
                        "Change the if_exists parameter to "
                        "append or replace data.")
                elif if_exists == 'replace':
                    self.tables.delete_and_recreate_table(
                        dataset_id, table_id, table_schema)
                elif if_exists == 'append':
                    if not self.tables.schema_is_subset(dataset_id,
                                                        table_id,
                                                        table_schema):
                        raise InvalidSchema("Please verify that the structure and "
                                            "data types in the DataFrame match "
                                            "the schema of the destination table.")
            else:
                self.tables.insert(dataset_id, table_id, table_schema)

            self.tabledata.insert_all(dataframe, dataset_id, table_id, chunksize)

    @staticmethod
    def _check_strict(query, strict):
        return strict and \
               not any([x in query.lower() for x in ('partitiondate', '_partitiontime', '_table_suffix', '$')])

    @staticmethod
    def run_with_retry(func, max_tries=10, **kwargs):
        for i in range(0, max_tries):
            try:
                return func(**kwargs), i + 1
            except Exception as err:
                log.warning("run_with_retry error, trying again {0}/{1}".format(i + 1, max_tries))
                if i == max_tries - 1:
                    raise err

    # Legacy methods

    def copy(self, source_dataset, source_table, destination_dataset, destination_table):
        return self.jobs.copy(source_dataset, source_table, destination_dataset, destination_table)

    def generate_schema(self, df, default_type='STRING'):
        return Tables.generate_schema_from_dataframe(df, default_type)

    def schema(self, dataset_id, table_id):
        return self.tables.get_schema(dataset_id, table_id)

    def resource(self, dataset_id, table_id):
        return self.tables.get(dataset_id, table_id)

    def verify_schema(self, dataset_id, table_id, schema):
        return self.tables.schema_matches(dataset_id, table_id, schema)

    def table_copy(self, source_dataset_id, source_table_id, destination_dataset_id, destination_table_id):
        return self.jobs.copy(source_dataset_id, source_table_id, destination_dataset_id, destination_table_id)

    def table_create(self, dataset_id, table_id, schema, **kwargs):
        return self.tables.insert(dataset_id, table_id, schema, **kwargs)

    def table_delete(self, dataset_id, table_id):
        return self.tables.delete(dataset_id, table_id)

    @property
    def dataset_list(self):
        return self.datasets.list()

    def dataset_delete(self, dataset_id, delete_contents=False):
        return self.datasets.delete(dataset_id, delete_contents)

    def query_batch(self, query, dialect='standard', strict=True, **kwargs):
        return self.query(query=query, dialect=dialect, priority='BATCH', strict=strict, **kwargs)
