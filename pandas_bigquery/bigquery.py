from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from httplib2 import Http
import os, hashlib
import pandas as pd
import logging
from pandas_bigquery.gbq import GbqConnector
from pandas_bigquery import gbq

try:
    from googleapiclient.errors import HttpError
except:
    from apiclient.errors import HttpError
from google.auth.exceptions import RefreshError

log = logging.getLogger()


class Bigquery:
    def __init__(self,
                 project_id=os.getenv('BIGQUERY_PROJECT', 'tactile-analytics'),
                 private_key_path=os.getenv('BIGQUERY_KEY_PATH', None),
                 cache_prefix='/tmp/queryresults_'):

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

        self._cache_path = cache_prefix

        self._connector = GbqConnector(project_id, private_key=private_key_path)

    @property
    def _dataset(self):
        return gbq._Dataset(self.project_id,
                            private_key=self.private_key_path)

    def _table(self, dataset_id):
        return gbq._Table(self.project_id, dataset_id,
                          private_key=self.private_key_path)

    def query_async(self, query, dialect='standard', strict=True, **kwargs):

        if Bigquery._check_strict(query, strict):
            raise Exception('Strict mode error',
                            "partition reference not found in query, "
                            "please add a partitiondate, _partitiontime or _table_suffix restriction "
                            "in the where-clause or set strict = False if you are confident in what you're doing.")

        job_collection = self._connector.service.jobs()

        job_config = {
            'query': {
                'query': query,
                'useLegacySql': dialect == 'legacy',
                'priority': 'BATCH'
            }
        }
        config = kwargs.get('configuration')
        if config is not None:
            if len(config) != 1:
                raise ValueError("Only one job type must be specified, but "
                                 "given {}".format(','.join(config.keys())))
            if 'query' in config:
                if 'query' in config['query'] and query is not None:
                    raise ValueError("Query statement can't be specified "
                                     "inside config while it is specified "
                                     "as parameter")

                job_config['query'].update(config['query'])
            else:
                raise ValueError("Only 'query' job type is supported")

        job_data = {
            'configuration': job_config
        }

        try:
            query_reply = job_collection.insert(
                projectId=self.project_id, body=job_data).execute()
        except (RefreshError, ValueError):
            if self._connector.private_key:
                raise gbq.AccessDenied(
                    "The service account credentials are not valid")
            else:
                raise gbq.AccessDenied(
                    "The credentials have been revoked or expired, "
                    "please re-run the application to re-authorize")
        except HttpError as ex:
            self._connector.ess_http_error(ex)

        return query_reply['jobReference']['jobId']

    def query_batch(self, query, dialect='standard', strict=True, **kwargs):

        config = kwargs.get('configuration')
        if config is not None and 'query' in config:
            config['query']['priority'] = 'BATCH'
        else:
            config = {
                'query': {
                    'priority': 'BATCH'
                }
            }

        if Bigquery._check_strict(query, strict):
            raise Exception('Strict mode error',
                            "partition reference not found in query, "
                            "please add a partitiondate, _partitiontime or _table_suffix restriction "
                            "in the where-clause or set strict = False if you are confident in what you're doing.")

        return gbq.read_gbq(query, self.project_id, dialect=dialect, private_key=self.private_key_path,
                            configuration=config)

    def query(self, query, dialect='standard', local_cache=True, strict=True):

        if Bigquery._check_strict(query, strict):
            raise Exception('Strict mode error',
                            "partition reference not found in query, "
                            "please add a partitiondate, _partitiontime or _table_suffix restriction "
                            "in the where-clause or set strict = False if you are confident in what you're doing.")

        querycomment = """/* Query launched from JupyterHub
                User: {user}
                Notebook: {notebook} */"""

        user = os.getenv('USER', 'pytt')

        notebook = os.path.abspath(os.path.curdir)
        commentedquery = "\n".join(
            [querycomment.format(user=user, notebook=notebook), query])

        if local_cache:
            fn = self._cache_path + hashlib.md5(
                self.project_id.encode('ascii') + query.encode(
                    'ascii')).hexdigest() + '.tmp'

            if os.path.exists(fn):
                log.info('Query cached.')
                df = pd.read_pickle(fn)
            else:
                df = gbq.read_gbq(commentedquery, self.project_id, dialect=dialect, private_key=self.private_key_path)
                with open(os.path.splitext(fn)[0] + '.qry', 'w') as f:
                    f.write(query)

                df.to_pickle(fn)
            return df
        else:
            return gbq.read_gbq(commentedquery, self.project_id, dialect=dialect, private_key=self.private_key_path)

    def upload(self, dataframe, destination_table, if_exists='fail'):
        gbq.to_gbq(dataframe, destination_table, project_id=self.project_id, if_exists=if_exists,
                   private_key=self.private_key_path)

    def copy(self, source_dataset, source_table, destination_dataset, destination_table):
        return self._connector.copy(source_dataset, source_table, destination_dataset, destination_table)

    @staticmethod
    def _check_strict(query, strict):
        return strict and \
               not any([x in query.lower() for x in ('partitiondate', '_partitiontime', '_table_suffix', '$')])

    @staticmethod
    def run_with_retry(function, max_tries=10, **kwargs):
        for i in range(0, max_tries):
            try:
                return function(**kwargs), i + 1
            except Exception as err:
                log.warning("eun_with_retry error, trying again {0}/{1}".format(i + 1, max_tries))
                if i == max_tries - 1:
                    raise err

    def generate_schema(self, df, default_type='STRING'):
        return gbq._generate_bq_schema(df, default_type)

    def schema(self, dataset_id, table_id):
        return self._connector.schema(dataset_id, table_id)

    def resource(self, dataset_id, table_id):
        return self._connector.resource(dataset_id, table_id)

    def verify_schema(self, dataset_id, table_id, schema):
        return self._connector.verify_schema(dataset_id, table_id, schema)

    def table_copy(self, source_dataset_id, source_table_id, destination_dataset_id, destination_table_id):
        return self._connector.copy(source_dataset_id, source_table_id, destination_dataset_id, destination_table_id)

    def table_create(self, dataset_id, table_id, schema, **kwargs):
        return self._table(dataset_id).create(table_id, schema, **kwargs)

    def table_delete(self, dataset_id, table_id):
        return self._table(dataset_id).delete(table_id)

    @property
    def dataset_list(self):
        return self._dataset.datasets()

    def dataset_delete(self, dataset_id, delete_contents=False):
        return self._dataset.delete(dataset_id, delete_contents)
