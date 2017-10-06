from pandas_bigquery.exceptions import *
from pandas_bigquery.gbqconnector import GbqConnector


class Jobs(GbqConnector):
    def __init__(self, project_id, reauth=False, verbose=False, private_key=None):
        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError
        self.http_error = HttpError
        super(Jobs, self).__init__(project_id, reauth, verbose, private_key)

    def copy(self, source_dataset_id, source_table_id, destination_dataset_id, destination_table_id, **kwargs):
        """ Run a copy job

        Parameters
        ----------
        source_dataset_id : str
            Name of the source dataset
        source_table_id: str
            Name of the source table
        destination_dataset_id : str
            Name of the destination dataset
        destination_table_id: str
            Name of the destination table
        **kwargs : Arbitrary keyword arguments
            configuration (dict): table creation extra parameters
            For example:

                configuration = {'copy':
                                    {'writeDisposition': 'WRITE_TRUNCATE'}
                                }

            For more information see `BigQuery SQL Reference
            <https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource>`__
        """
        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError
        from google.auth.exceptions import RefreshError

        job_collection = self.service.jobs()

        job_config = {
            'copy': {
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': destination_dataset_id,
                    'tableId': destination_table_id
                },
                'sourceTable': {
                    'projectId': self.project_id,
                    'datasetId': source_dataset_id,
                    'tableId': source_table_id
                }
            }
        }
        config = kwargs.get('configuration')
        if config is not None:
            if len(config) != 1:
                raise ValueError("Only one job type must be specified, but "
                                 "given {}".format(','.join(config.keys())))
            if 'copy' in config:
                if 'destinationTable' in config['copy'] or 'sourceTable' in \
                        config['copy']:
                    raise ValueError("source and destination table must "
                                     "be specified as parameters")

                job_config['copy'].update(config['copy'])
            else:
                raise ValueError("Only 'copy' job type is supported")

        job_data = {
            'configuration': job_config
        }

        self._start_timer()
        try:
            self._print('Requesting copy... ', end="")
            job_reply = job_collection.insert(
                projectId=self.project_id, body=job_data).execute()
            self._print('ok.')
        except (RefreshError, ValueError):
            if self.private_key:
                raise AccessDenied(
                    "The service account credentials are not valid")
            else:
                raise AccessDenied(
                    "The credentials have been revoked or expired, "
                    "please re-run the application to re-authorize")
        except HttpError as ex:
            self.process_http_error(ex)

        job_reference = job_reply['jobReference']
        job_id = job_reference['jobId']
        self._print('Job ID: %s\nCopy running...' % job_id)

        while job_reply['status']['state'] == 'RUNNING':
            self.print_elapsed_seconds('  Elapsed', 's. Waiting...')

            try:
                job_reply = job_collection.get(
                    projectId=job_reference['projectId'],
                    jobId=job_id).execute()
            except HttpError as ex:
                self.process_http_error(ex)

        if self.verbose:
            self._print('Copy completed.')

    def query(self, query, **kwargs):
        """ Run a query job and wait for completion

        Parameters
        ----------
        query : str
            query to be executed
        **kwargs : Arbitrary keyword arguments
            configuration (dict): table creation extra parameters
            For example:

                configuration = {'query':
                                    {'writeDisposition': 'WRITE_TRUNCATE'}
                                }

            For more information see `BigQuery SQL Reference
            <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query>`__
        """

        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError
        from google.auth.exceptions import RefreshError

        job_collection = self.service.jobs()

        job_config = {
            'query': {
                'query': query
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

                if 'destinationTable' in job_config['query']:
                    job_config['query']['allowLargeResults'] = True
            else:
                raise ValueError("Only 'query' job type is supported")

        job_data = {
            'configuration': job_config
        }

        self._start_timer()
        try:
            self._print('Requesting query... ', end="")
            query_reply = job_collection.insert(
                projectId=self.project_id, body=job_data).execute()
            self._print('ok.')
        except (RefreshError, ValueError):
            if self.private_key:
                raise AccessDenied(
                    "The service account credentials are not valid")
            else:
                raise AccessDenied(
                    "The credentials have been revoked or expired, "
                    "please re-run the application to re-authorize")
        except HttpError as ex:
            self.process_http_error(ex)

        job_reference = query_reply['jobReference']
        job_id = job_reference['jobId']
        self._print('Job ID: %s\nQuery running...' % job_id)

        while not query_reply.get('jobComplete', False):
            self.print_elapsed_seconds('  Elapsed', 's. Waiting...')

            timeout_ms = job_config['query'].get('timeoutMs')
            if timeout_ms and timeout_ms < self.get_elapsed_seconds() * 1000:
                raise QueryTimeout('Query timeout: {} ms'.format(timeout_ms))

            try:
                query_reply = job_collection.getQueryResults(
                    projectId=job_reference['projectId'],
                    jobId=job_id).execute()
            except HttpError as ex:
                self.process_http_error(ex)

        if self.verbose:
            if query_reply['cacheHit']:
                self._print('Query done.\nCache hit.\n')
            else:
                bytes_processed = int(query_reply.get(
                    'totalBytesProcessed', '0'))
                self._print('Query done.\nProcessed: {}'.format(
                    self.sizeof_fmt(bytes_processed)))
                self._print('Standard price: ${:,.2f} USD\n'.format(
                    bytes_processed * self.query_price_for_TB))

            self._print('Retrieving results...')

        try:
            total_rows = int(query_reply['totalRows'])
        except KeyError:
            total_rows = 0

        result_pages = list()
        seen_page_tokens = list()
        current_row = 0
        # Only read schema on first page
        schema = query_reply['schema']

        # Loop through each page of data
        while 'rows' in query_reply and current_row < total_rows:
            page = query_reply['rows']
            result_pages.append(page)
            current_row += len(page)

            self.print_elapsed_seconds(
                '  Got page: {}; {}% done. Elapsed'.format(
                    len(result_pages),
                    round(100.0 * current_row / total_rows)))

            if current_row == total_rows:
                break

            page_token = query_reply.get('pageToken', None)

            if not page_token and current_row < total_rows:
                raise InvalidPageToken("Required pageToken was missing. "
                                       "Received {0} of {1} rows"
                                       .format(current_row, total_rows))

            elif page_token in seen_page_tokens:
                raise InvalidPageToken("A duplicate pageToken was returned")

            seen_page_tokens.append(page_token)

            try:
                query_reply = job_collection.getQueryResults(
                    projectId=job_reference['projectId'],
                    jobId=job_id,
                    pageToken=page_token).execute()
            except HttpError as ex:
                self.process_http_error(ex)

        if current_row < total_rows:
            raise InvalidPageToken()

        # print basic query stats
        self._print('Got {} rows.\n'.format(total_rows))

        return schema, result_pages

    def query_async(self, query, **kwargs):
        """ Run a query job and wait for completion

        Parameters
        ----------
        query : str
            query to be executed
        **kwargs : Arbitrary keyword arguments
            configuration (dict): table creation extra parameters
            For example:

                configuration = {'query':
                                    {'writeDisposition': 'WRITE_TRUNCATE'}
                                }

            For more information see `BigQuery SQL Reference
            <https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query>`__
        """

        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError
        from google.auth.exceptions import RefreshError

        job_collection = self.service.jobs()

        job_config = {
            'query': {
                'query': query
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

                if 'destinationTable' in job_config['query']:
                    job_config['query']['allowLargeResults'] = True

            else:
                raise ValueError("Only 'query' job type is supported")

        job_data = {
            'configuration': job_config
        }

        try:
            query_reply = job_collection.insert(
                projectId=self.project_id, body=job_data).execute()
        except (RefreshError, ValueError):
            if self.private_key:
                raise AccessDenied(
                    "The service account credentials are not valid")
            else:
                raise AccessDenied(
                    "The credentials have been revoked or expired, "
                    "please re-run the application to re-authorize")
        except HttpError as ex:
            self.process_http_error(ex)

        return query_reply['jobReference']['jobId']
