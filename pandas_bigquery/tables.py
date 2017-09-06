from pandas_bigquery.exceptions import *
from pandas_bigquery.gbqconnector import GbqConnector
from pandas_bigquery.datasets import Datasets
from time import sleep


class Tables(GbqConnector):
    def __init__(self, project_id, reauth=False, verbose=False, private_key=None):
        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError
        self.http_error = HttpError
        super(Tables, self).__init__(project_id, reauth, verbose, private_key)

    def insert(self, dataset_id, table_id, schema, **kwargs):
        """ Create a table in Google BigQuery given a table and schema

        Parameters
        ----------
        dataset_id : str
            Dataset name of table to be written
        table_id : str
            Name of table to be written
        schema : str
            Use the generate_schema_from_dataframe to generate
             your table schema from a dataframe.

        **kwargs : Arbitrary keyword arguments
            body (dict): table creation extra parameters
            For example:

                body = {'timePartitioning': {'type': 'DAY'}}

            For more information see `BigQuery SQL Reference
            <https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource>`__
        """

        if self.exists(dataset_id, table_id):
            raise TableCreationError("Table {0} already "
                                     "exists".format(table_id))

        if not Datasets(self.project_id,
                        private_key=self.private_key).exists(dataset_id):
            Datasets(self.project_id,
                     private_key=self.private_key).insert(dataset_id)

        body = {
            'schema': schema,
            'tableReference': {
                'tableId': table_id,
                'projectId': self.project_id,
                'datasetId': dataset_id
            }
        }

        config = kwargs.get('body')
        if config is not None:
            body.update(config)

        try:
            self.service.tables().insert(
                projectId=self.project_id,
                datasetId=dataset_id,
                body=body).execute()
        except self.http_error as ex:
            self.process_http_error(ex)

    def delete(self, dataset_id, table_id):
        """ Delete a table in Google BigQuery

        Parameters
        ----------
        dataset_id : str
            Name of dataset containing the table to be deleted

        table_id : str
            Name of table to be deleted
        """

        if not self.exists(dataset_id, table_id):
            raise NotFoundException("Table does not exist")

        try:
            self.service.tables().delete(
                datasetId=dataset_id,
                projectId=self.project_id,
                tableId=table_id).execute()
        except self.http_error as ex:
            # Ignore 404 error which may occur if table already deleted
            if ex.resp.status != 404:
                self.process_http_error(ex)

    def list(self, dataset_id):
        """ List tables in the specific dataset in Google BigQuery

        Parameters
        ----------
        dataset_id : str
            Name of dataset to list tables for

        Returns
        -------
        list
            List of tables under the specific dataset
        """

        table_list = []
        next_page_token = None
        first_query = True

        while first_query or next_page_token:
            first_query = False

            try:
                list_table_response = self.service.tables().list(
                    projectId=self.project_id,
                    datasetId=dataset_id,
                    pageToken=next_page_token).execute()

                table_response = list_table_response.get('tables')
                next_page_token = list_table_response.get('nextPageToken')

                if not table_response:
                    return table_list

                for row_num, raw_row in enumerate(table_response):
                    table_list.append(raw_row['tableReference']['tableId'])

            except self.http_error as ex:
                self.process_http_error(ex)

        return table_list

    def get(self, dataset_id, table_id):
        """Retrieve the resource describing a table

        Obtain from BigQuery complete description
        for the table defined by the parameters

        Parameters
        ----------
        dataset_id : str
            Name of the BigQuery dataset for the table
        table_id : str
            Name of the BigQuery table

        Returns
        -------
        object
            Table resource
        """

        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError

        try:
            return self.service.tables().get(
                projectId=self.project_id,
                datasetId=dataset_id,
                tableId=table_id).execute()
        except HttpError as ex:
            self.process_http_error(ex)

    def get_schema(self, dataset_id, table_id):
        """Retrieve the schema of the table

        Obtain from BigQuery the field names and field types
        for the table defined by the parameters

        Parameters
        ----------
        dataset_id : str
            Name of the BigQuery dataset for the table
        table_id : str
            Name of the BigQuery table

        Returns
        -------
        list of dicts
            Fields representing the schema
        """

        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError

        try:
            remote_schema = self.service.tables().get(
                projectId=self.project_id,
                datasetId=dataset_id,
                tableId=table_id).execute()['schema']

            remote_fields = [{'name': field_remote['name'],
                              'type': field_remote['type']}
                             for field_remote in remote_schema['fields']]

            return remote_fields
        except HttpError as ex:
            self.process_http_error(ex)

    def schema_matches(self, dataset_id, table_id, schema):
        """Indicate whether schemas match exactly

        Compare the BigQuery table identified in the parameters with
        the schema passed in and indicate whether all fields in the former
        are present in the latter. Order is not considered.

        Parameters
        ----------
        dataset_id :str
            Name of the BigQuery dataset for the table
        table_id : str
            Name of the BigQuery table
        schema : list(dict)
            Schema for comparison. Each item should have
            a 'name' and a 'type'

        Returns
        -------
        bool
            Whether the schemas match
        """

        fields_remote = sorted([{'name': f['name'].lower(), 'type': f['type']}
                                for f in self.get_schema(dataset_id, table_id)],
                               key=lambda x: x['name'])
        fields_local = sorted([{'name': f['name'].lower(), 'type': f['type']}
                               for f in schema['fields']],
                              key=lambda x: x['name'])

        return fields_remote == fields_local

    def schema_is_subset(self, dataset_id, table_id, schema):
        """Indicate whether the schema to be uploaded is a subset

        Compare the BigQuery table identified in the parameters with
        the schema passed in and indicate whether a subset of the fields in
        the former are present in the latter. Order is not considered.

        Parameters
        ----------
        dataset_id : str
            Name of the BigQuery dataset for the table
        table_id : str
            Name of the BigQuery table
        schema : list(dict)
            Schema for comparison. Each item should have
            a 'name' and a 'type'

        Returns
        -------
        bool
            Whether the passed schema is a subset
        """

        fields_remote = [{'name': f['name'].lower(), 'type': f['type']}
                         for f in self.get_schema(dataset_id, table_id)]
        fields_local = [{'name': f['name'].lower(), 'type': f['type']}
                        for f in schema['fields']]

        return all(field in fields_remote for field in fields_local)

    @staticmethod
    def generate_schema_from_dataframe(df, default_type='STRING'):
        """ Given a passed df, generate the associated Google BigQuery schema.

        Parameters
        ----------
        df : DataFrame
        default_type : string
            The default big query type in case the type of the column
            does not exist in the schema.
        """

        type_mapping = {
            'i': 'INTEGER',
            'b': 'BOOLEAN',
            'f': 'FLOAT',
            'O': 'STRING',
            'S': 'STRING',
            'U': 'STRING',
            'M': 'TIMESTAMP'
        }

        fields = []
        for column_name, dtype in df.dtypes.iteritems():
            fields.append({'name': column_name,
                           'type': type_mapping.get(dtype.kind, default_type)})

        return {'fields': fields}

    def exists(self, dataset_id, table_id):
        """ Check if a table exists in Google BigQuery

        Parameters
        ----------
        dataset_id : str
            Dataset name of table to be verified

        table_id : str
            Name of table to be verified

        Returns
        -------
        boolean
            true if table exists, otherwise false
        """

        if Tables.contains_partition_decorator(table_id):
            root_table_id, partition_id = table_id.rsplit('$', 1)
        else:
            root_table_id = table_id

        try:
            self.service.tables().get(
                projectId=self.project_id,
                datasetId=dataset_id,
                tableId=root_table_id).execute()
            return True
        except self.http_error as ex:
            if ex.resp.status == 404:
                return False
            else:
                self.process_http_error(ex)

    def delete_and_recreate_table(self, dataset_id, table_id, table_schema):
        delay = 0

        # Changes to table schema may take up to 2 minutes as of May 2015 See
        # `Issue 191
        # <https://code.google.com/p/google-bigquery/issues/detail?id=191>`__
        # Compare previous schema with new schema to determine if there should
        # be a 120 second delay

        if not self.schema_matches(dataset_id, table_id, table_schema):
            self._print('The existing table has a different schema. Please '
                        'wait 2 minutes. See Google BigQuery issue #191')
            delay = 120

        table = Tables(self.project_id, private_key=self.private_key)
        table.delete(dataset_id, table_id)
        table.insert(dataset_id, table_id, table_schema)
        sleep(delay)

    @staticmethod
    def contains_partition_decorator(table_id):
        return "$" in table_id
