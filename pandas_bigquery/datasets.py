from pandas_bigquery.exceptions import *
from pandas_bigquery.gbqconnector import GbqConnector


class Datasets(GbqConnector):
    def __init__(self, project_id, reauth=False, verbose=False,
                 private_key=None):
        try:
            from googleapiclient.errors import HttpError
        except:
            from apiclient.errors import HttpError
        self.http_error = HttpError
        super(Datasets, self).__init__(project_id, reauth, verbose,
                                       private_key)

    def exists(self, dataset_id):
        """ Check if a dataset exists in Google BigQuery

        Parameters
        ----------
        dataset_id : str
            Name of dataset to be verified

        Returns
        -------
        boolean
            true if dataset exists, otherwise false
        """

        try:
            self.service.datasets().get(
                projectId=self.project_id,
                datasetId=dataset_id).execute()
            return True
        except self.http_error as ex:
            if ex.resp.status == 404:
                return False
            else:
                self.process_http_error(ex)

    def list(self):
        """ Return a list of datasets in Google BigQuery

        Parameters
        ----------

        Returns
        -------
        list
            List of datasets under the specific project
        """

        dataset_list = []
        next_page_token = None
        first_query = True

        while first_query or next_page_token:
            first_query = False

            try:
                list_dataset_response = self.service.datasets().list(
                    projectId=self.project_id,
                    pageToken=next_page_token).execute()

                dataset_response = list_dataset_response.get('datasets')
                if dataset_response is None:
                    dataset_response = []

                next_page_token = list_dataset_response.get('nextPageToken')

                if dataset_response is None:
                    dataset_response = []

                for row_num, raw_row in enumerate(dataset_response):
                    dataset_list.append(
                        raw_row['datasetReference']['datasetId'])

            except self.http_error as ex:
                self.process_http_error(ex)

        return dataset_list

    def insert(self, dataset_id):
        """ Create a dataset in Google BigQuery

        Parameters
        ----------
        dataset_id : str
            Name of dataset to be written
        """

        if self.exists(dataset_id):
            raise DatasetCreationError("Dataset {0} already "
                                       "exists".format(dataset_id))

        body = {
            'datasetReference': {
                'projectId': self.project_id,
                'datasetId': dataset_id
            }
        }

        try:
            self.service.datasets().insert(
                projectId=self.project_id,
                body=body).execute()
        except self.http_error as ex:
            self.process_http_error(ex)

    def delete(self, dataset_id, delete_contents=False):
        """ Delete a dataset in Google BigQuery

        Parameters
        ----------
        dataset_id : str
            Name of dataset to be deleted
        delete_contents : boolean
            If True, delete all the tables in the dataset.
            If False and the dataset contains tables,
            the request will fail. Default is False
        """

        if not self.exists(dataset_id):
            raise NotFoundException(
                "Dataset {0} does not exist".format(dataset_id))

        try:
            self.service.datasets().delete(
                datasetId=dataset_id,
                projectId=self.project_id,
                deleteContents=delete_contents).execute()

        except self.http_error as ex:
            # Ignore 404 error which may occur if dataset already deleted
            if ex.resp.status != 404:
                self.process_http_error(ex)
