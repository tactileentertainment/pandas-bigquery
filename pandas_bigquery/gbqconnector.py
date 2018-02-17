import json
import time
import sys

from distutils.version import StrictVersion
from pandas import compat
from pandas.compat import bytes_to_str
from pandas_bigquery.exceptions import *


def _check_google_client_version():
    try:
        import pkg_resources

    except ImportError:
        raise ImportError('Could not import pkg_resources (setuptools).')

    # Version 1.6.0 is the first version to support google-auth.
    # https://github.com/google/google-api-python-client/blob/master/CHANGELOG
    google_api_minimum_version = '1.6.0'

    _GOOGLE_API_CLIENT_VERSION = pkg_resources.get_distribution(
        'google-api-python-client').version

    if (StrictVersion(_GOOGLE_API_CLIENT_VERSION) <
            StrictVersion(google_api_minimum_version)):
        raise ImportError('pandas requires google-api-python-client >= {0} '
                          'for Google BigQuery support, '
                          'current version {1}'
                          .format(google_api_minimum_version,
                                  _GOOGLE_API_CLIENT_VERSION))


def _test_google_api_imports():
    try:
        import httplib2  # noqa
    except ImportError as ex:
        raise ImportError(
            'pandas requires httplib2 for Google BigQuery support: '
            '{0}'.format(ex))

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow  # noqa
    except ImportError as ex:
        raise ImportError(
            'pandas requires google-auth-oauthlib for Google BigQuery '
            'support: {0}'.format(ex))

    try:
        from google_auth_httplib2 import AuthorizedHttp  # noqa
        from google_auth_httplib2 import Request  # noqa
    except ImportError as ex:
        raise ImportError(
            'pandas requires google-auth-httplib2 for Google BigQuery '
            'support: {0}'.format(ex))

    try:
        from googleapiclient.discovery import build  # noqa
        from googleapiclient.errors import HttpError  # noqa
    except ImportError as ex:
        raise ImportError(
            "pandas requires google-api-python-client for Google BigQuery "
            "support: {0}".format(ex))

    try:
        import google.auth  # noqa
    except ImportError as ex:
        raise ImportError(
            "pandas requires google-auth for Google BigQuery support: "
            "{0}".format(ex))

    _check_google_client_version()


def _try_credentials(project_id, credentials):
    import httplib2
    from googleapiclient.discovery import build
    import googleapiclient.errors
    from google_auth_httplib2 import AuthorizedHttp

    if credentials is None:
        return None

    http = httplib2.Http()
    try:
        authed_http = AuthorizedHttp(credentials, http=http)
        bigquery_service = build('bigquery', 'v2', http=authed_http)
        # Check if the application has rights to the BigQuery project
        jobs = bigquery_service.jobs()
        job_data = {'configuration': {'query': {'query': 'SELECT 1'}}}
        jobs.insert(projectId=project_id, body=job_data).execute()
        return credentials
    except googleapiclient.errors.Error:
        return None


class GbqConnector(object):
    # Added scopes to support federated tables in Google Drive
    scope = ['https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive']

    def __init__(self, project_id, reauth=False, verbose=False,
                 private_key=None, auth_local_webserver=False):
        self.project_id = project_id
        self.reauth = reauth
        self.verbose = verbose
        self.private_key = private_key
        self.auth_local_webserver = auth_local_webserver
        self.credentials = self.get_credentials()
        self.service = self.get_service()

        # BQ Queries costs $5 per TB. First 1 TB per month is free
        # see here for more: https://cloud.google.com/bigquery/pricing
        self.query_price_for_TB = 5. / 2 ** 40  # USD/TB

    def get_credentials(self):
        if self.private_key:
            return self.get_service_account_credentials()
        else:
            # Try to retrieve Application Default Credentials
            credentials = self.get_application_default_credentials()
            if not credentials:
                credentials = self.get_user_account_credentials()
            return credentials

    def get_application_default_credentials(self):
        """
        This method tries to retrieve the "default application credentials".
        This could be useful for running code on Google Cloud Platform.

        Parameters
        ----------

        Returns
        -------
        - GoogleCredentials,
            If the default application credentials can be retrieved
            from the environment. The retrieved credentials should also
            have access to the project (self.project_id) on BigQuery.
        - OR None,
            If default application credentials can not be retrieved
            from the environment. Or, the retrieved credentials do not
            have access to the project (self.project_id) on BigQuery.
        """
        import google.auth
        from google.auth.exceptions import DefaultCredentialsError

        try:
            credentials, _ = google.auth.default(scopes=self.scope)
        except (DefaultCredentialsError, IOError):
            return None

        return _try_credentials(self.project_id, credentials)

    def load_user_account_credentials(self):
        """
        Loads user account credentials from a local file.

        .. versionadded 0.2.0

        Parameters
        ----------

        Returns
        -------
        - GoogleCredentials,
            If the credentials can loaded. The retrieved credentials should
            also have access to the project (self.project_id) on BigQuery.
        - OR None,
            If credentials can not be loaded from a file. Or, the retrieved
            credentials do not have access to the project (self.project_id)
            on BigQuery.
        """
        import httplib2
        from google_auth_httplib2 import Request
        from google.oauth2.credentials import Credentials

        try:
            with open('bigquery_credentials.dat') as credentials_file:
                credentials_json = json.load(credentials_file)
        except (IOError, ValueError):
            return None

        credentials = Credentials(
            token=credentials_json.get('access_token'),
            refresh_token=credentials_json.get('refresh_token'),
            id_token=credentials_json.get('id_token'),
            token_uri=credentials_json.get('token_uri'),
            client_id=credentials_json.get('client_id'),
            client_secret=credentials_json.get('client_secret'),
            scopes=credentials_json.get('scopes'))

        # Refresh the token before trying to use it.
        http = httplib2.Http()
        request = Request(http)
        credentials.refresh(request)

        return _try_credentials(self.project_id, credentials)

    def save_user_account_credentials(self, credentials):
        """
        Saves user account credentials to a local file.

        .. versionadded 0.2.0
        """
        try:
            with open('bigquery_credentials.dat', 'w') as credentials_file:
                credentials_json = {
                    'refresh_token': credentials.refresh_token,
                    'id_token': credentials.id_token,
                    'token_uri': credentials.token_uri,
                    'client_id': credentials.client_id,
                    'client_secret': credentials.client_secret,
                    'scopes': credentials.scopes,
                }
                json.dump(credentials_json, credentials_file)
        except IOError:
            self._print('Unable to save credentials.')

    def get_user_account_credentials(self):
        """Gets user account credentials.

        This method authenticates using user credentials, either loading saved
        credentials from a file or by going through the OAuth flow.

        Parameters
        ----------

        Returns
        -------
        GoogleCredentials : credentials
            Credentials for the user with BigQuery access.
        """
        from google_auth_oauthlib.flow import InstalledAppFlow
        from oauthlib.oauth2.rfc6749.errors import OAuth2Error

        credentials = self.load_user_account_credentials()

        client_config = {
            'installed': {
                'client_id': ('495642085510-k0tmvj2m941jhre2nbqka17vqpjfddtd'
                              '.apps.googleusercontent.com'),
                'client_secret': 'kOc9wMptUtxkcIFbtZCcrEAc',
                'redirect_uris': ['urn:ietf:wg:oauth:2.0:oob'],
                'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                'token_uri': 'https://accounts.google.com/o/oauth2/token',
            }
        }

        if credentials is None or self.reauth:
            app_flow = InstalledAppFlow.from_client_config(
                client_config, scopes=[self.scope])

            try:
                if self.auth_local_webserver:
                    credentials = app_flow.run_local_server()
                else:
                    credentials = app_flow.run_console()
            except OAuth2Error as ex:
                raise AccessDenied(
                    "Unable to get valid credentials: {0}".format(ex))

            self.save_user_account_credentials(credentials)

        return credentials

    def get_service_account_credentials(self):
        import httplib2
        from google_auth_httplib2 import Request
        from google.oauth2.service_account import Credentials
        from os.path import isfile

        try:
            if isfile(self.private_key):
                with open(self.private_key) as f:
                    json_key = json.loads(f.read())
            else:
                # ugly hack: 'private_key' field has new lines inside,
                # they break json parser, but we need to preserve them
                json_key = json.loads(self.private_key.replace('\n', '   '))
                json_key['private_key'] = json_key['private_key'].replace(
                    '   ', '\n')

            if compat.PY3:
                json_key['private_key'] = bytes(
                    json_key['private_key'], 'UTF-8')

            credentials = Credentials.from_service_account_info(json_key)
            credentials = credentials.with_scopes(self.scope)

            # Refresh the token before trying to use it.
            http = httplib2.Http()
            request = Request(http)
            credentials.refresh(request)

            return credentials
        except (KeyError, ValueError, TypeError, AttributeError):
            raise InvalidPrivateKeyFormat(
                "Private key is missing or invalid. It should be service "
                "account private key JSON (file path or string contents) "
                "with at least two keys: 'client_email' and 'private_key'. "
                "Can be obtained from: https://console.developers.google."
                "com/permissions/serviceaccounts")

    def _print(self, msg, end='\n'):
        if self.verbose:
            sys.stdout.write(msg + end)
            sys.stdout.flush()

    def _start_timer(self):
        self.start = time.time()

    def get_elapsed_seconds(self):
        return round(time.time() - self.start, 2)

    def print_elapsed_seconds(self, prefix='Elapsed', postfix='s.',
                              overlong=7):
        sec = self.get_elapsed_seconds()
        if sec > overlong:
            self._print('{} {} {}'.format(prefix, sec, postfix))

    # http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        fmt = "%3.1f %s%s"
        for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < 1024.0:
                return fmt % (num, unit, suffix)
            num /= 1024.0
        return fmt % (num, 'Y', suffix)

    def get_service(self):
        import httplib2
        from google_auth_httplib2 import AuthorizedHttp
        from googleapiclient.discovery import build

        http = httplib2.Http()
        authed_http = AuthorizedHttp(
            self.credentials, http=http)
        bigquery_service = build('bigquery', 'v2', http=authed_http)

        return bigquery_service

    @staticmethod
    def process_http_error(ex):
        # See `BigQuery Troubleshooting Errors
        # <https://cloud.google.com/bigquery/troubleshooting-errors>`__

        status = json.loads(bytes_to_str(ex.content))['error']
        errors = status.get('errors', None)

        if errors:
            for error in errors:
                reason = error['reason']
                message = error['message']

                raise GenericGBQException(
                    "Reason: {0}, Message: {1}".format(reason, message))

        raise GenericGBQException(errors)

    def process_insert_errors(self, insert_errors):
        for insert_error in insert_errors:
            row = insert_error['index']
            errors = insert_error.get('errors', None)
            for error in errors:
                reason = error['reason']
                message = error['message']
                location = error['location']
                error_message = ('Error at Row: {0}, Reason: {1}, '
                                 'Location: {2}, Message: {3}'
                                 .format(row, reason, location, message))

                # Report all error messages if verbose is set
                if self.verbose:
                    self._print(error_message)
                else:
                    raise StreamingInsertError(error_message +
                                               '\nEnable verbose logging to '
                                               'see all errors')

        raise StreamingInsertError
