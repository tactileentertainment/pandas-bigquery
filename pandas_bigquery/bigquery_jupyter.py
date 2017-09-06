from pandas_bigquery import Bigquery
import os, hashlib
import pandas as pd
import logging

log = logging.getLogger()


class BigqueryJupyter(Bigquery):
    def __init__(self,
                 project_id=os.getenv('BIGQUERY_PROJECT'),
                 private_key_path=os.getenv('BIGQUERY_KEY_PATH'),
                 cache_prefix='/tmp/queryresults_'):
        self._cache_path = cache_prefix
        self.super = super(BigqueryJupyter, self)
        super(BigqueryJupyter, self).__init__(project_id, private_key_path)

    def query(self, query, dialect='standard', priority='INTERACTIVE', strict=True, local_cache=True, **kwargs):

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
                df = self.super.query(commentedquery, dialect=dialect, strict=strict, priority=priority)
                with open(os.path.splitext(fn)[0] + '.qry', 'w') as f:
                    f.write(query)

                df.to_pickle(fn)
            return df
        else:
            return self.super.query(commentedquery, dialect=dialect, strict=strict, priority=priority)
