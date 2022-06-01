from airflow.models import Variable

import json
import logging
import re
import requests

from bs4 import BeautifulSoup
from markdown import markdown
from requests import HTTPError


#authentication libraries
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

#Defining Superset's class
class Superset:
    """A class for accessing the Superset API in an easy way."""

    def __init__(self, api_url, access_token=None, refresh_token=None):
        """Instantiates the class.

        If ``access_token`` is None, attempts to obtain it using ``refresh_token``.

        Args:
            api_url: Base API URL of a Superset instance, e.g. https://my-superset/api/v1.
            access_token: Access token to use for accessing protected endpoints of the Superset
                API. Can be automatically obtained if ``refresh_token`` is not None.
            refresh_token: Refresh token to use for obtaining or refreshing the ``access_token``.
                If None, no refresh will be done.
        """

        self.api_url = api_url
        self.access_token = access_token
        self.refresh_token = refresh_token

        if self.access_token is None:
            self._refresh_access_token()

    def _headers(self, **headers):
        if self.access_token is None:
            return headers

        return {
            'Authorization': f'Bearer {self.access_token}',
            **headers,
        }

    def _refresh_access_token(self):
        logger.debug("Refreshing API token")

        if self.refresh_token is None:
            logging.warning("Cannot refresh access_token, refresh_token is None")
            return False

        res = self.request('POST', '/security/refresh',
                           headers={'Authorization': f'Bearer {self.refresh_token}'},
                           refresh_token_if_needed=False)
        self.access_token = res['access_token']

        logger.debug("Token refreshed successfully")
        return True

    def request(self, method, endpoint, refresh_token_if_needed=True, headers=None,
                **request_kwargs):
        """Executes a request against the Superset API.

        Args:
            method: HTTP method to use.
            endpoint: Endpoint to use.
            refresh_token_if_needed: Whether the ``access_token`` should be automatically refreshed
                if needed.
            headers: Additional headers to use.
            **request_kwargs: Any ``requests.request`` arguments to use.

        Returns:
            A dictionary containing response body parsed from JSON.

        Raises:
            HTTPError: There is an HTTP error (detected by ``requests.Response.raise_for_status``)
                even after retrying with a fresh ``access_token``.
        """

        logger.info("About to %s execute request for endpoint %s", method, endpoint)

        if headers is None:
            headers = {}

        url = self.api_url + endpoint
        res = requests.request(method, url, headers=self._headers(**headers), **request_kwargs)

        logger.debug("Request finished with status: %d", res.status_code)

        if refresh_token_if_needed and res.status_code == 401 \
                and res.json().get('msg') == 'Token has expired' and self._refresh_access_token():
            logger.debug("Retrying %s request for endpoint %s with refreshed token")
            res = requests.request(method, url, headers=self._headers(**headers))
            logger.debug("Request finished with status: %d", res.status_code)

        res.raise_for_status()
        return res.json()

def token_request():

    username = Variable.get("SUPERSET_USER")
    password = Variable.get("SUPERSET_PASSWORD")
    client_id = Variable.get("SUPERSET_CLIENT_ID")
    client_secret = Variable.get("SUPERSET_CLIENT_SECRET")
    token_url= Variable.get("TOKEN_URL")

    params={
      "order_column": "changed_on",
      "order_direction": "asc",
      "page": 0,
      "page_size": 2
    }

    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    token = oauth.fetch_token(token_url=token_url,
            username=username, password=password, client_id=client_id,
            client_secret=client_secret)
    return token['access_token']

def get_datasets_from_superset(superset, superset_db_id):
    logging.info("Getting physical datasets from Superset.")

    page_number = 0
    datasets = []
    datasets_keys = set()

    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request('GET', f'/dataset/?q={{"page":{page_number},"page_size":100}}')
        result = res['result']
        if result:
            for r in result:
                kind = r['kind']
                database_id = r['database']['id']

                if kind == 'physical' \
                        and (superset_db_id is None or database_id == superset_db_id):

                    dataset_id = r['id']

                    name = r['table_name']
                    schema = r['schema']
                    dataset_key = f'{schema}.{name}'  # used as unique identifier

                    dataset_dict = {
                        'id': dataset_id,
                        'schema': schema,
                        'key': dataset_key
                    }

                    # fail if it breaks uniqueness constraint
                    assert dataset_key not in datasets_keys, \
                        f"Dataset {dataset_key} is a duplicate name (schema + table) " \
                        "across databases. " \
                        "This would result in incorrect matching between Superset and dbt. " \
                        "To fix this, remove duplicates or add the ``superset_db_id`` argument."

                    datasets_keys.add(dataset_key)
                    datasets.append(dataset_dict)
            page_number += 1
        else:
            break
    assert datasets, "There are no datasets in Superset!"

    return datasets


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    tables = {}
    for table_type in ['nodes']:
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table['name']
            schema = table['schema']
            database = table['database']

            table_key_short = schema + '.' + name
            columns = table['columns']

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key_short not in tables, \
                    f"Table {table_key_short} is a duplicate name (schema + table) " \
                    f"across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add the ``dbt_db_name`` argument."

                tables[table_key_short] = {'columns': columns}

    assert tables, "Manifest is empty!"

    return tables


def refresh_columns_in_superset(superset, dataset_id):
    logging.info("Refreshing columns in Superset.")
    superset.request('PUT', f'/dataset/{dataset_id}/refresh')

def add_certifications_in_superset(superset, dataset_id):
    logging.info("Refreshing columns in Superset.")
    body = {"extra": "{\"certification\": \n  {\"certified_by\": \"Data Analytics Team\", \n   \"details\": \"This table is the source of truth.\" \n    \n  }\n}"}
    superset.request('PUT', f'/dataset/{dataset_id}',json=body)

def add_superset_columns(superset, dataset):
    logging.info("Pulling fresh columns info from Superset.")
    res = superset.request('GET', f"/dataset/{dataset['id']}")
    columns = res['result']['columns']
    dataset['columns'] = columns
    return dataset


def convert_markdown_to_plain_text(md_string):
    """Converts a markdown string to plaintext.

    The following solution is used:
    https://gist.github.com/lorey/eb15a7f3338f959a78cc3661fbc255fe
    """

    # md -> html -> text since BeautifulSoup can extract text cleanly
    html = markdown(md_string)

    # remove code snippets
    html = re.sub(r'<pre>(.*?)</pre>', ' ', html)
    html = re.sub(r'<code>(.*?)</code >', ' ', html)

    # extract text
    soup = BeautifulSoup(html, 'html.parser')
    text = ''.join(soup.findAll(text=True))

    # make one line
    single_line = re.sub(r'\s+', ' ', text)

    # make fixes
    single_line = re.sub('→', '->', single_line)
    single_line = re.sub('<null>', '"null"', single_line)

    return single_line


def merge_columns_info(dataset, tables):
    logging.info("Merging columns info from Superset and manifest.json file.")

    key = dataset['key']
    sst_columns = dataset['columns']
    dbt_columns = tables.get(key, {}).get('columns', {})
    columns_new = []
    for sst_column in sst_columns:

        # add the mandatory field
        column_new = {'column_name': sst_column['column_name']}

        # add optional fields only if not already None, otherwise it would error out
        for field in ['expression', 'filterable', 'groupby', 'python_date_format',
                      'verbose_name', 'type', 'is_dttm', 'is_active']:
            if sst_column[field] is not None:
                column_new[field] = sst_column[field]

        # add description
        if sst_column['column_name'] in dbt_columns \
                and 'description' in dbt_columns[sst_column['column_name']] \
                and sst_column['expression'] == '':  # database columns
            description = dbt_columns[sst_column['column_name']]['description']
            description = convert_markdown_to_plain_text(description)
        else:
            description = sst_column['description']
        column_new['description'] = description

        columns_new.append(column_new)

    dataset['columns_new'] = columns_new

    return dataset


def put_columns_to_superset(superset, dataset):
    logging.info("Putting new columns info with descriptions back into Superset.")
    body = {'columns': dataset['columns_new']}
    superset.request('PUT', f"/dataset/{dataset['id']}?override_columns=true", json=body)

def main(dbt_project_dir, superset_url, dbt_db_name=None,
         superset_db_id=None, superset_refresh_column=None,
         superset_access_token=None, superset_refresh_token=None):

    #requesting a token
    superset_access_token=token_request()
    # require at least one token for Superset
    assert superset_access_token is not None or superset_refresh_token is not None, \
           "Add ``SUPERSET_ACCESS_TOKEN`` or ``SUPERSET_REFRESH_TOKEN`` " \
           "to your environment variables or provide in CLI " \
           "via ``superset-access-token`` or ``superset-refresh-token``."

    superset = Superset(superset_url + '/api/v1',
                        access_token=superset_access_token, refresh_token=superset_refresh_token)

    logging.info("Starting the script!")

    sst_datasets = get_datasets_from_superset(superset, superset_db_id)
    logging.info("There are %d physical datasets in Superset.", len(sst_datasets))

    with open(f'{dbt_project_dir}/target/manifest.json') as f:
        dbt_manifest = json.load(f)

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)

    for i, sst_dataset in enumerate(sst_datasets):
        columns_refreshed=0
        logging.info("Processing dataset %d/%d.", i + 1, len(sst_datasets))
        sst_dataset_id = sst_dataset['id']
        try:
            refresh_columns_in_superset(superset, sst_dataset_id)
            columns_refreshed=1
        except HTTPError as e:
            #refreshing token
            superset_access_token=token_request()
            superset = Superset(superset_url + '/api/v1',
                                access_token=superset_access_token, refresh_token=superset_refresh_token)
        try:
            if columns_refreshed==1:
                columns_refreshed=1
            else:
                refresh_columns_in_superset(superset, sst_dataset_id)
            #Otherwise, just adding the normal analytics certification
            add_certifications_in_superset(superset, sst_dataset_id)
            sst_dataset_w_cols = add_superset_columns(superset, sst_dataset)
            sst_dataset_w_cols_new = merge_columns_info(sst_dataset_w_cols, dbt_tables)
            put_columns_to_superset(superset, sst_dataset_w_cols_new)
        except HTTPError as e:
            logging.error("The dataset with ID=%d wasn't updated. Check the error below.",
                          sst_dataset_id, exc_info=e)

    logging.info("All done!")

if __name__ == "__main__":
    dbt_project_dir=Variable.get("DBT_PROJECT_DIR")
    superset_url=Variable.get("SUPERSET_URL")

    main(dbt_project_dir=dbt_project_dir, superset_url=superset_url, dbt_db_name=None,
             superset_db_id=None, superset_refresh_column=None,
             superset_access_token=None, superset_refresh_token=None)
