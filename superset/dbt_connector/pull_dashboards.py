from airflow.models import Variable

import json
import logging
import re
import requests

from pathlib import Path
import ruamel.yaml
import sqlfluff

from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

logging.basicConfig(level=logging.INFO)
logging.getLogger('sqlfluff').setLevel(level=logging.WARNING)
logger = logging.getLogger(__name__)


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

def get_tables_from_sql_simple(sql):
    '''
    (Superset) Fallback SQL parsing using regular expressions to get tables names.
    '''
    sql = re.sub(r'(--.*)|(#.*)', '', sql)  
    sql = re.sub(r'\s+', ' ', sql).lower()  
    sql = re.sub(r'(/\*(.|\n)*\*/)', '', sql)  

    regex = re.compile(r'\b(from|join)\b\s+(\"?(\w+)\"?(\.))?\"?(\w+)\"?\b')  
    tables_match = regex.findall(sql)
    tables = [table[2] + '.' + table[4] if table[2] != '' else table[4]  
              for table in tables_match
              if table[4] != 'unnest'] 

    tables = list(set(tables))

    return tables


def get_tables_from_sql(sql, dialect):
    '''
    (Superset) SQL parsing using sqlfluff to get clean tables names.
    If sqlfluff parsing fails it runs the above regex parsing fun.
    Returns a tables list.
    '''
    try:
        sql_parsed = sqlfluff.parse(sql, dialect=dialect)
        tables_raw = [table.raw for table in sql_parsed.tree.recursive_crawl('table_reference')]
        tables_cleaned = ['.'.join(table.replace('"', '').lower().split('.')[-2:]) for table in
                          tables_raw] 
    except (sqlfluff.core.errors.SQLParseError,
            sqlfluff.core.errors.SQLLexError,
            sqlfluff.core.errors.SQLFluffUserError,
            sqlfluff.api.simple.APIParsingError) as e:
        logging.warning("Parsing SQL through sqlfluff failed. "
                        "Let me attempt this via regular expressions at least and "
                        "check the problematic query and error below.\n%s",
                        sql, exc_info=e)
        tables_cleaned = get_tables_from_sql_simple(sql)

    tables = list(set(tables_cleaned))

    return tables


def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    tables = {}
    for table_type in ['nodes']: 
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table['name'] 
            schema = table['schema']
            database = table['database']
            alias = table['alias'] 
            source = table['unique_id'].split('.')[-2]
            table_key = schema + '.' + alias

            if dbt_db_name is None or database == dbt_db_name:
                # fail if it breaks uniqueness constraint
                assert table_key not in tables, \
                    f"Table {table_key} is a duplicate name (schema + table) across databases. " \
                    "This would result in incorrect matching between Superset and dbt. " \
                    "To fix this, remove duplicates or add ``dbt_db_name``."
                tables[table_key] = {
                    'name': name,
                    'schema': schema,
                    'database': database,
                    'type': table_type[:-1],
                    'ref':
                        f"ref('{name}')" if table_type == 'nodes'
                        else f"source('{source}', '{name}')"
                }

    assert tables, "Manifest is empty!"

    return tables


def get_dashboards_from_superset(superset, superset_url, superset_db_id):
    '''
    This function gets
    1. Get dashboards id list from Superset iterating on the pages of the url
    2. Get a dashboard detail information :
        title, owner, url, unique datasets names 

    Returns dashboards, dashboards_datasets
    '''
    
    logging.info("Getting published dashboards from Superset.")
    page_number = 0
    dashboards_id = []
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request('GET', f'/dashboard/?q={{"page":{page_number},"page_size":100}}')
        result = res['result']
        if result:
            for r in result:
                if r['published']:
                    dashboards_id.append(r['id'])
            page_number += 1
        else:
            break

    assert dashboards_id, "There are no dashboards in Superset!"

    logging.info("There are %d published dashboards in Superset.", len(dashboards_id))

    dashboards = []
    dashboards_datasets_w_db = set()
    for i, d in enumerate(dashboards_id):
        logging.info("Getting info for dashboard %d/%d.", i + 1, len(dashboards_id))
        res = superset.request('GET', f'/dashboard/{d}')
        result = res['result']

        dashboard_id = result['id']
        title = result['dashboard_title']
        url = superset_url + '/superset/dashboard/' + str(dashboard_id)
        owner_name = result['owners'][0]['first_name'] + ' ' + result['owners'][0]['last_name']

        # take unique dataset names, formatted as "[database].[schema].[table]" by Superset
        res_table_names = superset.request('GET', f'/dashboard/{d}/datasets')
        result_table_names = res_table_names['result']

        testing=[]
        for i in range(0,len(result_table_names)):
            testing.append(result_table_names[i]['name'])

        #datasets_raw = list(set(result['table_names'].split(', ')))
        datasets_raw = testing

        # parse dataset names into parts
        datasets_parsed = [dataset[1:-1].split('].[', maxsplit=2) for dataset in datasets_raw]
        datasets_parsed = [[dataset[0], 'None', dataset[1]]  # add None in the middle
                           if len(dataset) == 2 else dataset  # if missing the schema
                           for dataset in datasets_parsed]

        # put them all back together to get "database.schema.table"
        datasets_w_db = ['.'.join(dataset) for dataset in datasets_parsed]

        dbt_project_name='your_dbt_project.'
        datasets_w_db = [dbt_project_name + sub for sub in testing]

        dashboards_datasets_w_db.update(datasets_w_db)

        # skip database, i.e. first item, to get only "schema.table"
        datasets_wo_db = ['.'.join(dataset[1:]) for dataset in datasets_parsed]

        datasets_wo_db=testing
        dashboard = {
            'id': dashboard_id,
            'title': title,
            'url': url,
            'owner_name': owner_name,
            'owner_email': '',  # required for dbt to accept owner_name but not in response
            'datasets': datasets_wo_db  # add in "schema.table" format
        }
        dashboards.append(dashboard)
    # test if unique when database disregarded
    # loop to get the name of duplicated dataset and work with unique set of datasets w db
    dashboards_datasets = set()
    for dataset_w_db in dashboards_datasets_w_db:
        dataset = '.'.join(dataset_w_db.split('.')[1:])  # similar logic as just a bit above

        # fail if it breaks uniqueness constraint and not limited to one database
        assert dataset not in dashboards_datasets or superset_db_id is not None, \
            f"Dataset {dataset} is a duplicate name (schema + table) across databases. " \
            "This would result in incorrect matching between Superset and dbt. " \
            "To fix this, remove duplicates or add ``superset_db_id``."

        dashboards_datasets.add(dataset)

    return dashboards, dashboards_datasets


def get_datasets_from_superset(superset, dashboards_datasets, dbt_tables,
                               sql_dialect, superset_db_id):
    '''
    Returns datasets (dict) containing table info and dbt references
    '''

    logging.info("Getting datasets info from Superset.")
    page_number = 0
    datasets = {}
    while True:
        logging.info("Getting page %d.", page_number + 1)
        res = superset.request('GET', f'/dataset/?q={{"page":{page_number},"page_size":100}}')
        result = res['result']
        if result:
            for r in result:
                name = r['table_name']
                schema = r['schema']
                database_name = r['database']['database_name']
                database_id = r['database']['id']

                dataset_key = f'{schema}.{name}'  # same format as in dashboards

                # only add datasets that are in dashboards, optionally limit to one database
                if dataset_key in dashboards_datasets \
                        and (superset_db_id is None or database_id == superset_db_id):
                    kind = r['kind']
                    if kind == 'virtual':  # built on custom sql
                        sql = r['sql']
                        tables = get_tables_from_sql(sql, sql_dialect)
                        tables = [table if '.' in table else f'{schema}.{table}'
                                  for table in tables]
                    else:  # built on tables
                        tables = [dataset_key]
                    dbt_refs = [dbt_tables[table]['ref'] for table in tables
                                if table in dbt_tables]

                    datasets[dataset_key] = {
                        'name': name,
                        'schema': schema,
                        'database': database_name,
                        'kind': kind,
                        'tables': tables,
                        'dbt_refs': dbt_refs
                    }
            page_number += 1
        else:
            break

    return datasets


def merge_dashboards_with_datasets(dashboards, datasets):
    for dashboard in dashboards:
        refs = set()
        for dataset in dashboard['datasets']:
            if dataset in datasets:
                refs.update(datasets[dataset]['dbt_refs'])
        refs = list(sorted(refs))

        dashboard['refs'] = refs

    return dashboards


def get_exposures_dict(dashboards, exposures):
    dashboards.sort(key=lambda dashboard: dashboard['id'])
    titles = [dashboard['title'] for dashboard in dashboards]
    # fail if it breaks uniqueness constraint for exposure names
    assert len(set(titles)) == len(titles), "There are duplicate dashboard names!"

    exposures_orig = {exposure['url']: exposure for exposure in exposures}
    exposures_dict = [{
        'name': f"superset.{dashboard['title']}",
        'type': 'dashboard',
        'url': dashboard['url'],
        'description': exposures_orig.get(dashboard['url'], {}).get('description', ''),
        'depends_on': dashboard['refs'],
        'owner': {
            'name': dashboard['owner_name'],
            'email': dashboard['owner_email']
        }
    } for dashboard in dashboards]

    return exposures_dict


class YamlFormatted(ruamel.yaml.YAML):
    def __init__(self):
        super(YamlFormatted, self).__init__()
        self.default_flow_style = False
        self.allow_unicode = True
        self.encoding = 'utf-8'
        self.block_seq_indent = 2
        self.indent = 4


def main(dbt_project_dir, superset_url, exposures_path, 
         dbt_db_name=None, superset_db_id=None, superset_access_token=None, 
         superset_refresh_token=None, sql_dialect=None):

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
    

    with open(f'{dbt_project_dir}/target/manifest.json') as f:
        dbt_manifest = json.load(f)

    exposures_yaml_path = dbt_project_dir + exposures_path

    try:
        with open(exposures_yaml_path) as f:
            yaml = ruamel.yaml.YAML(typ='safe')
            exposures = yaml.load(f)['exposures']

    except (FileNotFoundError, TypeError):
        Path(exposures_yaml_path).parent.mkdir(parents=True, exist_ok=True)
        Path(exposures_yaml_path).touch(exist_ok=True)
        exposures = {}

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)

    dashboards, dashboards_datasets = get_dashboards_from_superset(superset,
                                                                   superset_url,
                                                                   superset_db_id)

    datasets = get_datasets_from_superset(superset,
                                          dashboards_datasets,
                                          dbt_tables,
                                          sql_dialect,
                                          superset_db_id)

    dashboards = merge_dashboards_with_datasets(dashboards, datasets)

    exposures_dict = get_exposures_dict(dashboards, exposures)
    
    # insert empty line before each exposure, except the first
    exposures_yaml = ruamel.yaml.comments.CommentedSeq(exposures_dict)
    for e in range(len(exposures_yaml)):
        if e != 0:
            exposures_yaml.yaml_set_comment_before_after_key(e, before='\n')
    
    exposures_yaml_schema = {
        'version': 2,
        'exposures': exposures_yaml
    }
    
    exposures_yaml_file = YamlFormatted()

    with open(exposures_yaml_path, 'w+', encoding='utf-8') as f:
        exposures_yaml_file.dump(exposures_yaml_schema, f)
    
    logging.info("Transferred into a YAML file at %s.", exposures_yaml_path)
    logging.info("All done!")   

if __name__ == "__main__":
    main(dbt_project_dir=None, superset_url=None, exposures_path=None,
        dbt_db_name=None,superset_db_id=None, superset_access_token=None, 
        superset_refresh_token=None, sql_dialect=None)