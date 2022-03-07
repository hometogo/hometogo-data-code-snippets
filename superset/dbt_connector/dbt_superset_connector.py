#Parse libraries
import sys
import argparse
#Script libraries
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import LegacyApplicationClient
import logging
import json
import requests
from itertools import compress

def _parse_args(argv):
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--api_pointer',
        help='''The url for API pointer.''',
        required=True
    )
    parser.add_argument(
        '--username',
        help='''Superset API username.''',
        required=True
    )
    parser.add_argument(
        '--password',
        help='''Superset API password.''',
        required=True
    )
    parser.add_argument(
        '--client_id',
        help='''Superset API client id.''',
        required=True
    )
    parser.add_argument(
        '--client_secret',
        help='''Superset API client secret.''',
        required=True
    )
    parser.add_argument(
        '--token_url',
        help='''The url for the token to grant access.''',
        required=True
    )
    parser.add_argument(
        '--dbt_project_dir_name',
        help='''The name of the dbt project directory.''',
        required=True

    )
    parser.add_argument(
        '--schema_prefix_name',
        help='''The name of the schema, prefixed to the table names under the schema.''',
        required=True
    )
    parser.add_argument(
        '--database_number',
        help='''The number of the database as of Superset.''',
        required=True,
        type=int
    )
    parser.add_argument(
        '--owner_id',
        help='''The user id of the table's owner.''',
        required=True,
        type=int
    )
    return parser.parse_args(argv)

def get_tables_from_dbt(dbt_manifest):
    tables = {}
    for table_type in ['nodes']: #removing sources from this list
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]
            name = table['name'] #used for referencing within the dbt project
            schema = table['schema']
            database = table['database']
            alias = table['alias'] #used for referencing from the outside
            source = table['unique_id'].split('.')[-2]
            table_key = schema + '.' + alias
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

def get_datasets_from_superset(access_token, api_pointer):
    logging.info("Getting datasets info from Superset.")
    page_number = 0
    datasets = {}
    while True:
        logging.info("Getting page %d.", page_number + 1)
        rison_request=f'dataset/?q=(page_size:100,page:{page_number},order_column:changed_on_delta_humanized,order_direction:asc,filters:!((col:table_name,opr:nct,value:archived),(col:sql,opr:dataset_is_null_or_empty,value:true)))'
        request_body=api_pointer+rison_request
        res = requests.get(
        request_body,
            headers={'Authorization': 'Bearer {}'.format(access_token)}
        )
        res=res.json()
        result = res['result']
        if result:
            for r in result:
                name = r['table_name']
                schema = r['schema']
                database_name = r['database']['database_name']
                database_id = r['database']['id']
                dataset_id = r['id']

                dataset_key = f'{schema}.{name}'  # same format as in dashboards
                kind = r['kind']
                if kind == 'virtual':  # built on custom sql
                    True
                else:  # built on tables
                    tables = [dataset_key]

                datasets[dataset_key] = {
                    'name': name,
                    'schema': schema,
                    'database': database_name,
                    'dataset_id':dataset_id,
                    'kind': kind,
                    'tables': tables
                }
            page_number += 1
        else:
            break

    return datasets

def token_request(username,password,client_id,client_secret,token_url):

    oauth = OAuth2Session(client=LegacyApplicationClient(client_id=client_id))
    token = oauth.fetch_token(token_url=token_url,
            username=username, password=password, client_id=client_id,
            client_secret=client_secret)
    return token['access_token']

def main():
    # gather argument
    flags = _parse_args(sys.argv[1:])

    api_pointer=flags.api_pointer
    dbt_project_dir=flags.dbt_project_dir_name
    schema_prefix_name=flags.schema_prefix_name
    username=flags.username
    password=flags.password
    client_id=flags.client_id
    client_secret=flags.client_secret
    token_url=flags.token_url
    database_number=flags.database_number
    owner_id=flags.owner_id

    access_token=token_request(username=username,password=password,client_id=client_id,client_secret=client_secret,token_url=token_url)
    superset_tables_dict=get_datasets_from_superset(access_token=access_token, api_pointer=api_pointer)

    print('Starting!')

    with open(f'{dbt_project_dir}/target/manifest.json') as f:
            dbt_manifest = json.load(f)
    dbt_tables=get_tables_from_dbt(dbt_manifest)

    #Getting the dbt tables keys
    dbt_tables_names=list(dbt_tables.keys())

    #Only tables that start with a given schema prefix name

    mapped = map (lambda x: x.startswith(schema_prefix_name), dbt_tables_names)
    mask=list(mapped)
    mapped_superset = map (lambda x: x.startswith(schema_prefix_name), superset_tables_dict)
    mask_superset=list(mapped_superset)

    dbt_tables_reporting=list(compress(dbt_tables_names, mask))
    superset_tables=list(compress(superset_tables_dict, mask_superset))

    #Parsing as sets
    dbt_tables_reporting=set(dbt_tables_reporting)
    superset_tables=set(superset_tables)

    #To add to superset
    add_to_superset=list(dbt_tables_reporting.difference(superset_tables))
    len(add_to_superset)

    #To remove from superset
    remove_from_superset=list(superset_tables.difference(dbt_tables_reporting))
    len(remove_from_superset)

    for i in add_to_superset:
        print('Starting datasets addition')
        print(i)
        api_pointer
        rison_request='dataset/'
        request_body=api_pointer+rison_request
        array = i.split(".")
        schema=array[0]
        table_name=array[1]
        # Data to be written
        dictionary ={
        #Parameter database
          "database": database_number,
          "schema":schema,
          "table_name":array[1],
          "owners":[owner_id]
        }
        # Serializing json
        json_object = json.dumps(dictionary)
        access_token=token_request(username=username,password=password,client_id=client_id,client_secret=client_secret,token_url=token_url)
        response = requests.post(
        request_body,
            headers={'Authorization': 'Bearer {}'.format(access_token)},
            json=dictionary
        )
    print('Done!')

    for i in remove_from_superset:
        print('Starting datasets removal')
        print(i)
        # Dataset id to be deleted
        dataset_id=superset_tables_dict[i]['dataset_id']

        rison_request='dataset/'+str(dataset_id)
        request_body=api_pointer+rison_request
        access_token=token_request(username=username,password=password,client_id=client_id,client_secret=client_secret,token_url=token_url)
        response = requests.delete(
        request_body,
            headers={'Authorization': 'Bearer {}'.format(access_token)}
        )
    print('Done with removing tables!')

if __name__ == "__main__":
    main()
