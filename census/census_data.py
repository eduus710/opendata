from census import Census
from census.core import ACS5Client
from timeit import default_timer as timer
import utils as util

CENSUS_SCHEMA = 'dbo'
CENSUS_GROUP = 'census_group'
CENSUS_VARIABLE = 'census_variable'

logger = util.get_logger()

# todo connection handling
# todo db exceptions


# generate DDL for census group and variable tables
# based off of current vintage group/variables

def create_meta_tables():
    logger.info('')

    # group table
    table_name = f'{CENSUS_SCHEMA}.{CENSUS_GROUP}'
    ddl = f'DROP TABLE IF EXISTS {table_name};\n'
    util.db_execute(ddl)

    ddl = f'CREATE TABLE {table_name} (\n'
    ddl += 'dataset CHAR(16) NOT NULL,\n'
    ddl += 'id CHAR(16) NOT NULL,\n'
    ddl += 'label VARCHAR(256) NOT NULL,\n'
    ddl += 'PRIMARY KEY(dataset, id))'
    util.db_execute(ddl)

    # variable table
    table_name = f'{CENSUS_SCHEMA}.census_variable'
    ddl = f'DROP TABLE IF EXISTS {table_name};\n'
    util.db_execute(ddl)

    ddl = f'CREATE TABLE {table_name} (\n'
    ddl += 'dataset CHAR(16) NOT NULL,\n'
    ddl += 'id CHAR(32) NOT NULL,\n'
    ddl += 'group_id CHAR(16) NOT NULL,\n'
    ddl += 'predicate_type CHAR(32) NOT NULL,\n'
    ddl += 'label VARCHAR(256) NOT NULL,\n'
    ddl += 'PRIMARY KEY(dataset, id),\n'
    ddl += 'INDEX idx01(dataset, group_id, id))'
    util.db_execute(ddl)


# generate DDL for one census data table
# assume NAME variable not included; add vintage, name, geo

def create_data_table(dataset, group_name, variables):
    logger.info(f'{dataset} {group_name}')

    predicate_types = {
        'int': 'INT',
        'float': 'FLOAT',
        'string': 'CHAR(64)'}

    table_name = f'{CENSUS_SCHEMA}.{dataset.lower()}_{group_name.lower()}'
    ddl = f'DROP TABLE IF EXISTS {table_name};\n'
    util.db_execute(ddl)

    ddl = f'CREATE TABLE {table_name} (\n'
    ddl += 'vintage INT NOT NULL,\n'
    ddl += 'name CHAR(64) NOT NULL,\n'
    ddl += 'geo CHAR(64) NOT NULL,\n'
    ddl += 'geo_id CHAR(64) NOT NULL,\n'

    # sort variables to order table columns
    for variable_name in sorted(variables):
        v = variables[variable_name]
        predicate_type = v.get('predicateType', 'string')
        sql_type = predicate_types[predicate_type]
        ddl += f'{variable_name.lower()} {sql_type},\n'
    ddl += 'PRIMARY KEY(vintage, name),\n'
    ddl += 'INDEX idx01(vintage, geo, geo_id))'
    util.db_execute(ddl)


# store census group metadata

def insert_groups(dataset, groups):
    logger.info(dataset)

    ddl = f'INSERT INTO {CENSUS_SCHEMA}.{CENSUS_GROUP} (dataset, id, label)\n'
    ddl += 'VALUES (?,?,?)'
    values = [(dataset, item['name'], item['description']) for (item) in groups]
    util.db_execute_many(ddl, values)


# store census variable metadata

def insert_variables(dataset, variables):
    logger.info(dataset)

    ddl = f'INSERT INTO {CENSUS_SCHEMA}.{CENSUS_VARIABLE} (dataset, id, group_id, predicate_type, label)\n'
    ddl += 'VALUES (?,?,?,?,?)'
    values = [(dataset, var_id, item['group'], item.get('predicateType', 'n/a')[:32], item['label'][:255])
              for (var_id, item) in variables.items() if var_id.startswith(item['group'])]
    util.db_execute_many(ddl, values)


# insert census data for a single group/vintage/geo
# retrieve name & geo_id from dataset

def insert_data(dataset, groupname, vintage, geo, data):
    logger.info(f'{dataset} {groupname} {vintage} {geo}')

    table_name = f'{CENSUS_SCHEMA}.{dataset.lower()}_{groupname.lower()}'
    ddl = f'INSERT INTO {table_name} '
    col_ddl = '(vintage,geo'
    val_ddl = 'VALUES(?,?'
    geo_out = CensusHelper.geo_long(geo)
    # build column and value specifiers from first row
    # each row a dictionary
    # name and geography included as data elements
    for col, val in (data[0]).items():
        # convert specific geo to generic geo_id
        if col == geo_out:
            col = 'geo_id'
        col_ddl += ',' + col.lower()
        val_ddl += ',?'
    col_ddl += ')'
    val_ddl += ')'
    ddl += col_ddl + ' ' + val_ddl

    # batch it up for executemany()
    all_values = [(vintage, geo) + tuple(row.values()) for row in data]
    util.db_execute_many(ddl, all_values)


# extend class to support acs5 subject data
class ACS5StClient(ACS5Client):
    dataset = 'acs5/subject'
    years = (2017, 2016, 2015, 2014, 2013, 2012)


# extend class to support acs5 subject data
class CensusPlus(Census):
    def __init__(self, key, year=None):
        Census.__init__(self, key, year)
        self.acs5st = ACS5StClient(key, year, self.session)
        # todo - dynamically fetch years for acs, acs5


# helper utilities to support groups, group_variables, etc

class CensusHelper:

    # return min vintage available for acs data_set
    # todo get this via lookup and per group
    # todo check if vintage supports geo
    @staticmethod
    def min_vintage():
        return 2011
        # return min(acsclient.years)

    # return max vintage available for acs data_set
    @staticmethod
    def max_vintage(acsclient):
        return max(acsclient.years)

    # return groups availalbe for acs / vintage
    @staticmethod
    def groups(acsclient, vintage):
        tables = acsclient.tables(year=vintage)
        return {table['name']: table for table in tables}

    # return variables available for acs / group / vintage
    # returns None if group not found for vintage
    @staticmethod
    def group_variables(acsclient, group_name, vintage):
        tables = acsclient.tables(year=vintage)
        for table in tables:
            # found it?
            if table['name'] == group_name:
                r = acsclient.session.get(table['variables'])
                # discard the EA and MA variables for now
                # not used, and they bump up against SQL server table column limits
                variables = {}
                for key, value in r.json()['variables'].items():
                    if not key.endswith(('EA', 'MA')):
                        variables[key] = value
                return variables
        return None

    # return all variables for a group across vintages
    # traverse all vintages to collect a superset
    @classmethod
    def group_variables_all(cls, acsclient, group_name, first_vintage):
        all_variables = {}
        for vintage in range(first_vintage, cls.max_vintage(acsclient) + 1):
            variables = cls.group_variables(acsclient, group_name, vintage)
            if variables is not None:
                all_variables.update(variables)
        return all_variables

    # convenience method; translate long census geo ids to shorter
    @staticmethod
    def geo_short(geo):
        if geo == 'zip code tabulation area':
            return 'zcta5'
        return geo

    # convenience method; translate short census geo id to long
    @staticmethod
    def geo_long(geo):
        if geo == 'zcta5':
            return 'zip code tabulation area'
        return geo


# retrieve and store census meta data; groups and variables

def load_meta(census):
    create_meta_tables()

    groups = CensusHelper.groups(census.acs5, 2017).values()
    insert_groups('acs5', groups)

    groups = CensusHelper.groups(census.acs5st, 2017).values()
    insert_groups('acs5st', groups)

    variables = census.acs5.fields()
    insert_variables('acs5', variables)

    variables = census.acs5st.fields()
    insert_variables('acs5st', variables)


# retrieve and store census data; across geographies and vintages

def load_data(acsclient, dataset_prefix, get_groups, get_geos):
    min_vintage = CensusHelper.min_vintage()
    max_vintage = CensusHelper.max_vintage(acsclient)

    # table creation step
    for group_id in get_groups:
        group_vars = CensusHelper.group_variables_all(acsclient, group_id, min_vintage)
        create_data_table(dataset_prefix, group_id, group_vars)

    # fetch/store step
    for vintage in range(min_vintage, max_vintage + 1):
        for group_id in get_groups:
            group_vars = CensusHelper.group_variables(acsclient, group_id, vintage)
            # not found - vintage does not exist for this group
            if group_vars is None:
                continue
            # sort variables for census api request
            var_list = sorted(group_vars)
            for geo_id in get_geos:
                logger.info(f'fetch {vintage} {group_id} {geo_id}')
                geo_long = CensusHelper.geo_long(geo_id)
                data = acsclient.get(var_list + ['NAME'],
                                     {'for': f'{geo_long}:*'},
                                     year=vintage)
                insert_data(dataset_prefix, group_id, vintage, geo_id, data)


def main():
    # todo move this to database table
    # load_items = [
    #    [census.acs5, 'acs5',
    #     ['B01001','B03002','B15003','B17026','B19055','B19113',
    #      'B19126','B22007','B23001','B23025','B25002','B25004',
    #      'B25104','B25105','B25106','B27001','C17002','C27006']
    #    ],
    #    [census.acs5st, 'acs5st',
    #     ['S2503']
    #    ]
    # ]

    api_key = util.get_param('CENSUS_API_KEY')
    census = CensusPlus(api_key)

    load_items = [
        [census.acs5, 'acs5',
         ['B17026']
         ]
    ]
    # todo move this to database table
    load_geos = ['us', 'state', 'zcta5']

    logger.info('begin')
    start = timer()
    #util.db_init()
    load_meta(census)
    for item in load_items:
        load_data(item[0], item[1], item[2], load_geos)
    end = timer()
    duration = str(end - start)
    logger.info(f'done in {duration}')


if __name__ == "__main__":
    main()
