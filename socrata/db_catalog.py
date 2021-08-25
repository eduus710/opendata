import socrata.api as api
import socrata.db as db

DB_SCHEMA = 'socrata'
DB_DATASET_TABLE = 'dataset'
DB_COLUMN_TABLE = 'column'


# generate and execute DROP TABLE statements for socrata catalog tables
# TODO logging
def drop_catalog_tables(db_schema, cursor):
    db.execute(cursor, f'DROP TABLE IF EXISTS {db_schema}.{DB_DATASET_TABLE};\n')
    db.execute(cursor, f'DROP TABLE IF EXISTS {db_schema}.{DB_COLUMN_TABLE};\n')


# generate and execute CREATE TABLE statements for socrata catalog tables
# TODO key and index    
def create_catalog_tables(db_schema, cursor):
    ddl = f'CREATE TABLE {db_schema}.{DB_DATASET_TABLE} (\n'
    ddl += 'domain_id CHAR(64) NOT NULL,\n'
    ddl += 'dataset_id CHAR(9) NOT NULL,\n'
    ddl += 'name VARCHAR(255) NOT NULL,\n'
    ddl += 'description TEXT NOT NULL)\n'
    #    ddl += "PRIMARY KEY(domain_id, dataset_id))\n"
    db.execute(cursor, ddl)

    ddl = f'CREATE TABLE {db_schema}.{DB_COLUMN_TABLE} (\n'
    ddl += 'domain_id CHAR(64) NOT NULL,\n'
    ddl += 'dataset_id VARCHAR(255) NOT NULL,\n'
    ddl += 'field_name VARCHAR(255) NOT NULL,\n'
    ddl += 'name VARCHAR(255) NOT NULL,\n'
    ddl += 'datatype VARCHAR(255) NOT NULL,\n'
    ddl += 'format VARCHAR(255) NOT NULL,\n'
    ddl += 'description TEXT NOT NULL)\n'
    #   ddl += "PRIMARY KEY(domain_id, dataset_id,field_name))\n\n"
    db.execute(cursor, ddl)


# insert catalog information; dataset and related columns
def insert_catalog_data(db_schema, cursor, domain, ds):
    ddl = f'INSERT INTO {db_schema}.{DB_DATASET_TABLE} \n'
    ddl += 'VALUES (%s,%s,%s,%s)\n'
    db.execute(cursor, ddl, (domain, ds['id'], ds['name'], ds['description']))

    ddl = "INSERT INTO {}.{} ".format(DB_SCHEMA, DB_COLUMN_TABLE)
    ddl += "VALUES (%s,%s,%s,%s,%s,%s,%s)\n"

    # zip the column-related attributes into a single list
    col_count = len(ds['columns_name'])
    domain_ids = [domain] * col_count
    ds_ids = [ds['id']] * col_count
    col_formats = [str(col_format) for col_format in ds['columns_format']]
    cols = zip(domain_ids, ds_ids, ds['columns_field_name'], ds['columns_name'], ds['columns_datatype'],
               col_formats, ds['columns_description'])
    cols = list(cols)
    db.execute_many(cursor, ddl, cols)


def doit(domain):
    catalog = api.fetch_catalog(domain)
    for resource in catalog:
        insert_catalog_data(DB_SCHEMA, mydb._db_cur, domain, resource['resource'])


mydb = db.mysql_db(DB_SCHEMA)
print(drop_catalog_tables(DB_SCHEMA, mydb._db_cur))
print(create_catalog_tables(DB_SCHEMA, mydb._db_cur))

doit('data.delaware.gov')
# doit('data.maryland.gov', 'odmd')
# doit('data.pa.gov', 'odpa')
# doit('data.nj.gov', 'odnj')
