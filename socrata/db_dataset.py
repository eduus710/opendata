import csv
import getopt
import os
import string
import sys

import socrata.api as api
import socrata.db as db


# generate and execute DROP TABLE for specified table name
#
def drop_raw_table(cursor, table_name):
    ddl = "DROP TABLE IF EXISTS `{}`;".format(table_name)
    db.execute(cursor, ddl)


# generate and execute CREATE TABLE DDL for specified CSV
#
def create_raw_table(cursor, tablename, csvfile):
    with open(csvfile, encoding='utf-8') as f:
        r = csv.reader(f)
        fields = next(r)

    fieldnames = []
    # todo - for now only load first 100 columns
    ddl = 'CREATE TABLE `{}` (\n'.format(tablename)
    for field in fields[:100]:
        # lower case and find a way to shrink to 64
        field = field.lower().strip()
        if len(field) > 64:
            field = field.translate(str.maketrans("", "", string.punctuation))
        if len(field) > 64:
            field = field.translate(str.maketrans("", "", string.whitespace))
        if len(field) > 64:
            field = field.translate(str.maketrans("", "", "aeiou"))
        ddl += "`{}` TEXT,\n".format(field)
        fieldnames.append(field)
    ddl = ddl[:-2] + ");"  # remove last comma+newline
    # TODO check for other encoding spots
    # print(ddl.encode('utf-8'))
    db.execute(cursor, ddl)
    if len(fields) > 100:
        fieldnames.extend(['@dummy'] * (len(fields) - 100))
    return fieldnames


# generate and execute LOAD DATA DDL for specified csv file
# 
def load_raw_table(cursor, table_name, fieldnames, csvfile):
    stmt = "LOAD DATA INFILE '{}'\n".format(os.path.abspath(csvfile).replace("\\", "/"))
    stmt += "REPLACE INTO TABLE `{}`\n".format(table_name)

    stmt += """FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'\n"""
    stmt += "LINES TERMINATED BY '\\n' \n"
    stmt += "IGNORE 1 LINES\n"
    stmt += "("
    for field in fieldnames:
        if field.startswith('@'):
            stmt += "{},".format(field)
        else:
            stmt += "`{}`,".format(field)
    stmt = stmt[:-1] + ")\n;"
    db.execute(cursor, stmt)


def load_one_csv(cursor, csvpath, csvfile):
    print('load {}'.format(csvfile))
    [csv_name, csv_ext] = os.path.splitext(csvfile)
    csvpathtofile = os.path.join(csvpath, csvfile)
    csvnorm = os.path.normpath(csvpathtofile)
    csvnormesc = csvnorm.replace('\\', '\\\\')
    print(csvpathtofile, csvnorm)
    tablename = csv_name + '-raw'
    drop_raw_table(cursor, tablename)
    fieldnames = create_raw_table(cursor, tablename, os.path.join(csvpath, csvfile))
    load_raw_table(cursor, tablename, fieldnames, csvnormesc)
    cursor.execute("select count(*) from `{}`".format(tablename))
    myresult = cursor.fetchall()
    for x in myresult:
        print(x)


def load_all_csv(cursor, csvpath):
    # retrieve all csv in directory
    # TODO can't use with context manager due to python 3.5. upgrade to python 3.6
    files = os.scandir(csvpath)
    csvfiles = [f.name for f in files if f.is_file() and f.name.endswith('.csv')]

    # load all csv
    return [load_one_csv(cursor, csvpath, csvfile) for csvfile in csvfiles]


def load_csv(csvdir, schema):
    mydb = db.mysql_db(schema)
    load_all_csv(mydb._db_cur, csvdir)


def main():
    mode = 'all'
    csvdir = './'
    portal = ''
    portals = {
        'de': ('data.delaware.gov', 'odde'),
        'md': ('data.maryland.gov', 'odmd'),
        'pa': ('data.pa.gov', 'odpa'),
        'nj': ('data.nj.gov', 'odnj')
    }

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:m:p:")
        print(opts)
    except getopt.GetoptError:
        print('ddgload.py -m [fetch:load:all] -d <csvdir>')
        sys.exit(2)
    for opt, arg in opts:
        print(opt, arg)
        if opt == '-h':
            print('ddgload.py -m [fetch:load:all] -p portal -d <csvdir>')
            sys.exit()
        elif opt == '-m':
            mode = arg
        elif opt in '-d':
            csvdir = arg
        elif opt in '-p':
            portal = arg

    print('executing {} using {} and {}'.format(mode, csvdir, portal))
    (domain, schema) = portals[portal]

    if mode in ("fetch", "all"):
        api.fetch_domain_csv(domain, csvdir)
    if mode in ("load", "all"):
        load_csv(csvdir, schema)


if __name__ == "__main__":
    main()
