import requests
import os
import re
import timeit

SOCRATA_CATALOG = r'http://api.us.socrata.com/api/catalog/v1?domains={}&only=datasets&limit={}&offset={}'
SOCRATA_CSV_RESOURCE = r'https://{}/resource/{}.csv?$limit={}&$offset={}'


# fetch a portal catalog from socrata
# returns json
#
def fetch_catalog(domain):
    page = 0
    limit = 50
    more_results = True
    results = []

    # paginate catalog requests and assemble
    while more_results:
        url = SOCRATA_CATALOG.format(domain, limit, page * limit)
        json = requests.get(url).json()
        if len(json['results']) == 0:
            more_results = False
            # TODO use resultSize to confirm correct download
        else:
            page += 1
            results += json['results']
    return results


# fetch a socrata dataset as csv to specified local file
#
def fetch_one_csv(domain, dsname, csvfile):
    # pagination control variables
    more_pages = True
    page = 0
    limit = 50000

    print('fetching {} to {}'.format(dsname, csvfile))

    # socrata api for some portals supports 50,000 records / request
    # have to paginate
    # open output file. don't let python alter newline for platform
    with open(csvfile, 'w', encoding='utf-8', newline='') as f:
        while more_pages:
            # print a progress meter
            endchar = '\n' if (page > 0 and page % 20 == 0) else ' '
            print('.', end=endchar, flush=True)

            # request a page
            url = SOCRATA_CSV_RESOURCE.format(domain, dsname, limit, page * limit)
            with requests.get(url, stream=True) as r:
                iterlines = r.iter_lines(decode_unicode=True)
                # all pages come with headers; ignore them after 1st page
                if page > 0:
                    next(iterlines)
                # determine end of dataset by empty result
                more_pages = False
                for line in iterlines:
                    # check for error; kind of dirty, we are expecting CSV and get JSON
                    if re.match(r'^\s*"error"\s*:\s*true', line):
                        raise ValueError('received unexpected payload from socrata ' + line)

                    # handle embedded \ and add trailing newline
                    s = line.replace('\\', '\\\\') + '\n'
                    f.write(s)
                    more_pages = True
                page += 1
    print('\ndone')


# fetch multiple socrata datasets as csv to local file
# store in specified directory (default to ./)
#
def fetch_many_csv(domain, dslist, directory='./'):
    start_time = timeit.default_timer()

    if not os.path.exists(directory):
        os.makedirs(directory)

    for dsname in dslist:
        csv = "{}/{}.csv".format(directory, dsname)
        try:
            fetch_one_csv(domain, dsname, csv)
        except Exception as e:
            print('unexpected error for dataset {}'.format(dsname))
            print(e)

    elapsed = timeit.default_timer() - start_time
    print(elapsed)


# fetch all socrata datasets from a portal catalog
#
def fetch_all_catalog_csv(domain, csvdir):
    catalog = fetch_catalog(domain)
    dslist = []
    for result in catalog:
        resource = result['resource']
        # TODO asset resource type a dataset for validity
        dslist.append(resource['id'])
    print(dslist)
    fetch_many_csv(domain, dslist, csvdir)
