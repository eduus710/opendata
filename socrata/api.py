import os
import re
import logging
import timeit
import requests

logger = logging.getLogger('odde.socrata.api')


# fetch a portal catalog from socrata
# returns json
def fetch_catalog(domain):
    page = 0
    limit = 50
    more_results = True
    results = []

    # paginate catalog requests and assemble
    while more_results:
        url = f'http://api.us.socrata.com/api/catalog/v1?domains={domain}&only=datasets&limit={limit}&offset={page*limit}'
        json = requests.get(url).json()
        if len(json['results']) == 0:
            more_results = False
            # TODO use resultSize to confirm correct download
        else:
            page += 1
            results += json['results']
    return results


# fetch a socrata dataset as csv to specified local file
# csv via api differs from csv download link on portal
# api headers are 'field-name' vs 'column-name' and seem
# more database-friendly.
def fetch_one_csv(domain, dsname, csvfile):
    # pagination control variables
    more_pages = True
    page = 0
    limit = 50000

    logger.info(f'fetching {dsname} to {csvfile}')

    # socrata api for some portals supports max 50,000 records / request
    # have to paginate
    # open output file. don't let python alter newline for platform
    with open(csvfile, 'w', encoding='utf-8', newline='') as f:
        while more_pages:
            # log progress
            if page > 0 and page % 5 == 0:
                logger.info(f'working: {page*limit} records')

            # request a page
            url = f'https://{domain}/resource/{dsname}.csv?$limit={limit}&$offset={page*limit}'
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
    logger.info('done')


# fetch multiple specified socrata datasets from a portal
# store in specified directory
def fetch_many_csv(domain, dslist, target_dir='./'):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for dsname in dslist:
        csv = "{}/{}.csv".format(target_dir, dsname)
        try:
            fetch_one_csv(domain, dsname, csv)
        except Exception as e:
            logger.error(f'unexpected error for dataset {dsname}')
            logger.error(e)


# fetch all socrata datasets from a portal
# use catalog to determine available datasets
# store in specified directory
def fetch_domain_csv(domain, target_dir='./'):
    start_time = timeit.default_timer()

    catalog = fetch_catalog(domain)
    datasets = []
    for result in catalog:
        resource = result['resource']
        datasets.append(resource['id'])
    logger.info(f'found {datasets}')
    fetch_many_csv(domain, datasets, target_dir)

    elapsed = timeit.default_timer() - start_time
    logger.info(f'completed in {elapsed} seconds')