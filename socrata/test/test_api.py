import socrata.db_dataset as ds
import socrata.api as api
import logging

logging.basicConfig(level=logging.INFO)

# c = api.fetch_catalog('data.delaware.gov')
# print(c)

# assume working directory #set to /opendatade
api.fetch_domain_csv('data.delaware.gov', 'data/socrata/de')

#ds.load_csv('data/socrata/de', 'socrata')