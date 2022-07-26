import logging
import csv
from elasticsearch7 import Elasticsearch, helpers,RequestError

'''Common utility functions for handeling
   creating & indexes with ElasticSearch'''

def bulk_index_json_documents(es,filename,index):
    '''
        @args es client
        @args filename in JSONL format to load into ElasticSearch
        @args index name of index
    '''
    with open(filename) as f:
        res = helpers.bulk(es, f, chunk_size=1000, request_timeout=200,index=index)

def create_index(es,index_name,mapping):
    '''@args es client
       @args index_name, name of index to load data -> str
       @args mapping, mapping of types -> dict'''
    if not es.indices.exists(index=index_name):
        try:
            response = es.indices.create(
                index=index_name,
                body=mapping,
            )
            logging.info("Response from ES: %s for creating index %s: ",response,index_name)
        except RequestError as re:
            logging.error("Error generating request to create index with name: %s",index_name)
            raise re
        except Exception as e:
            logging.error("Exception %s creating index with name: %s",e,index_name)
            raise e

def bulk_index_csv_documents(es,filename,index):
    with open(filename) as f:
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index=index)