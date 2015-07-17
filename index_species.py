import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import glob
import os.path
import time
from indexer import indexer
from __init__ import DIR


INDEX = 'reach'
BULK=True

if __name__ == '__main__':


    start = time.time()
    print "Indexing species..."
    indexer(INDEX, "events", 'species.json', BULK)

    end = time.time()

    print 'Elapsed time: %f' % (end-start)
