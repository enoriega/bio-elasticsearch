import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import glob
import os.path
import time


DIR = 'summer-eval-out-uaz-final2-release'
index = 'reach4'
BULK=True

def indexer(index, doctype, pattern, BULK):
    es = Elasticsearch()

    for fn in glob.glob(os.path.join(DIR, pattern)):
        with open(fn) as f:
            entity = json.load(f)

        operations = []
        for ev in entity['frames']:
            if not BULK:
                es.index(index=index, doc_type=doctype, body=ev)
            else:
                ev['_index'] = index
                ev['_type'] = doctype
                ev['_ttl'] = '1d'
                operations.append(ev)

        if BULK:
            bulk(es, operations)

if __name__ == '__main__':


    start = time.time()
    print "Indexing events..."
    indexer(index, "events", '*.uaz.events.json', BULK)

    print "Indexing entities..."
    indexer(index, "entities", '*.uaz.entities.json', BULK)

    print "Indexing sentences..."
    indexer(index, "sentences", '*.uaz.sentences.json', BULK)

    end = time.time()

    print 'Elapsed time: %f' % (end-start)
