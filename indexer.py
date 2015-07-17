import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import glob
import os.path
import time
from __init__ import DIR


INDEX = 'reach'
BULK=True

def indexer(index, doctype, pattern, BULK):
    es = Elasticsearch()

    for fn in glob.glob(os.path.join(DIR, pattern)):
        with open(fn) as f:
            entity = json.load(f)

        operations = []
        for ev in entity['frames']:
            # Extract the PMCID of the source document and index it
            doc = ev["frame-id"].split('-')[1]

            assert doc[:3] == 'PMC', "Problem extracting the PMC ID of %s" % ev["frame-id"]
            ev["PMCID"] = doc

            if not BULK:
                es.index(index=index, doc_type=doctype, body=ev)
            else:
                ev['_index'] = index
                ev['_type'] = doctype
                operations.append(ev)

        if BULK:
            bulk(es, operations)

if __name__ == '__main__':


    start = time.time()
    print "Indexing events..."
    indexer(INDEX, "events", '*.uaz.events.json', BULK)

    print "Indexing entities..."
    indexer(INDEX, "entities", '*.uaz.entities.json', BULK)

    print "Indexing sentences..."
    indexer(INDEX, "sentences", '*.uaz.sentences.json', BULK)

    end = time.time()

    print 'Elapsed time: %f' % (end-start)
