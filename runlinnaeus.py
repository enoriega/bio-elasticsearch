''' Runs the species NER over the sentences and generates Hans output-like json to be indexed. Assumes linnaeus-2.0.jar is in the current directory '''

import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import glob
import os
import os.path
import time
import shutil
from __init__ import *

startt = time.time()
# Make sure the working dir will exist
WORK = os.path.join(DIR, "work")
CLEANUP = False
WRITE = False

if not os.path.exists(WORK):
    os.makedirs(WORK)

# Read the sentences
print "Retrieving sentences ..."
for fn in glob.glob(os.path.join(DIR, '*uaz.sentences.json')):
    with open(fn) as f:
        entity = json.load(f)

    sentences = {}
    for sen in entity['frames']:
        if sen["frame-type"] == "sentence":
            sid = sen["frame-id"]
            sentences[sid] = sen
            try:
                # Extract the PMCID of the source document and index it
                doc = sid.split('-')[1]

                assert doc[:3] == 'PMC', "Problem extracting the PMC ID of %s" % sid

                if WRITE:
                    with open(os.path.join(WORK, sid+".txt"), 'w') as f:
                        f.write(sen['text'])
            except Exception as e:
                print "Problem with %s" % sid

print "Running linnaeus ..."
os.system("java -Xmx4G -jar linnaeus-2.0.jar --properties linnaeus.conf --textDir %s --out %s" % (WORK, LINOUT))

print "Parsing linnaeus output ..."
with open(LINOUT) as f:
    entries = []
    for i, l in enumerate(f):
        # Skip the headers
        if i == 0:
            continue
        # Parse a dictionary for each line in the file
        ncbiid, sentid, start, end, text = l.split('\t', 4)

        frameid = "species-%s-%i" % (sentid.split('-', 1)[1], i)

        entry= {"end-pos": {
            "object-type": "relative-pos",
            "reference": sentid,
            "offset": int(end)
          },
          "xrefs": [
            {
              "object-type": "db-reference",
              "namespace": "ncbitax",
              "id": ncbiid
            }
          ],
          "object-type": "frame",
          "start-pos": {
            "object-type": "relative-pos",
            "reference": sentid,
            "offset": int(start)
          },
          "text": text,
          "object-meta": {
            "object-type": "meta-info",
            "component": "Linnaeus"
          },
          "sentence": sentid,
          "frame-type": "entity-mention",
          "frame-id": frameid,
          "type": "species",
          "PMCID": sentid.split('-')[1]
        }

        entries.append(entry)

with open(os.path.join(DIR, 'species.json'), 'w') as f:
    json.dump({'frames':entries}, f)

if CLEANUP:
    print "Cleaning up ..."
    shutil.rmtree(WORK)


end = time.time()

print 'Elapsed time: %f' % (end-startt)
