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

start = time.time()
# Make sure the working dir will exist
WORK = os.path.join(DIR, "work")
if not os.path.exists(WORK):
    os.makedirs(WORK)

# Read the sentences
print "Retrieving sentences ..."
for fn in glob.glob(os.path.join(DIR, '*uaz.sentences.json')):
    with open(fn) as f:
        entity = json.load(f)

    for sen in entity['frames']:
        if sen["frame-type"] == "sentence":
            sid = sen["frame-id"]
            try:
                # Extract the PMCID of the source document and index it
                doc = sid.split('-')[1]

                assert doc[:3] == 'PMC', "Problem extracting the PMC ID of %s" % sid

                with open(os.path.join(WORK, sid+".txt"), 'w') as f:
                    f.write(sen['text'])
            except Exception as e:
                print "Problem with %s" % sid

print "Running linnaeus ..."
# os.system("java -jar linnaeus-2.0.jar --default --default-proxy --report 10000 --threads 4 --textDir %s --out %s" % (WORK, "lin-out.tsv"))
os.system("java -Xmx4G -jar linnaeus-2.0.jar --properties linnaeus.conf --textDir %s --out %s" % (WORK, LINOUT))


print "Cleaning up ..."
shutil.rmtree(WORK)


end = time.time()

print 'Elapsed time: %f' % (end-start)
