#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Mariethoz <Johnny.Mariethoz@rero.ch>"
__version__ = "0.0.0"
__copyright__ = "Copyright (c) 2009 Rero, Johnny Mariethoz"
__license__ = "Internal Use Only"


#---------------------------- Modules -----------------------------------------

# import of standard modules
import sys
import os
import glob
from optparse import OptionParser

# third party modules


#---------------------------- Main Part ---------------------------------------

if __name__ == '__main__':

    usage = "usage: %prog [options]"

    parser = OptionParser(usage)

    parser.set_description ("Change It")

    
    (options,args) = parser.parse_args ()
    from flask import request
    from invenio.base.factory import create_app
    current_app = create_app()
    
    with current_app.test_request_context():
        print "-- Connect to the ES server --"
        es = current_app.extensions.get("elasticsearch")

        print "-- Delete old index --"
        es.delete_index()

        print "-- Create the index --"
        es.create_index()

        print "-- Index records --"
        es.index_records(range(1,100), bulk_size=10)

        print "-- Index bibdocs --"
        es.index_bibdocs(range(1,100), bulk_size=10)

        print "-- Index collections --"
        es.index_collections(range(1,100), bulk_size=1000)

        print "-- Perform search --"
        res = es.search(query="Oxford or unification",
                facet_filters=[("facet_authors", "Ellis, J"), ("facet_authors",
                    "Ibanez, L E")])

        print "Hits:"
        print [hit for hit in res.hits]
        import json
        print "Facets:"
        print json.dumps(res.facets.__dict__, indent=2)
        print "Highlights:"
        print json.dumps(res.highlights.__dict__, indent=2)
