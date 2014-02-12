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



    parser.add_option ("-v", "--verbose", dest="verbose",
                       help="Verbose mode",
                       action="store_true", default=False)
    
    parser.add_option ("-r", "--reset", dest="reset",
                       help="Reset the db",
                       action="store_true", default=False)
    
    parser.add_option ("-d", "--delete", dest="delete",
                       help="Delete index",
                       action="store_true", default=False)

    parser.add_option ("-b", "--bibdoc", dest="bibdoc",
                       help="Index bibliographic records",
                       action="store_true", default=False)

    parser.add_option ("-C", "--root-collection", dest="rootcollection",
                       help="Index collection",
                       type="string", default="Atlantis Institute of Fictive Science")

    parser.add_option ("-c", "--collection", dest="facetcollections",
                       help="Index collection",
                       action="store_true", default=False)
    parser.add_option ("-i", "--index-name", dest="index_name",
                       help="Index name",
                       type="string", default="rerodoc")
    (options,args) = parser.parse_args ()
    from flask import request
    from invenio.base.factory import create_app
    current_app = create_app()
    
    with current_app.test_request_context():
        es = current_app.extensions.get("elasticsearch")
        es.delete_index()
        es.create_index()
        es.index(range(1,100), bulk_size=10)
        #res = es.search("*")
        res = es.search(query="authors.affiliation:Oxford",
                facet_filters=[("facet_authors", "Ellis, J"), ("facet_authors",
                    "Ibanez, L E")])
        for r in res.hits:
            print r
        print res.facets
