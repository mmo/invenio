# -*- coding: utf-8 -*-
##
## This file is part of Invenio.
## Copyright (C) 2014 CERN.
##
## Invenio is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License as
## published by the Free Software Foundation; either version 2 of the
## License, or (at your option) any later version.
##
## Invenio is distributed in the hope that it will be useful, but
## WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Invenio; if not, write to the Free Software Foundation, Inc.,
## 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""
es_query contains the function wrapper between Invenio and elasticsearch.

usage:
def setup_app(app):

    from es_query import process_es_query, process_es_results
    es = ElasticSearch(app)
    es.set_query_handler(process_es_query)
    es.set_results_handler(process_es_results)


"""
from UserDict import UserDict


def process_es_query(query):
    """Convert an Invenio query into an ES query.

    @param query: [string] Invenio query
    @return: [dict] ES query
    """
    es_query = {
            "query": {
                "bool": {
                    "should": [{
                        "simple_query_string": {
                            "query": query
                            }
                        },
                        {
                            "has_child": {
                                "type": "bibdocs",
                                "query": {
                                    "simple_query_string": {
                                        "fields": ["fulltext"],
                                        "query": query
                                        }
                                    }
                                }
                            }],
                    "minimum_should_match" : 1
                    }
                }
            }
    return es_query


def process_es_results(results):
    """Convert ES results into Invenio search engine's results.

    @param results: [object] elasticsearch results
    @return: [object] standard Invenio search engine results
    """
    return Response(results)


class Response(object):

    def __init__(self, data):
        self.data = data

    @property
    def hits(self):
        return Hits(self.data)

    @property
    def facets(self):
        return Facets(self.data)

    @property
    def highlights(self):
        return Highlights(self.data)


class Hits(object):
    """Iterator over all recids that matched the query."""

    def __init__(self, data):
        self.data = data.get("hits")

    def __iter__(self):
        """
        TODO: query with token if you ask for more then len(self)
        """
        for hit in self.data['hits']:
            yield hit['_id']

    def __len__(self):
        return self.data['total']


class Facets(UserDict):

    def __init__(self, data):
        UserDict.__init__(self, data.get("facets"))


class Highlights(UserDict):

    def __init__(self, data):
        """
        TODO: add fulltext highlights.
        """
        new_data = {}
        for hit in data.get('hits', {}).get('hits', []):
            new_data[hit.get('_id')] = hit.get("highlight", {})
        UserDict.__init__(self, new_data)
