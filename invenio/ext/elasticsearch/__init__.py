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
invenio.ext.elasticsearch
-------------------------

...

"""
from werkzeug.utils import cached_property
from pyelasticsearch import ElasticSearch as PyElasticSearch


class ElasticSearch(object):
    """
    Flask extension

    Initialization of the extension:

    >>> from flask import Flask
    >>> from flask_elasticsearch import ElasticSearch
    >>> app = Flask('myapp')
    >>> s = ElasticSearch(app=app)

    or alternatively using the factory pattern:

    >>> app = Flask('myapp')
    >>> s = ElasticSearch()
    >>> s.init_app(app)
    """

    def __init__(self, app=None):
        self.app = app

        self.process_query = lambda x: x
        self.process_results = lambda x: x
        self.records_doc_type = "records"
        self.bibdocs_doc_type = "bibdocs"
        self.collection_doc_type = "collections"
        self._recids_collections = {}

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """
        Initialize a Flask application.

        Only one Registry per application is allowed.
        """
        app.config.setdefault('ELASTICSEARCH_URL', 'http://localhost:9200/')
        app.config.setdefault('ELASTICSEARCH_INDEX', 'invenio')

        # Follow the Flask guidelines on usage of app.extensions
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        if 'elasticsearch' in app.extensions:
            raise Exception("Flask application already initialized")

        app.extensions['elasticsearch'] = self
        self.app = app

    @cached_property
    def connection(self):
        return PyElasticSearch(self.app.config['ELASTICSEARCH_URL'])

    def query_handler(self, handler):
        self.process_query = handler

    def results_handler(self, handler):
        self.process_results = handler

    @property
    def status(self):
        """
        Return the status of the ES cluster.
        green : means cluster is ready
        yellow : is allows only for developpement
        See: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-health.html
        for more.
        """
        return self.connection.health().get("status")

    def index_exists(self, index=None):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        if self.connection.status().get("indices").get(index):
            return True
        return False

    def delete_index(self, index=None):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        try:
            self.connection.delete_index(index=index)
            return True
        except:
            return False

    def create_index(self, index=None):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        if self.index_exists(index=index):
            return True
        try:
            settings = {

                    #should be set to 1 for exact facet count
                    "number_of_shards" : 1,

                    #in case of primary shard failed
                    "number_of_replicas" : 1,

                    #disable automatic type detection
                    #that can cause errors depending of the indexing order
                    "date_detection" : False,
                    "numeric_detection" : False,
                    }
            self.connection.create_index(index=index, settings=settings)
            self._records_mapping(index=index)
            self._bibdocs_mapping(index=index)
            self._collections_mapping(index=index)
            return True
        except:
            return False

    def _collections_mapping(self, index):
        mapping = {
                self.collection_doc_type : {
                    "_parent": {"type": self.records_doc_type},
                    #define the mapping for all Marc entries
                    "properties": {
                        #force recid type to integer for default sorting
                        "recid": {"type": "integer"},
                        "name": {
                            "type": "string", 
                            "analyzer": "keyword"
                            }
                        }
                    }
        }
        return self._mapping(index, self.collection_doc_type, mapping)

    def _bibdocs_mapping(self, index):
        mapping = {
                self.bibdocs_doc_type: {
                    "_parent": {"type": self.records_doc_type},
                    #define the mapping for all Marc entries
                    "properties": {
                        #force recid type to integer for default sorting
                        "recid": {"type": "integer"},
                        "fulltext": {"type": "string"},
                        }
                    }
        }
        return self._mapping(index, self.bibdocs_doc_type, mapping)

    def _records_mapping(self, index):
        mapping = {
                self.records_doc_type: {
                    #define the mapping for all Marc entries
                    "properties": {
                        #force recid type to integer for default sorting
                        "recid": {"type": "integer"},

                        "email": {"type": "string", "index": "not_analyzed"},
                        "primary_report_number": {"type": "string", "index": "not_analyzed"},
                        "creation_date": {"type": "date"},
                        "modification_date": {"type": "date"},
                        "number_of_authors": {"type": "date"},
                        "keywords": {
                            "properties": {
                                "term": {"type": "string", "index": "not_analyzed"},
                                }
                            },
                        #title: sorting use a separate field
                        "title": {
                            "properties": {
                                "title": {
                                    #can be accessible with "title" instead of "title.title"
                                    "index_name": "title",
                                    "type": "multi_field",
                                    "fields": {
                                        "title": {"type": "string", "analyzer": "standard"},
                                        "sort_title": {"type": "string", "analyzer": "simple"}
                                        }
                                    }
                                }
                            },
                        "authors": {
                            "properties": {
                                "affiliation": {"type": "string"},
                                "first_name": {"type": "string"},
                                "last_name": {"type": "string"},
                                "full_name": {
                                    #can be accessible with "title" instead of "title.title"
                                    #"index_name": "toto",
                                    "type": "multi_field",
                                    "fields": {
                                        "authors": {"type": "string", "analyzer": "standard"},
                                        "facet_authors": {"type": "string",
                                            "analyzer": "keyword"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
        return self._mapping(index, self.records_doc_type, mapping)
        
    def _mapping(self, index, doc_type, mapping):
        try:
            self.connection.put_mapping(index=index, doc_type=doc_type,
                mapping=mapping)
            return True
        except:
            return False

    def _bulk_index_docs(self, docs, doc_type, index):
        if not docs:
            return []
        print "Indexing: %d records for %s" % (len(docs), doc_type)
        results = self.connection.bulk_index(index=index,
                doc_type=doc_type, docs=docs, id_field='_id',
                refresh=self.app.config.get("DEBUG"))
        errors = []
        for it in results.get("items"):
            if it.get("index").get("error"):
                errors.append((it.get("index").get("_id"), it.get("index").get("error")))
        return errors

    def _get_record(self, recid):
        from invenio.modules.records.api import get_record
        record_as_dict = get_record(recid).dumps()
        del record_as_dict["__meta_metadata__"]
        return record_as_dict

    def _get_text(self, recid):
        from invenio.legacy.bibdocfile.api import BibRecDocs
        text = BibRecDocs(recid).get_text()
        if not text:
            return None
        return {
            "fulltext": BibRecDocs(recid).get_text(),
            "recid": recid,
            "_id": recid,
            "_parent": recid
        }

    def _get_collections(self, recid):
        return {
            "recid": recid,
            "_id": recid,
            "_parent": recid,
            "name": self._recids_collections.get(recid, ""),
        }

    def get_all_collections_record(self, recreate_cache_if_needed=True):
        """Return a dict with recid as key and collection list as value. This
        replace existing Invenio function but is faster."""
        from invenio.legacy.search_engine import collection_reclist_cache, get_collection_reclist
        ret = {}
    
        #update the cache?
        if recreate_cache_if_needed:
            collection_reclist_cache.recreate_cache_if_needed()
    
        for name in collection_reclist_cache.cache.keys():
            recids = get_collection_reclist(name, recreate_cache_if_needed=False)
            for recid in recids:
                ret.setdefault(recid, []).append(name)
        self._recids_collections = ret

    def index_collections(self, recids, index=None, bulk_size=100000, **kwargs):
        self.get_all_collections_record()
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        return self._index_docs(recids, self.collection_doc_type, index,
                bulk_size, self._get_collections)

    def index_bibdocs(self, recids, index=None, bulk_size=100000, **kwargs):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        return self._index_docs(recids, self.bibdocs_doc_type, index, bulk_size, self._get_text)

    def index_records(self, recids, index=None, bulk_size=100000, **kwargs):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        return self._index_docs(recids, self.records_doc_type, index, bulk_size, self._get_record)

    def _index_docs(self, recids, doc_type, index, bulk_size, get_docs):
        docs = []
        errors = []
        for recid in recids:
            doc = get_docs(recid)
            if doc:
                docs.append(doc)
            if len(docs) >= bulk_size:
                errors += self._bulk_index_docs(docs, doc_type=doc_type, index=index)
                docs = []
        errors += self._bulk_index_docs(docs, doc_type=doc_type, index=index)
        return errors

    def find_similar(self, recid, index=None, **kwargs):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        fields_to_compute_similarity = ["_all"]
        return self.connection.more_like_this(index=index,
                doc_type=self.records_doc_type,
                id=recid, mlt_fields=fields_to_compute_similarity)

    def search(self, query, index=None, cc=None, c=[], p="", f="", rg=None,
            sf=None, so="d", jrec=0, facet_filters=[], **kwargs):
        """ """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']

        if cc is None:
            cc = self.app.config['CFG_SITE_NAME']

        if rg is None:
            rg = int(self.app.config['CFG_WEBSEARCH_DEF_RECORDS_IN_GROUPS'])

        query = self.process_query(query)
        options = {
            "size": rg,
            "from": jrec,
            "fields": [
                "_all"
                ]
        }
        if sf:
            options["sort"] =  [
                    {
                        "sort_%s" % sf: {
                            "order": "desc" if so == "d" else "asc"
                            }
                        }
                    ] 
        filters = []
        for ft in facet_filters:
            (term, value) = ft
            filters.append({
                "term": {
                    term: value
                }
            })
        for col in c:
            (term, value) = ft
            filters.append({
                "has_child": {
                    "type": self.collection_doc_type,
                    "query": {
                        "term": {
                            "name": {
                                "value": col
                                }
                            }
                        }
                    }
            })
        if filters:
            query = {
                "query" : {
                    "filtered": {
                        "query": query.get("query"),
                        "filter": {
                            "bool": {
                                "must": filters
                            }
                        }
                    }
                }
            }
        query["facets"] = {
            "authors": {
                "terms": {
                    "field": "facet_authors",
                    "size": 10
                }
            }
        }
        query["highlight"] = {
                "fields": {
                    "number_of_fragments" : 3,
                    "fragment_size" : 150,
                    "authors.*": {},
                    "title.*": {}
                    }
                }
        return self.process_results(self.connection.search(query, index=index,
                                                          **kwargs))



class Hits(object):

    def __init__(self, data):
        self.data = data.get("hits")

    def __iter__(self):
        #TODO query with token if you ask for more then len(self)
        for hit in self.data['hits']:
            yield hit['_id']

    def __len__(self):
        return self.data['total']


from UserDict import UserDict
class Facets(UserDict):
    
    def __init__(self, data):
        UserDict.__init__(self, data.get("facets"))

from UserDict import UserDict
class Highlights(UserDict):
    
    def __init__(self, data):
        new_data = {}
        for hit in data.get('hits', {}).get('hits', []):
            new_data[hit.get('_id')] = hit.get("highlight", {})
        UserDict.__init__(self, new_data)

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


def process_es_results(results):
    return Response(results)


def process_es_query(query):
    es_query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "query_string": {
                                "query": query
                                }
                            },
                        {
                            "has_child": {
                                "type": "bibdocs",
                                "query": {
                                    "query_string": {
                                        "default_field": "fulltext",
                                        "query": query
                                        }
                                    }
                                }
                            }
                        ],
                        "minimum_should_match" : 1
                    }
                }
            }
    return es_query


def setup_app(app):

    #from somewhere import process_es_query, process_es_result
    es = ElasticSearch(app)
    es.query_handler(process_es_query)
    es.results_handler(process_es_results)

    #record_changed.connect(es.index)
