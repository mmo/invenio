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
            self.connection.create_index(index=index)
            self._mapping(index=index)
            return True
        except:
            return False

    def _mapping(self, index):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        mapping = {
                self.records_doc_type: {
                    #default analyzer is set to the fulltext detected language
                    #"_analyzer": {"path": "fulltext_language"},

                    #reduce the index size: to remove for version > 0.9, will
                    # be the default
                    #"_source": { "compress": "true" },

                    # can be used to create relations between records such as PRINT_MEDIA ISSUE relations.
                    #"_parent": {"type": self.records_doc_type},


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
                            }
                        },
                    "authors": {
                        "properties": {
                            "affiliation": {"type": "string"},
                            "first_name": {"type": "string"},
                            "last_name": {"type": "string"},
                            "full_name": {
                                    #can be accessible with "title" instead of "title.title"
                                    "index_name": "authors",
                                    "type": "multi_field",
                                    "fields": {
                                        "authors": {"type": "string", "analyzer": "standard"},
                                        "facet_authors": {"type": "string", "index": "not_analyzed"}
                                        }
                                    }
                            },
                        }
                    }
                }
        try:
            self.connection.put_mapping(index=index,
                    doc_type=self.records_doc_type,
                mapping=mapping)
            return True
        except:
            return False

    def index(self, recids, index=None, **kwargs):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        from invenio.modules.records.api import get_record
        records = []
        for recid in recids:
            record_as_dict = get_record(recid).dumps()
            del record_as_dict["__meta_metadata__"]
            records.append(record_as_dict)
        print "Indexing: %d records" % len(records)
        results = self.connection.bulk_index(index=index,
                doc_type=self.records_doc_type, docs=records, id_field='_id',
                refresh=self.app.config.get("DEBUG"))
        errors = []
        for it in results.get("items"):
            if it.get("index").get("error"):
                errors.append((it.get("index").get("_id"), it.get("index").get("error")))
        return errors

    def find_similar(self, recid, index=None, **kwargs):
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        fields_to_compute_similarity = ["_all"]
        return self.connection.more_like_this(index=index,
                doc_type=self.records_doc_type,
                id=recid, mlt_fields=fields_to_compute_similarity)

    def search(self, query, index=None, **kwargs):
        """ """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']

        query = self.process_query(query)

        return self.process_results(self.connection.search(query, index=index,
                                                          **kwargs))


class ResultResponse(object):

    def __init__(self, data):
        self.data = data

    def __iter__(self):
        #TODO query with token if you ask for more then len(self)
        for hit in self.data['hits']['hits']:
            yield hit['_id']

    def __len__(self):
        return self.data['hits']['total']


class Facets(object):
    # from here down lets make new class???
    def facets(self):
        return self.data['facets']


class Response(object):

    def __init__(self, data):
        self.data = data

    @property
    def hits(self):
        return ResultResponse(self.data)

    @property
    def facets(self):
        return Facets(self.data)


def process_es_results(results):
    return Response(results)


def process_es_query(query):
    es_query = {
        "query": {
            "query_string": {
                "query": query
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
