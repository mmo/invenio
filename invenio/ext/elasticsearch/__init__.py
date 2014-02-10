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

        self.process_results = lambda x: x
        self.process_query = lambda x: x

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
        return PyElasticSearch(self.gkapp.config['ELASTICSEARCH_URL'])

    def result_handler(self, handler):
        self.process_result = handler

    def query_handler(self, handler):
        self.process_query = handler

    def search(self, query, index=None, **kwargs):
        """ """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']

        query = self.process_query(query)

        return self.process_result(self.connection.search(query, index=index,
                                                          **kwargs))


class ResultResponse(object):

    def __init__(self, data):
        self.data = data

    def __iter__(self):
        for hit in self.data['hits']['hits']:
            yield hit['_id']
        #TODO query with token if you ask for more then len(self)

    def __len__(self):
        return self.data['hits']['total']


class Facets(object):
    # from here down lets make new class???
    def facets(self):
        return self.data['facets']


def Response(object):

    @property
    def hits(self):
        return ResultResponse(self.data)

    def facets(self):
        return Facets(self.data)


def setup_app(app):
    #from somewhere import process_es_query, process_es_result
    ElasticSearch(app)
    print 'Elastic Search juchuuuuuu'
    #es.process_query(process_es_query)
    #es.process_result(process_es_result)

    #record_changed.connect(es.index)
