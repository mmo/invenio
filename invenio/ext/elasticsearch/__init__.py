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
Elasticsearch a search engine for invenio.

It should be able to perform:

- metadata and fulltext search almost without DB
- metadata facets such as authors, Invenio collection facets both with the
  corresponding filters
- fulltext and metadata fields highlightings
- fast collecition indexing, mid-fast metadata indexing, almost fast fulltext
  indexing

TODO:
- exceptions
- fulltext highlighting
- decide if we create one ES document type for each JsonAlchemy document type
- convert an Invenio query into a ES query
- specify the mapping, facets and sorting in JsonAlchemy
- check collection access restriction with collection filters
    - probably needs a collection exclusion list as search params
    - Note: file access restriction is not managed by the indexer
- multi-lingual support (combo plugin)
- term boosting configuration (in JsonAlchemy?)
- search by marc field support?
- connect indexing to record, collection, bibdocs change signals
- test hierachical collections
- test similar documents
- and many more...
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

        #default process functions
        self.process_query = lambda x: x
        self.process_results = lambda x: x

        # TODO: to put in config?
        self.records_doc_type = "records"
        self.bibdocs_doc_type = "bibdocs"
        self.collections_doc_type = "collections"

        # to cache recids collections
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
        """
        Specify a function to convert the invenio query into a
        ES query.
        @param handler: [function] take a query[string] parameter
        """
        self.process_query = handler

    def results_handler(self, handler):
        """
        Specify a function to convert a ES search result into an Invenio
        understanding object.
        @param handler: [function] take a query[string] parameter
        """
        self.process_results = handler

    @property
    def status(self):
        """The status of the ES cluster.
        See: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/cluster-health.html
        for more.
        TODO: is it usefull?
        @return: [string] possible values: green, yellow, red. green means all
        ok including replication, yellow means replication not active, red
        means partial results.
        """
        return self.connection.health().get("status")

    def index_exists(self, index=None):
        """Check if the index exist in the cluster.
        @param index: [string] index name
        @return: [bool] True if exists
        """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        if self.connection.status().get("indices").get(index):
            return True
        return False

    def delete_index(self, index=None):
        """Delete the given index.
        @param index: [string] index name
        @return: [bool] True if success
        """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        try:
            self.connection.delete_index(index=index)
            return True
        except:
            return False

    def create_index(self, index=None):
        """Create the given index.
        It also put basic configuration and doc types mapping.
        @param index: [string] index name
        @return: [bool] True if success
        """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        if self.index_exists(index=index):
            return True
        try:
            from .config.index import index_settings
            #create index
            self.connection.create_index(index=index, settings=index_settings())
            
            from .config.fields import records_mapping, bibdocs_mapping,\
                collections_mapping 
            #mappings
            self._mapping(index=index, doc_type=self.records_doc_type,
                    fields_mapping=records_mapping())

            self._mapping(index=index, doc_type=self.bibdocs_doc_type,
                    fields_mapping=bibdocs_mapping(),
                    parent_type=self.records_doc_type)

            self._mapping(index=index, doc_type=self.collections_doc_type,
                    fields_mapping=collections_mapping(),
                    parent_type=self.records_doc_type)

            return True
        except:
            return False

    def _mapping(self, index, doc_type, fields_mapping, parent_type=None):
        mapping = {
                doc_type: {
                    "properties": fields_mapping
                    }
                }
        if parent_type:
            mapping[doc_type]["_parent"] = {"type": parent_type}
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
        replace existing Invenio function for performance reason.
        @param recreate_cache_if_needed: [bool] True if regenerate the cache
        """
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
        """Index collections.
        Collections maps computed by webcoll is indexed into the given index in order to
        filter by collections.
        @param recids: [list of int] recids to index
        @param index: [string] index name
        @param bulk_size: [int] batch size to index
        @return: [list of int] list of recids not indexed due to errors
        """
        self.get_all_collections_record()
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        return self._index_docs(recids, self.collections_doc_type, index,
                bulk_size, self._get_collections)

    def index_bibdocs(self, recids, index=None, bulk_size=100000, **kwargs):
        """Index fulltext files.
        Put the fullext extracted by Invenio into the given index.
        @param recids: [list of int] recids to index
        @param index: [string] index name
        @param bulk_size: [int] batch size to index
        @return: [list of int] list of recids not indexed due to errors
        """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        return self._index_docs(recids, self.bibdocs_doc_type, index, bulk_size, self._get_text)

    def index_records(self, recids, index=None, bulk_size=100000, **kwargs):
        """Index bibliographic records.
        The document structure is provided by JsonAlchemy.
        Note: the __metadata__ is removed for the moment.
        TODO: is should be renamed as index?
        @param recids: [list of int] recids to index
        @param index: [string] index name
        @param bulk_size: [int] batch size to index
        @return: [list] list of recids not indexed due to errors
        """
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
        """Find simlar documents to the given recid.
        Note: not tested
        @param recid: [int] document id to find similar
        @param index: [string] index name
        @return: [list] list of recids
        """
        if index is None:
            index = self.app.config['ELASTICSEARCH_INDEX']
        fields_to_compute_similarity = ["_all"]
        return self.connection.more_like_this(index=index,
                doc_type=self.records_doc_type,
                id=recid, mlt_fields=fields_to_compute_similarity)

    def search(self, query, index=None, cc=None, c=[], f="", rg=None,
            sf=None, so="d", jrec=0, facet_filters=[], **kwargs):
        """Perform a search query.
        Note: a lot of work to do.
        @param query: [string] search query
        @param recids: [list of int] recids to index
        @param index: [string] index name
        @param cc: [string] main collection name
        @param c: [list of string] list of collection names for filter
        @param c: [list of string] list of collection names for filter
        @param f: [string] field to search (not yet used)
        @param rg: [int] number of results to return
        @param sf: [string] sort field
        @param so: [string] sort order in [d,a]
        @param jrec: [int] result offset for paging
        @param facet_filters: [list of tupple of strings] filters to prune the
            results. Each filter is defined as a tupple of term, value: (i.e.
            [("facet_authors", "Ellis, J.")])
        @return: [object] response
        """
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

        # sorting
        if sf:
            options["sort"] = [{
                "sort_%s" % sf: {
                    "order": "desc" if so == "d" else "asc"
                    }
                }]

        filters = []
        # facet_filters
        for ft in facet_filters:
            (term, value) = ft
            filters.append({
                "term": {
                    term: value
                }
            })

        # collection filters
        for col in c:
            (term, value) = ft
            filters.append({
                "has_child": {
                    "type": self.collections_doc_type,
                    "query": {
                        "term": {
                            "name": {
                                "value": col
                                }
                            }
                        }
                    }
            })

        # filters concatenation
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

        # facet configuration
        from .config.facets import records_config
        query["facets"] = records_config()

        # hightlight configuration
        from .config.highlights import records_config
        query["highlight"] = records_config()
        return self.process_results(self.connection.search(query, index=index,
                                                          **kwargs))


def setup_app(app):

    from es_query import process_es_query, process_es_results
    es = ElasticSearch(app)
    es.query_handler(process_es_query)
    es.results_handler(process_es_results)

    #record_changed.connect(es.index)
