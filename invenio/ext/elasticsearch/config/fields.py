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
General config file for ES index.
"""

def collections_mapping():
    mapping = {
        "recid": {"type": "integer"},
        "name": {
            "type": "string",
            "analyzer": "keyword"}}

    return mapping


def bibdocs_mapping():
    mapping = {
            #force recid type to integer for default sorting
            "recid": {"type": "integer"},
            "fulltext": {"type": "string"},
            }
    return mapping


def records_mapping():
    mapping = {
            #force recid type to integer for default sorting
            "recid": {"type": "integer"},

            "email": {"type": "string", "index": "not_analyzed"},
            "primary_report_number": {
                "type": "string",
                "index": "not_analyzed"},
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
                        # can be accessible with "title"
                        # instead of "title.title"
                        "index_name": "title",
                        "type": "multi_field",
                        "fields": {
                            "title": {
                                "type": "string",
                                "analyzer": "standard"
                                },
                            "sort_title": {
                                "type": "string",
                                "analyzer": "simple"
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
                        "type": "multi_field",
                        "fields": {
                            "authors": {
                                "type": "string",
                                "analyzer": "standard"},
                            "facet_authors": {"type": "string",
                                "analyzer": "keyword"}
                            }
                        }
                    }
                }
            }
    return mapping
