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


def index_settings():
    settings = {
            #should be set to 1 for exact facet count
            "number_of_shards": 1,

            #in case of primary shard failed
            "number_of_replicas": 1,

            #disable automatic type detection
            #that can cause errors depending of the indexing order
            "date_detection": False,
            "numeric_detection": False,
            }
    return settings
