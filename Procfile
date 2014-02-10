web: inveniomanage runserver
cache: redis-server
worker: celeryd -E -A invenio.celery.celery --loglevel=INFO --workdir=$VIRTUAL_ENV
workermon: flower --broker=redis://localhost:6379/1
indexer: /Users/marietho/Devel/elasticsearch/elasticsearch-0.90.11/bin/elasticsearch -f -D es.config=/Users/marietho/Devel/elasticsearch/elasticsearch-0.90.11/config/elasticsearch.yml
