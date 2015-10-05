from pylons import request
from ckan import logic
from ckan import model
import ckan.lib.helpers as h
import ckan.plugins as p
import bson
import pymongo
import configparser
from ckan.lib.base import c

config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient = config['ckan:odm_extensions']['mongoclient']
mongoport = config['ckan:odm_extensions']['mongoport']
client = pymongo.MongoClient(str(mongoclient), int(mongoport))
db2 = client.odm
collection = db2.jobs
catalogues = []


def harmonisation_sources_rules_list():

    catalogues[:] = []
    document = collection.find()
    i = 0
    while i < document.count():
        catalogues.append(document[i]['title'])
        i += 1
    catalogues.append("All my catalogues")
    return [{'value': f} for f in catalogues]


def harmonisation_sources_list():

    catalogues[:] = []
    document = collection.find()
    i = 0
    while i < document.count():
        catalogues.append(document[i]['title'])
        i += 1
    return [{'value': f} for f in catalogues]
