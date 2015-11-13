from pylons import request
from ckan import logic
from ckan import model
import ckan.lib.helpers as h
import ckan.plugins as p
import bson
import pymongo
import configparser
from ckan.lib.base import c
import re


config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']
client = pymongo.MongoClient(str(mongoclient), int(mongoport))
db2 = client.odm
collection=db2.jobs
catalogues=[]
collection_groups = client['odm']['category_groups']


def harmonisation_sources_rules_list():
    catalogues[:]=[]
    document=collection.find()
    i=0
    while i<document.count():
        try:
            catalogues.append(document[i]['title'])
        except KeyError,e:
            print(e,document[i])
        i+=1
    catalogues.sort()
    catalogues.append("All my catalogues")
    return [{ 'value': f} for f in reversed(catalogues)]

def harmonisation_sources_list():
    catalogues[:]=[]
    document=collection.find()
    i=0
    while i<document.count():
        try:
            catalogues.append(document[i]['title'])
        except KeyError,e:
            print(e,document[i])
        i+=1
    return [{ 'value': f} for f in catalogues]


def cat_list(extra_elems=[]):
    catalogues=[]
    document=collection.find()
    i=0
    while i<document.count():
        try:
            catalogues.append(document[i]['title'])
        except KeyError,e:
            print(e,document[i])
        i+=1
    catalogues.sort(reverse=True)
    # catalogues.extend(["All catalogues","All my catalogues"])
    if extra_elems:
        catalogues.extend(extra_elems)

    return catalogues


def get_category_groups(category_type=0):
    categories=[]

    document=collection_groups.find_one()
    del document['_id']

    for key in sorted(document,key=natural_keys):
        categories.append(document[key][category_type])

    seen = set()
    seen_add = seen.add

    return [x for x in categories if not (x in seen or seen_add(x))]

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]

def escape_key(key,replace="\uFF0E"):
    return key.replace(".",replace)


def unescape_key(key,replace="\uFF0E"):
    return key.replace(replace,".")
