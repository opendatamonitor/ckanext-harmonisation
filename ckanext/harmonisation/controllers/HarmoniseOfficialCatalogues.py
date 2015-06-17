import pymongo
from pymongo import errors
import sys
import json
import bson
import urllib2
import FindFormat
from bs4 import BeautifulSoup
import requests
import time
import re
import dateutil.parser
from date_parser.date_parser import parse
from datetime import datetime
import pytz
import socket
from dateutil import parser
import goslate
from planar import BoundingBox
from country_bounding_boxes import (
        country_subunits_containing_point,
        country_subunits_by_iso_code
        )

import lepl.apps.rfc3696
import custom_logging
import logging
import configparser 
import dictionaries
import csv
MaxDocsInDB=400000

##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']
log_path=config['ckan:odm_extensions']['log_path']
harmonisation_engine_log=str(log_path)+'harmonisation_engine/harmonisation_engine_log.txt'

client1=pymongo.MongoClient(str(mongoclient), int(mongoport))
db=client1.odm


collection_jobs=db.jobs


## search for new datasets
def FindOfficialCatalogues():
  counter=0
  official_list={}
  reader = csv.reader(open('OpenDataMonitor_ harvested catalogues - ODM harvested.csv', "rb"), delimiter=',', quoting=csv.QUOTE_NONE, escapechar="\\")
  for row in reader:
	main_cat_url=''
	official=''
	cat_url=row[1]
	if 'https' in cat_url:
	  main_cat_url=cat_url[8:]
	  main_cat_url=main_cat_url[:main_cat_url.find('/')]
	  main_cat_url='https://'+main_cat_url
	  #print(main_cat_url)
	if 'http://' in cat_url:
	  main_cat_url=cat_url[7:]
	  main_cat_url=main_cat_url[:main_cat_url.find('/')]
	  main_cat_url='http://'+main_cat_url
	  #print(main_cat_url)
	  #({"name": /.*m.*/})
	if main_cat_url=='http://opendata.euskadi.net':
		main_cat_url='http://opendata.euskadi.eus'
	if main_cat_url=='https://datos.upo.gob.es':
		main_cat_url='http://catalogo.upo.gob.es'
	if main_cat_url=='http://gobiernoabierto.gobex.es':
		main_cat_url='http://ckan.gobex.es'
	document=collection_jobs.find_one({"cat_url":{'$regex': main_cat_url}})
	if row[5]=='Y':
		official=True
	if row[5]=='N':
		official=False
	if row[5]=='U':
		official='U'
	document.update({"official":official})
	collection_jobs.save(document)
	
	  
  print("counter: "+str(counter))
  datasets=list(collection_jobs.find());
  print(str(len(datasets)))
  i=0
  while i<len(datasets):
	
	i+=1
	
FindOfficialCatalogues()