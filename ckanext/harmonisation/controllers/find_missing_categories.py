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


collection1=db.jobs
licenses_dict_catalogue=db.licenses_dict_catalogue
licenses_dict_basic=db.licenses_dict_basic
licenses_dict_user=db.licenses_dict_user

## search for new datasets
def Find_missing_categories():

  #the collection we wanna copy datasets
  odm_collection=db.odm
  #the collection we ll store all new datasets
  odm_harmonised_collection=db.odm_harmonised

 # datasets=list(odm_collection.find());
  #print(str(len(datasets)))
  harmonised_datasets=list(odm_harmonised_collection.find());
  counter=0
  j=0
  while j<len(harmonised_datasets):
	if 'category' not in harmonised_datasets[j]['extras'].keys():
	  temp_id=harmonised_datasets[j]['id']
	  find_odm_document=odm_collection.find_one({'id':temp_id})
	  #print(find_odm_document)
	  if 'category' in str(find_odm_document['extras']):
		print(temp_id)
		counter+=1
	j+=1

  print(counter)

Find_missing_categories()
