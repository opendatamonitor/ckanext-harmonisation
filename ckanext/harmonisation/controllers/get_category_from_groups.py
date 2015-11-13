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


## search for new datasets
def Get_category_from_groups():
  counter=0
  #the collection we wanna copy datasets
  #the collection we ll store all new datasets
  odm_harmonised_collection=db.odm_harmonised
  datasets=list(odm_harmonised_collection.find());
  i=0

#groups case
  #while i<len(datasets):
	#if 'groups' in datasets[i].keys() and len(datasets[i]['groups'])>0 and 'category' not in datasets[i]['extras'].keys() and 'platform' in datasets[i].keys() and datasets[i]['platform']=='ckan':
	  #groups=datasets[i]['groups']
	  #j=0
	  #category=''
	  #while j<len(groups):
		#category=category+groups[j]+','
		#j+=1
	  #category=category.rstrip(',')
	  #datasets[i]['extras'].update({'category':category})
	  #odm_harmonised_collection.save(datasets[i])
	  #print(category)
	#i+=1

#socrata case
  while i<len(datasets):
	if 'category' in datasets[i].keys() and 'category' not in datasets[i]['extras'].keys() and 'platform' in datasets[i].keys() and datasets[i]['platform']=='socrata':
	  if datasets[i]['category']!='' and datasets[i]['category']!=None:
	  	category=datasets[i]['category']
	  
	  	datasets[i]['extras'].update({'category':category})
	  	odm_harmonised_collection.save(datasets[i])
	  	print(category)
	i+=1



Get_category_from_groups()
