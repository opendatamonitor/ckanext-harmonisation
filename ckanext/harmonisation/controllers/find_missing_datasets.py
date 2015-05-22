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
def Find_missing_datasets():
  counter=0
  #the collection we wanna copy datasets
  odm_collection=db.odm
  #the collection we ll store all new datasets
  odm_harmonised_collection=db.odm_harmonised

  datasets=list(odm_collection.find());
  #print(str(len(datasets)))
  harmonised_datasets=list(odm_harmonised_collection.find());
  harmonised_ids=[]
  j=0
  while j<len(harmonised_datasets):
	try:
		harmonised_ids.append(harmonised_datasets[j]['id'])
	except:pass
	j+=1
  #print(str(len(harmonised_datasets)))
  i=0
  while i<len(datasets):
	try:
		id_temp=datasets[i]['id']
	except:id_temp=''
	if id_temp not in harmonised_ids and id_temp!='':
	  
	  
	  print(str(id_temp)+'  '+str(datasets[i]['catalogue_url']))
	i+=1

def Find_deleted_categories():
	harmonised_datasets=list(odm_harmonised_collection.find());
	i=0
	while i<len(harmonised_datasets):
	  i+=1
	#print(i)
	#old_license_id=''
	#old_license_title=''
	#old_license=''
	#if 'license_id' in datasets[i].keys():
	  #old_license_id=datasets[i]['license_id']
	#if 'license_title' in datasets[i].keys():
	  #old_license_title=datasets[i]['license_title']
	#if 'license' in datasets[i].keys():
	  #old_license=datasets[i]['license']
	##print(datasets[i])
	#try:
		#temp_id=datasets[i]['id']
		#cat_url=datasets[i]['catalogue_url']
	
		#document=odm_harmonised_collection.find_one({"id":temp_id})
		#if 'license_id' in document.keys() and old_license_id!='':
		  #document.update({"license_id":old_license_id})
		  #odm_harmonised_collection.save(document)
		#if 'license_title' in document.keys()and old_license_title!='':
		  #document.update({"license_title":old_license_title})
		  #odm_harmonised_collection.save(document)
		#if 'license' in document.keys()and old_license!='':
		  #document.update({"license":old_license})
		  #odm_harmonised_collection.save(document)
		#print(document['license_id'])
		#if 'copied' not in datasets[i].keys():
			#datasets[i].update({"copied":True})
			#odm_harmonised_collection.save(datasets[i])
			#odm_collection.save(datasets[i])
			#counter+=1

		#if 'copied' in datasets[i].keys() and 'updated_dataset' in datasets[i].keys():
			#temp_id=datasets[i]['id']
			#document=odm_harmonised_collection.find_one({"id":temp_id})
			#if len(document.keys())>1:
			  #odm_harmonised_collection.remove({"id":temp_id})
			#odm_harmonised_collection.save(datasets[i])
			##counter+=1
	#except:pass
	#i+=1
  #HarmoniseLicenses(cat_url)

Find_deleted_categories()
#Find_missing_datasets()