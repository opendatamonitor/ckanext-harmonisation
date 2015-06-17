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


## search for new datasets
def Find_unharmonised_categories():
  counter=0
  #the collection we wanna copy datasets
  unharmonised=db.odm_harmonised

  datasets=list(unharmonised.find());
  i=0
  while i<len(datasets):
	if datasets[i]['license_id']=='License Not Specified':
	  datasets[i].update({'license_id':'license not specified'})
	  unharmonised.save(datasets[i])
	i+=1
  


Find_unharmonised_categories()
