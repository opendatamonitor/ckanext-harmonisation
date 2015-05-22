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
def Copy_Odm_to_Odm_harmonised():
  counter=0
  #the collection we wanna copy datasets
  odm_collection=db.odm_lic
  #the collection we ll store all new datasets
  odm_harmonised_collection=db.odm_harmonised_lic

  datasets=list(odm_collection.find());
  print(str(len(datasets)))
  i=0
  while i<len(datasets):
	old_license_id=''
	old_license_title=''
	old_license=''
	if 'license_id' in datasets[i].keys():
	  old_license_id=datasets[i]['license_id']
	if 'license_title' in datasets[i].keys():
	  old_license_title=datasets[i]['license_title']
	if 'license' in datasets[i].keys():
	  old_license=datasets[i]['license']
	#print(datasets[i])
	temp_id=datasets[i]['id']
	cat_url=datasets[i]['catalogue_url']
	
	document=odm_harmonised_collection.find_one({"id":temp_id})
	if 'license_id' in document.keys() and old_license_id!='':
	  document.update({"license_id":old_license_id})
	  odm_harmonised_collection.save(document)
	if 'license_title' in document.keys()and old_license_title!='':
	  document.update({"license_title":old_license_title})
	  odm_harmonised_collection.save(document)
	if 'license' in document.keys()and old_license!='':
	  document.update({"license":old_license})
	  odm_harmonised_collection.save(document)
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
		#counter+=1
	i+=1
  #HarmoniseLicenses(cat_url)


def HarmoniseLicenses(cat_url):
  
  point=0
  counter=0
  counter2=0
  counter_broken_links=0
  licenses1=[]
  license1=""
  license2=""
  odm_harmonised_collection=db.odm_harmonised_lic
  #from dictionaries import basic_licenses_dict
  
  doc=collection1.find_one({"cat_url":cat_url})
  user=doc['user']
  #licenses_dictionary=licenses_dict.find_one({'cat_url':str(cat_url)})
  licenses_dictionary1=licenses_dict_basic.find_one()
  licenses_dictionary2=licenses_dict_user.find_one({'user':str(user)})
  if licenses_dictionary2==None:licenses_dictionary2={}
  licenses_dictionary3=licenses_dict_catalogue.find_one({'cat_url':str(cat_url)})
  licenses_dictionary={}
  i=0
  while i<len(licenses_dictionary1):
	licenses_dictionary.update({licenses_dictionary1.keys()[i]:licenses_dictionary1[licenses_dictionary1.keys()[i]]})
	i+=1
  i=0
  while i<len(licenses_dictionary2):
	licenses_dictionary.update({licenses_dictionary2.keys()[i]:licenses_dictionary2[licenses_dictionary2.keys()[i]]})
	i+=1
  i=0
  while i<len(licenses_dictionary3):
	licenses_dictionary.update({licenses_dictionary3.keys()[i]:licenses_dictionary3[licenses_dictionary3.keys()[i]]})
	i+=1

  print(len(licenses_dictionary.keys()))
  count=0
  while count<len(licenses_dictionary):
	key=licenses_dictionary.keys()[count]
	#print(key)
	if '(dot)' in key:
	  #print(key)
	  key1=key.replace('(dot)','.')
	  #print(key1)
	  licenses_dictionary.update({key1:licenses_dictionary[key]})
	  del licenses_dictionary[key]
	count+=1
  keys=licenses_dictionary.keys()
  
  
  #keys=basic_licenses_dict.licenses_dict.keys()
  dictionary=str(licenses_dictionary).lower()
  unharmonised_licenses=[]

  datasets=list(odm_harmonised_collection.find({'catalogue_url':cat_url}))
  print(datasets)
  i=0


  while i<len(datasets):
	counter2+=1
	if 'harmonised' not in datasets[i].keys():
	  datasets[i].update({'harmonised':{}})
	  odm_harmonised_collection.save(datasets[i])
	if 'harmonised_Licenses' not in datasets[i]['harmonised'].keys() or 'harmonised_Licenses' in datasets[i]['harmonised'].keys():
		try:
		  found=False
		  license=""
		  license1=""
		  license2=""

		  try:
			license=datasets[i]['license_title']
		  except KeyError:
			license=""
		  try:
			license1=datasets[i]['license_id']

		  except KeyError:
			license1=""
		  try:
			license2=datasets[i]['license']
		  except KeyError:
			license2=""

		  if license!=""and license!=None and found==False:
			  try:
				license=license.encode('utf-8')
			  except:
				license=""
			  j=0
			  while j<len(keys):
				if unicode(license,'utf-8').lower().strip() == keys[j].lower().strip():
				  datasets[i].update({"license_id":str(licenses_dictionary[keys[j]])})
				  datasets[i]['harmonised'].update({'harmonised_Licenses':True})
				  datasets[i]['harmonised'].update({'harmonised_Licenses_date':datetime.now()})
				  odm_harmonised_collection.save(datasets[i])
				  found=True
				  counter+=1
				if str(license).lower().strip() not in dictionary:

				  if str(license).lower().strip() not in unharmonised_licenses:
					  unharmonised_licenses.append(str(license).lower().strip())
				j+=1

		  if license1!=""and license1!=None and found==False:
			  try:
				license1=license1.encode('utf-8')
			  except:
				license1=""
			  j=0
			  while j<len(keys):

				if unicode(license1,'utf-8').lower().strip() == keys[j].lower().strip() :
				  datasets[i].update({"license_id":str(licenses_dictionary[keys[j]].encode('utf-8'))})
				  datasets[i]['harmonised'].update({'harmonised_Licenses':True})
				  datasets[i]['harmonised'].update({'harmonised_Licenses_date':datetime.now()})
				  odm_harmonised_collection.save(datasets[i])
				  found=True
				  counter+=1
				if str(license1).lower().strip() not in dictionary:

				  if str(license1).lower().strip() not in unharmonised_licenses:
					  unharmonised_licenses.append(str(license1).lower().strip())

				j+=1

		  if license2!=""and license2!=None and found==False:
			  try:
				license2=license2.encode('utf-8')
			  except:
				license2=""
			  j=0
			  while j<len(keys):
				if unicode(license2,'utf-8').lower().strip()  == keys[j].lower().strip() :
				  datasets[i].update({"license_id":str(licenses_dictionary[keys[j]])})
				  datasets[i]['harmonised'].update({'harmonised_Licenses':True})
				  datasets[i]['harmonised'].update({'harmonised_Licenses_date':datetime.now()})
				  odm_harmonised_collection.save(datasets[i])
				  found=True
				  counter+=1
				if str(license2).lower().strip() not in dictionary:

				  if str(license2).lower().strip() not in unharmonised_licenses:
					  unharmonised_licenses.append(str(license2).lower().strip())
				j+=1

		  if "license_id" not in str(datasets[i]):

			  try:
				license1=datasets[i]['license_title']
			  except KeyError:
				try:
				  license1=datasets[i]['license']
				except KeyError:
				  license1=""
			  try:
				license1=license1.encode('utf-8')
			  except:
				license1=""
			  j=0

			  if license1!="" and license1!=None:
				datasets[i].update({"license_id":str(license1)})
				datasets[i]['harmonised'].update({'harmonised_Licenses':True})
				datasets[i]['harmonised'].update({'harmonised_Licenses_date':datetime.now()})
				odm_harmonised_collection.save(datasets[i])
				license2=datasets[i]['license_title']

			  if license2!="":
				while j<len(keys):
				  if str(license1) == keys[j] or str(license1).lower() == keys[j].lower():
					datasets[i].update({"license_id":str(licenses_dictionary[str(keys[j])])})
					datasets[i]['harmonised'].update({'harmonised_Licenses':True})
					datasets[i]['harmonised'].update({'harmonised_Licenses_date':datetime.now()})
					odm_harmonised_collection.save(datasets[i])

				  j+=1

		  i+=1
		except KeyError:
		  i+=1
	else:i+=1

  datasets[:]=[]
	#licenses1.sort()
  #k=0
  #while k<len(licenses1):
	#print('"'+str(licenses1[k].encode('utf-8'))+'":"'+str(licenses1[k].encode('utf-8')+'",'))
	#k+=1

  print('\n'+'\n'+"harmonised license in "+str(counter)+" datasets")

  i1=0

  while i1<len(unharmonised_licenses):

	i1+=1
  print("The unharmonised licenses are in the harmonisation_engine_log.txt file."+'\n'+'\n'+'\n')
  message="Harmonised license in: "+str(counter)+ " datasets"+'\n'
  return unharmonised_licenses,message




Copy_Odm_to_Odm_harmonised()
#jobs=list(collection1.find());
#j=0
#while j<len(jobs):
  #print(j)
  #temp_cat_url=jobs[j]['cat_url']
  ##HarmoniseLicenses(temp_cat_url)
  #j+=1