from ckan.lib.base import BaseController, c
from ckan.controllers.package import PackageController
import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.logic as logic
import ckan.model as model
from ckanext.harvestodm.logic.action.create import harvest_source_create
from ckan.lib.base import c
from ckan.model import Session, Package
from ckan.common import OrderedDict, _, json, request, c, g, response
import ckan.lib.navl.dictization_functions as dict_fns
from ckanext.harmonisation.plugin import DATASET_TYPE_NAME
from ckanext.harvestodm.plugin import _create_harvest_source_object
import json
import pymongo
import bson
import configparser
import logging
##read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient=config['ckan:odm_extensions']['mongoclient']
mongoport=config['ckan:odm_extensions']['mongoport']

log = logging.getLogger(__name__)
render = base.render
abort = base.abort
redirect = base.redirect
get_action = logic.get_action
clean_dict = logic.clean_dict
tuplize_dict = logic.tuplize_dict
parse_params = logic.parse_params
NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
client = pymongo.MongoClient(str(mongoclient), int(mongoport))



class CustomHarmonisationController(PackageController):


  def __before__(self, action, **params):

	  super(CustomHarmonisationController, self).__before__(action, **params)

	  c.dataset_type = DATASET_TYPE_NAME




  def main_dashboard(self, data=None):
	
	  data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
	  errors=''
	  vars={}
	
	##unharmonise
	  unharmonise_action = request.params.get('unharmonise')
	  if unharmonise_action == 'unharmonise-catalogue':

		db = client.odm
		
		collection_jobs=db.jobs
		document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
		cat_url=document_job['cat_url']
		
		if 'harmonisation' in document_job:
		  del document_job['harmonisation']
		  collection_jobs.save(document_job)
		if 'harmonised' in document_job:
		  del document_job['harmonised']
		  collection_jobs.save(document_job)
		
		odm_collection=db.odm
		odm_harmonised_collection=db.odm_harmonised
		odm_harmonised_collection.remove({'catalogue_url':cat_url})
		datasets=list(odm_collection.find({'catalogue_url':cat_url}));
		print(str(len(datasets)))
		
		i=0
		while i<len(datasets):
		  if 'copied' in datasets[i].keys():
			  #print(datasets[i])
			  del datasets[i]['copied']
			  odm_collection.save(datasets[i])
		  i+=1
	
	## save
	  save_action = request.params.get('save')
	  if save_action == 'go-harmonisation-complete':
		
		db = client.odm
		collection=db.jobs
		document=collection.find_one({"title":str(data['catalogue_selection'])})
		data['id']=document['id']
		data['cat_url']=document['cat_url']
		try:
		  harmonised=document['harmonised']
		except KeyError:
		  harmonised="not yet"
		data['harmonised']=harmonised
		data['status']="pending"
		vars = {'data': data, 'errors': errors}
	
	##create harmonise job to db
		collection=db.harmonise_jobs
		harvest_job=data
		collection.save(harvest_job)

	##view info
	  info_action = request.params.get('info')
	  if info_action == 'get-catalogue-info':
		db = client.odm
		collection=db.jobs
		document=collection.find_one({"title":str(data['catalogue_selection'])})
		data['id']=document['id']
		data['cat_url']=document['cat_url']
		try:
		  harmonised=document['harmonised']
		except KeyError:
		  harmonised="not yet"
		  
		try:
		  harmonised_basic=document['harmonisation']['harmonised_basic']
		except KeyError:
		  harmonised_basic="unharmonised"
		  
		try:
		  harmonised_basic_date=document['harmonisation']['harmonised_basic_date']
		except KeyError:
		  harmonised_basic_date=""
		  
		try:
		  message_HarmoniseTags=document['harmonisation']['message_HarmoniseTags']
		except KeyError:
		  message_HarmoniseTags=""
		  
		try:
		  message_Copy_Odm_to_Odm_harmonised=document['harmonisation']['message_Copy_Odm_to_Odm_harmonised']
		except KeyError:
		  message_Copy_Odm_to_Odm_harmonised=""
		
		try:
		  message_HarmoniseExtras=document['harmonisation']['message_HarmoniseExtras']
		except KeyError:
		  message_HarmoniseExtras=""
		
		try:
		  message_HarmoniseStringsToIntegers=document['harmonisation']['message_HarmoniseStringsToIntegers']
		except KeyError:
		  message_HarmoniseStringsToIntegers=""
		  
		try:
		  message_HarmoniseDatesLabels=document['harmonisation']['message_HarmoniseDatesLabels']
		except KeyError:
		  message_HarmoniseDatesLabels=""
		  
		try:
		  message_HarmoniseReleaseDates=document['harmonisation']['message_HarmoniseReleaseDates']
		except KeyError:
		  message_HarmoniseReleaseDates=""
		
		try:
		  message_HarmoniseUpdateDates=document['harmonisation']['message_HarmoniseUpdateDates']
		except KeyError:
		  message_HarmoniseUpdateDates=""
		
		try:
		  message_HarmoniseMetadataCreated=document['harmonisation']['message_HarmoniseMetadataCreated']
		except KeyError:
		  message_HarmoniseMetadataCreated=""
		  
		try:
		  message_HarmoniseMetadataModified=document['harmonisation']['message_HarmoniseMetadataModified']
		except KeyError:
		  message_HarmoniseMetadataModified=""
		  
		
		try:
		  harmonised_dates=document['harmonisation']['harmonised_dates']
		except KeyError:
		  harmonised_dates="unharmonised"
		  
		try:
		  harmonised_dates_date=document['harmonisation']['harmonised_dates_date']
		except KeyError:
		  harmonised_dates_date=""

		try:
		  harmonised_resources=document['harmonisation']['harmonised_resources']
		except KeyError:
		  harmonised_resources="unharmonised"
		  
		try:
		  message_HarmoniseBadFormats=document['harmonisation']['message_HarmoniseBadFormats']
		except KeyError:
		  message_HarmoniseBadFormats=""
		  
		try:
		  message_HarmoniseMimetypes=document['harmonisation']['message_HarmoniseMimetypes']
		except KeyError:
		  message_HarmoniseMimetypes=""
		
		try:
		  message_HarmoniseSizes=document['harmonisation']['message_HarmoniseSizes']
		except KeyError:
		  message_HarmoniseSizes=""
		
		try:
		  message_HarmoniseFormats=document['harmonisation']['message_HarmoniseFormats']
		except KeyError:
		  message_HarmoniseFormats=""
		  
		try:
		  message_HarmoniseNumTagsAndResources=document['harmonisation']['message_HarmoniseNumTagsAndResources']
		except KeyError:
		  message_HarmoniseNumTagsAndResources=""
		
		
		try:
		  harmonised_resources_date=document['harmonisation']['harmonised_resources_date']
		except KeyError:
		  harmonised_resources_date=""

		try:
		  harmonised_licenses=document['harmonisation']['harmonised_licenses']
		except KeyError:
		  harmonised_licenses="unharmonised"
		  
		
		try:
		  message_HarmoniseLicenses=document['harmonisation']['message_HarmoniseLicenses']
		except KeyError:
		  message_HarmoniseLicenses=""
		
		try:
		  harmonised_licenses_date=document['harmonisation']['harmonised_licenses_date']
		except KeyError:
		  harmonised_licenses_date=""
		  
		try:
		  harmonised_languages=document['harmonisation']['harmonised_languages']
		except KeyError:
		  harmonised_languages="unharmonised"
		  
		try:
		  message_HarmoniseLanguageLabels=document['harmonisation']['message_HarmoniseLanguageLabels']
		except KeyError:
		  message_HarmoniseLanguageLabels=""
		
		try:
		  message_HarmoniseLanguages=document['harmonisation']['message_HarmoniseLanguages']
		except KeyError:
		  message_HarmoniseLanguages=""
		  
		
		try:
		  harmonised_languages_date=document['harmonisation']['harmonised_languages_date']
		except KeyError:
		  harmonised_languages_date=""
		  
		try:
		  harmonised_categories=document['harmonisation']['harmonised_categories']
		except KeyError:
		  harmonised_categories="unharmonised"
		  
		try:
		  message_HarmoniseCategoryLabels=document['harmonisation']['message_HarmoniseCategoryLabels']
		except KeyError:
		  message_HarmoniseCategoryLabels=""
		 
		try:
		  message_HarmoniseCategories=document['harmonisation']['message_HarmoniseCategories']
		except KeyError:
		  message_HarmoniseCategories=""
		
		
		try:
		  harmonised_categories_date=document['harmonisation']['harmonised_categories_date']
		except KeyError:
		  harmonised_categories_date=""
		  
		  
		try:
		  harmonised_countries=document['harmonisation']['harmonised_countries']
		except KeyError:
		  harmonised_countries="unharmonised"
		  
		try:
		  harmonised_countries_date=document['harmonisation']['harmonised_countries_date']
		except KeyError:
		  harmonised_countries_date=""
		  

		try:
		  status=document['harmonisation']['harmonisation_status']
		except KeyError:
		  status="unharmonised"
		  
		data['harmonised']=harmonised
		data['status']=status
		
		data['harmonised_basic']=harmonised_basic
		data['harmonised_basic_date']=harmonised_basic_date
		
		data['harmonised_dates']=harmonised_dates
		data['harmonised_dates_date']=harmonised_dates_date
		
		data['harmonised_resources']=harmonised_resources
		data['harmonised_resources_date']=harmonised_resources_date
		
		data['harmonised_licenses']=harmonised_licenses
		data['harmonised_licenses_date']=harmonised_licenses_date
		
		data['harmonised_languages']=harmonised_languages
		data['harmonised_languages_date']=harmonised_languages_date
		
		data['harmonised_categories']=harmonised_categories
		data['harmonised_categories_date']=harmonised_categories_date
		
		data['harmonised_countries']=harmonised_countries
		data['harmonised_countries_date']=harmonised_countries_date
		
		data['message_Copy_Odm_to_Odm_harmonised']=message_Copy_Odm_to_Odm_harmonised
		data['message_HarmoniseTags']=message_HarmoniseTags
		data['message_HarmoniseExtras']=message_HarmoniseExtras
		data['message_HarmoniseStringsToIntegers']=message_HarmoniseStringsToIntegers
		
		data['message_HarmoniseDatesLabels']=message_HarmoniseDatesLabels
		data['message_HarmoniseReleaseDates']=message_HarmoniseReleaseDates
		data['message_HarmoniseUpdateDates']=message_HarmoniseUpdateDates
		data['message_HarmoniseMetadataCreated']=message_HarmoniseMetadataCreated
		data['message_HarmoniseMetadataModified']=message_HarmoniseMetadataModified

		data['message_HarmoniseBadFormats']=message_HarmoniseBadFormats
		data['message_HarmoniseMimetypes']=message_HarmoniseMimetypes
		data['message_HarmoniseSizes']=message_HarmoniseSizes
		data['message_HarmoniseFormats']=message_HarmoniseFormats
		data['message_HarmoniseNumTagsAndResources']=message_HarmoniseNumTagsAndResources
		
		data['message_HarmoniseLanguageLabels']=message_HarmoniseLanguageLabels
		data['message_HarmoniseLanguages']=message_HarmoniseLanguages
		
		data['message_HarmoniseLicenses']=message_HarmoniseLicenses
		
		data['message_HarmoniseCategoryLabels']=message_HarmoniseCategoryLabels
		data['message_HarmoniseCategories']=message_HarmoniseCategories
		vars = {'data': data, 'errors': errors}


	  return render('dashboard.html', extra_vars=vars)





  def edit_rules(self,data=None):

	  data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
	  errors=''
	  #print(data)

	  
## save and delete mappings	  
	  remove_and_save_action = request.params.get('remove-and-save')
	  if remove_and_save_action == 'editrules-complete':
		
		db = client.odm
		collection_jobs=db.jobs
##Per catalogue- case
		if data['catalogue_selection']!="All my catalogues":
		  document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
		  cat_url=document_job['cat_url']

		  if data['Select Rules Category']=='formats':
				collection=db.new_formats_mappings
				collection1=db.formats_dict_catalogue
		  if data['Select Rules Category']=='licenses':
			  collection=db.new_licenses_mappings
			  collection1=db.licenses_dict_catalogue
		  if data['Select Rules Category']=='categories_labels':
			  collection=db.new_categories_mappings
			  collection1=db.categories_dict_catalogue
		  if data['Select Rules Category']=='categories_values':
			  collection=db.new_categories_values_mappings
			  collection1=db.categories_values_dict_catalogue
		  if data['Select Rules Category']=='dates':
			  collection=db.new_dates_mappings
			  collection1=db.dates_dict_catalogue

		  i=0
		  while i<len(data.keys()):
			if 'custom_deleted-' in data.keys()[i]:
			  deleted_mapping=data.keys()[i][15:]
			  document=collection.find_one({'cat_url':str(cat_url)})
			  #print(document)
			  #print(deleted_mapping)
			  del document[unicode(deleted_mapping)]
			  collection.save(document)
			  
			i+=1
			
		  
		  document=collection.find_one({'cat_url':str(cat_url)})
		  document1=collection1.find_one({'cat_url':str(cat_url)})
		  print(document1)
		  #print('document1: ')
		  #print(document1)
		  if document!=None:
			del document['_id']
		  # print('+* storing *+')
			j=0
			while j<len(document.keys()):
			  
			  document1.update({document.keys()[j]:document[unicode(document.keys()[j])]})
			  collection1.save(document1)
			  j+=1
			#print(document)
			#document1.update(document)
			#collection1.save(document)
		  #print(document1)
		  #if len(document.keys())>=1:
			  #collection1.save(document)
		  i=0
		  document=collection.find_one({'cat_url':str(cat_url)})
		  
		  while i<len(document.keys()):
			if document.keys()[i]!='_id' and document.keys()[i]!='cat_url' and document.keys()[i]!='user':
			  del document[document.keys()[i]]
			  i-=1
			i+=1
		  collection.save(document)
		
##All my catalogues -case
		if data['catalogue_selection']=="All my catalogues":
		  #document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
		  #cat_url=document_job['cat_url']

		  if data['Select Rules Category']=='formats':
				collection=db.new_formats_mappings_user
				collection1=db.formats_dict_user
		  if data['Select Rules Category']=='licenses':
			  collection=db.new_licenses_mappings_user
			  collection1=db.licenses_dict_user
		  if data['Select Rules Category']=='categories_labels':
			  collection=db.new_categories_mappings_user
			  collection1=db.categories_dict_user
		  if data['Select Rules Category']=='categories_values':
			  collection=db.new_categories_values_mappings_user
			  collection1=db.categories_values_dict_user
		  if data['Select Rules Category']=='dates':
			  collection=db.new_dates_mappings_user
			  collection1=db.dates_dict_user

		  i=0
		  while i<len(data.keys()):
			if 'custom_deleted-' in data.keys()[i]:
			  deleted_mapping=data.keys()[i][15:]
			  document=collection.find_one({'user':str(c.user)})
			  #print(document)
			  #print(deleted_mapping)
			  del document[unicode(deleted_mapping)]
			  collection.save(document)
			  
			i+=1
			
		  
		  document=collection.find_one({'user':str(c.user)})
		  document1=collection1.find_one({'user':str(c.user)})
		  if document1==None: document1={}
		  print(document1)
		  #print('document1: ')
		  #print(document1)
		  if document!=None:
			del document['_id']
		  # print('+* storing *+')
			j=0
			while j<len(document.keys()):
			  
			  document1.update({document.keys()[j]:document[unicode(document.keys()[j])]})
			  collection1.save(document1)
			  j+=1
		  i=0
		  document=collection.find_one({'user':str(c.user)})
		  
		  while i<len(document.keys()):
			if document.keys()[i]!='_id' and document.keys()[i]!='user':
			  del document[document.keys()[i]]
			  i-=1
			i+=1
		  collection.save(document)
		





## view mappings

	  db = client.odm
##per catalogue - case
	  if "catalogue_selection" in data and data['catalogue_selection']!="All my catalogues":
		if 'Select Rules Category' in data.keys():
			if data['Select Rules Category']=='formats':
				collection=db.new_formats_mappings
			if data['Select Rules Category']=='licenses':
			  collection=db.new_licenses_mappings
			if data['Select Rules Category']=='categories_labels':
			  collection=db.new_categories_mappings
			if data['Select Rules Category']=='categories_values':
			  collection=db.new_categories_values_mappings
			if data['Select Rules Category']=='dates':
			  collection=db.new_dates_mappings
			  
			data['harmonise_category_selection']=data['Select Rules Category']

		#print(data)
		try:
		  
		  collection_jobs=db.jobs
		  document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
		  cat_url=document_job['cat_url']
		  document=collection.find_one({'cat_url':str(cat_url)})
		  del document['_id']
		  del document['cat_url']
		  del document['user']
		except: document={}
		data['new_formats_mappings']=document

##All my catalogues - case
	  if "catalogue_selection" in data and data['catalogue_selection']=="All my catalogues":
		if 'Select Rules Category' in data.keys():
			if data['Select Rules Category']=='formats':
				collection=db.new_formats_mappings_user
			if data['Select Rules Category']=='licenses':
			  collection=db.new_licenses_mappings_user
			if data['Select Rules Category']=='categories_labels':
			  collection=db.new_categories_mappings_user
			if data['Select Rules Category']=='categories_values':
			  collection=db.new_categories_values_mappings_user
			if data['Select Rules Category']=='dates':
			  collection=db.new_dates_mappings_user
			  
			data['harmonise_category_selection']=data['Select Rules Category']

		#print(data)
		try:
		  
		  #collection_jobs=db.jobs
		  #document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
		  #cat_url=document_job['cat_url']
		  document=collection.find_one({'user':str(c.user)})
		  del document['_id']
		  del document['user']
		except: document={}
		data['new_formats_mappings']=document
	  
	  vars = {'data': data, 'errors': errors}
	  
	  
	  if c.user=='admin':
		return render('editrules.html', extra_vars=vars)
	  else:
		abort(401, _('Not authorized to see this page'))
	  




  def view_rules(self, data=None):
	  errors=''
	  rules_action = request.params.get('rules')
	  
##save rules
	  save_action = request.params.get('save')
	  if save_action == 'viewrules-complete':
		  data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
		 # print(data)
		  print(data)
		  #print(data['Select Rules Category'])
		  db = client.odm
		  collection_jobs=db.jobs
		  
##Case - per catalogue
		  if data['catalogue_selection']!="All my catalogues":
			document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
			if data['catalogue_selection']!="All my catalogues":
			  cat_url=document_job['cat_url']
			
			if data['Select Rules Category']=='formats':
			  collection_new=db.new_formats_mappings
			  collection_unharmonised=db.unharmonised_formats
			if data['Select Rules Category']=='licenses':
			  collection_new=db.new_licenses_mappings
			  collection_unharmonised=db.unharmonised_licenses
			if data['Select Rules Category']=='categories_labels':
			  collection_new=db.new_categories_mappings
			if data['Select Rules Category']=='categories_values':
			  collection_new=db.new_categories_values_mappings
			  collection_unharmonised=db.unharmonised_category_values
			if data['Select Rules Category']=='dates':
			  collection_new=db.new_dates_mappings
			try:
			  document=collection_new.find_one({'cat_url':str(cat_url)})
			  if data['new_key']!="":
				document.update({data['new_key']:data['new_value']})
				document.update({'cat_url':cat_url})
				document.update({'user':c.user})
				collection_new.save(document)
			except:
			  if data['new_key']!="":
				document={data['new_key']:data['new_value']}
				document.update({'cat_url':cat_url})
				document.update({'user':c.user})
				collection_new.save(document)
			
			##store the unharmonised mappings to new_mappings db
			i=0
			while i<len(data):
			  try:
				document=collection_new.find_one({'cat_url':str(cat_url)})
				if 'unharmonised_value' in data.keys()[i]:
				  if data[str(data.keys()[i])]!="":
					temp_key=str(data.keys()[i])[19:]
					document.update({temp_key:data[str(data.keys()[i])]})
					document.update({'cat_url':cat_url})
					document.update({'user':c.user})
					collection_new.save(document)
					##remove from unharmonised
					document_unharm=collection_unharmonised.find_one({'cat_url':str(cat_url)})
					if document_unharm!=None:
					  #print('trying to delete')
					  del document_unharm[str(temp_key)]
					  collection_unharmonised.save(document_unharm)
				i+=1
			  except:
				if 'unharmonised_value' in data.keys()[i]:
				  if data[data.keys()[i]]!="":
					print(data.keys()[i])
					temp_key=data.keys()[i][19:]
					document={temp_key:data[str(data.keys()[i])]}
					document.update({'cat_url':cat_url})
					document.update({'user':c.user})
					collection_new.save(document)
					##remove from unharmonised
					document_unharm=collection_unharmonised.find_one({'cat_url':str(cat_url)})
					if document_unharm!=None:
					  #print('trying to delete')
					  print(temp_key)
					  
					  del document_unharm[unicode(temp_key)]
					  collection_unharmonised.save(document_unharm)
				i+=1
				
##All my catalogues - case
		  if data['catalogue_selection']=="All my catalogues":
			#document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
			#if data['catalogue_selection']!="All my catalogues":
			  #cat_url=document_job['cat_url']
			
			if data['Select Rules Category']=='formats':
			  collection_new=db.new_formats_mappings_user
			if data['Select Rules Category']=='licenses':
			  collection_new=db.new_licenses_mappings_user
			if data['Select Rules Category']=='categories_labels':
			  collection_new=db.new_categories_mappings_user
			if data['Select Rules Category']=='categories_values':
			  collection_new=db.new_categories_values_mappings_user
			if data['Select Rules Category']=='dates':
			  collection_new=db.new_dates_mappings_user
			try:
			  document=collection_new.find_one({'user':str(c.user)})
			  if data['new_key']!="":
				document.update({data['new_key']:data['new_value']})
				document.update({'user':c.user})
				collection_new.save(document)
			except:
			  if data['new_key']!="":
				document={data['new_key']:data['new_value']}
				document.update({'user':c.user})
				collection_new.save(document)
  

				
##  view rules	  
	  if rules_action == 'view-rules':
		   
		  ## edw na prosthesw pote tha emfanizetai i kathe lista
		  db = client.odm
		  data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
		  print(data)
		  collection_jobs=db.jobs

##Case - per catalogue
		  if data['catalogue_selection']!="All my catalogues":
			document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
			cat_url=document_job['cat_url']
			
			if data['Select Rules Category']=='formats':
			  collection=db.formats_dict_catalogue
			  collection1=db.unharmonised_formats
			  collection2=db.formats_dict_basic
			if data['Select Rules Category']=='licenses':
			  collection=db.licenses_dict_catalogue
			  collection1=db.unharmonised_licenses
			  collection2=db.licenses_dict_basic
			if data['Select Rules Category']=='categories_labels':
			  collection=db.categories_dict_catalogue
			  collection1=db.unharmonised_categories
			  collection2=db.categories_dict_basic
			if data['Select Rules Category']=='categories_values':
			  collection=db.categories_values_dict_catalogue
			  collection1=db.unharmonised_category_values
			  collection2=db.categories_values_dict_basic
			if data['Select Rules Category']=='dates':
			  collection=db.dates_dict_catalogue
			  collection1=db.unharmonised_dates
			  collection2=db.dates_dict_basic
		  
			data['harmonise_category_selection']=data['Select Rules Category']
			
			document=collection.find_one({'cat_url':str(cat_url)})
			if document!=None:
			  del document['_id']
			  del document['cat_url']
			  if 'user' in document:
				del document['user']
			else: document={}
			
			document2=collection2.find_one()
			if document2!=None:
			  del document2['_id']
			  if 'user' in document2:
				del document2['user']
			else: document2={}
			
			merged_document = {key: value for (key, value) in (document.items() + document2.items())}
			
			document1=collection1.find_one({'cat_url':str(cat_url)})
			if document1!=None:
			  del document1['_id']
			  del document1['cat_url']
			  if 'user' in document1:
				del document1['user']
			data['unharmonised']=document1
			data['dictionary']=merged_document

## All my catalogues - case
		  if data['catalogue_selection']=="All my catalogues":
			#document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
			#cat_url=document_job['cat_url']
			
			
			if data['Select Rules Category']=='formats':
			  collection=db.formats_dict_user
			if data['Select Rules Category']=='licenses':
			  collection=db.licenses_dict_user
			if data['Select Rules Category']=='categories_labels':
			  collection=db.categories_dict_user
			if data['Select Rules Category']=='categories_values':
			  collection=db.categories_values_dict_user
			if data['Select Rules Category']=='dates':
			  collection=db.dates_dict_catalogue
		  
			data['harmonise_category_selection']=data['Select Rules Category']
			document=collection.find_one({'user':str(c.user)})
			if document!=None:
			  del document['_id']
			  if 'user' in document:
				del document['user']

			data['unharmonised']=""
			data['dictionary']=document
		  
		  vars = {'data': data, 'errors': errors}
		  
		  return render('viewrules.html', extra_vars=vars)

	  return render('viewrules.html')