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
from ckan.logic import get_action
import json
import pymongo
import bson
import configparser
import logging
import re
import add_fieldsCounter_db

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
user_names=[]



class CustomHarmonisationController(PackageController):


  def __before__(self, action, **params):

	  super(CustomHarmonisationController, self).__before__(action, **params)

	  c.dataset_type = DATASET_TYPE_NAME

  def escape_key(self,key,replace="\uFF0E"):
      return key.replace(".",replace)


  def unescape_key(self,key,replace="\uFF0E"):
      return key.replace(replace,".")


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

		odm_harmonised_collection=db.odm_harmonised
		# odm_harmonised_collection.remove({'catalogue_url':cat_url})

                datasets=db['odm'].update({'catalogue_url':cat_url}
                        ,{ '$set': {'updated_dataset': True}
                            },multi=True,upsert=True
                        )

		# odm_collection=db.odm
                # datasets=list(odm_collection.find({'catalogue_url':cat_url}))
		# print(str(len(datasets)))
		# i=0
		# while i<len(datasets):
		#   if 'copied' in datasets[i].keys():
		# 	  #print(datasets[i])
		# 	  del datasets[i]['copied']
		# 	  odm_collection.save(datasets[i])
		#   i+=1

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
    # print(request.POST)
    data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
    errors=''
    # print(data)

## save and delete mappings
    remove_and_save_action = request.params.get('remove-and-save')
    if remove_and_save_action == 'editrules-complete':
        db = client.odm
        collection_jobs=db.jobs
        # document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
        # cat_url=document_job['cat_url']

        if data['Select Rules Category']=='formats':
            collection=db.new_formats_mappings
            save_coll=db.formats_dict_catalogue
            collection_unharmonised=db.unharmonised_formats
        if data['Select Rules Category']=='licenses':
              collection=db.new_licenses_mappings
              save_coll=db.licenses_dict_catalogue
              collection_unharmonised=db.unharmonised_licenses
        if data['Select Rules Category']=='categories_labels':
              collection=db.new_categories_mappings
              save_coll=db.categories_dict_catalogue
        if data['Select Rules Category']=='categories_values':
              collection=db.new_categories_values_mappings
              save_coll=db.categories_values_dict_catalogue
              collection_unharmonised=db.unharmonised_category_values
        if data['Select Rules Category']=='dates':
              collection=db.new_dates_mappings
              save_coll=db.dates_dict_catalogue

        if 'mappings' in data:
            maps_to_add = {}

            for info in data['mappings']:
                if 'deleted' not in info:
                    if info['cat_url'] not in maps_to_add:
                        maps_to_add[info['cat_url']] = {}
                    maps_to_add[info['cat_url']][self.escape_key(info['key'],'(dot)')] = info['value']


            for cat_url,values in maps_to_add.iteritems():
                save_coll.update({'cat_url': str(cat_url) },
                        {
                            '$set': values
                        },
                        upsert = True
                        )

                document=collection.find_one({'cat_url':str(cat_url)})
                i = 0
                while i<len(document.keys()):
                    if document.keys()[i] not in ['_id','cat_url','user']:
                      del document[document.keys()[i]]
                      i-=1
                    i+=1
                collection.save(document)


## view mappings
    if c.user=='admin':
        db = client.odm
##per catalogue - case
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
            document = {}
            for doc in collection.find():
                user = doc['user']
                if user not in document:
                    document[user] = {}
                cat = doc['cat_url']
                if cat not in document[user]:
                    document[user][cat] = {}
                for k,v in doc.iteritems():
                    if k not in ['user','cat_url','_id']:
                        document[user][cat][k] = v

        except: document={}
        data['new_formats_mappings']=document

        vars = {'data': data, 'errors': errors}

        return render('editrules.html', extra_vars=vars)
    else:
        abort(401, _('Not authorized to see this page'))



  def view_rules(self, data=None):
        errors=''
        user=c.user
        user_names[:]=[]
# print(parse_params(request.POST))
        data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
        # print(data)
        rules_action = request.params.get('rules')
        context = {'model':model,'user':c.user}
        users_list = get_action('user_list')(context,{})
        for user_info in users_list:
            user_names.append(user_info['name'])

          # count=0
	  # while count<len(users_list):
##save rules
        save_action = request.params.get('save')
        if save_action == 'viewrules-complete':
            # data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
              #print(data['Select Rules Category'])
            db = client.odm
            collection_jobs=db.jobs

            document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
            cat_url=document_job['cat_url']

            if data['Select Rules Category']=='formats':
              collection_new=db.new_formats_mappings
              # collection_unharmonised=db.unharmonised_formats
            if data['Select Rules Category']=='licenses':
              collection_new=db.new_licenses_mappings
              # collection_unharmonised=db.unharmonised_licenses
            if data['Select Rules Category']=='categories_labels':
              collection_new=db.new_categories_mappings
            if data['Select Rules Category']=='categories_values':
              collection_new=db.new_categories_values_mappings
              # collection_unharmonised=db.unharmonised_category_values
            if data['Select Rules Category']=='dates':
              collection_new=db.new_dates_mappings

            data_to_update={}
##store the unharmonised mappings to new_mappings db
            if 'unharmonised_value' in data:
                if not isinstance(data['unharmonised_value'],list):
                    data_to_update[self.escape_key(data['unharmonised_key'],"(dot)")]=data['unharmonised_value']
                else:
                    for i in range(0,len(data['unharmonised_value'])):
                        if data['unharmonised_value'][i] not in [None,'']:
                            # print(data['unharmonised_key'][i],data['unharmonised_value'][i])
                            data_to_update[self.escape_key(data['unharmonised_key'][i],"(dot)")]=data['unharmonised_value'][i]


##store the updated harmonised mappings to new_mappings db
            if 'harmonised' in data:
                    for i,val in enumerate(data['harmonised']):
                        if 'checkbox' in val and val['checkbox']=='on' and val['value'] not in [None,'']:
                            data_to_update[self.escape_key(val['key'],"(dot)")]=val['value']

##store the new recommended mappings to new_mappings db
            if 'extras' in data and len(data['extras'])>0:
                for i,val in enumerate(data['extras']):
                    if 'value' in val and data['extras'][i]['value'] not in [None,'']:
                        data_to_update[self.escape_key(data['extras'][i]['key'],"(dot)")]=data['extras'][i]['value']

            if data_to_update:
                # print(data_to_update)
                print(data_to_update)
                if data['catalogue_selection']!="All my catalogues":
                    collection_new.update({'cat_url':cat_url,'user':c.user},
                            {'$set':
                                data_to_update
                                },upsert=True
                            )
                elif data['catalogue_selection']=="All my catalogues":
                    collection_new.update({'user':c.user},
                            {'$set':
                                data_to_update
                                },upsert=True
                            )


##  view rules
        if rules_action == 'view-rules':
## edw na prosthesw pote tha emfanizetai i kathe lista
                    db = client.odm
                    # data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
                    collection_jobs=db.jobs
                    db_odm = db['odm']
                    db_odm_harmonised = db['odm_harmonised']

##Case - per catalogue
                    if 'dictionary' not in data:
                        data['dictionary']={}
                    if 'unharmonised' not in data:
                        data['unharmonised']={}
                    if 'next_harmonised' not in data:
                        data['next_harmonised']={}
                    if 'user_mappings' not in data:
                        data['user_mappings']={}

		    if data['catalogue_selection']!="All my catalogues":
			document_job=collection_jobs.find_one({"title":str(data['catalogue_selection'])})
			cat_url=document_job['cat_url']

			if data['Select Rules Category']=='formats':
                            formats={}
                            odm_result=db_odm.aggregate([
                                {'$match':{'catalogue_url':cat_url,
                                    }},
                                {'$unwind':'$resources'},
                                {'$group':{'_id':'','formats':{'$addToSet':'$resources.format'
                                        }}},
                                {'$project': {
                                    '_id':0,
                                    'formats':{'$setDifference':['$formats',[None] ]}
                                    }}
                                ])
                            # harm_result=db_odm_harmonised.aggregate([
                            #     {'$match':{'catalogue_url':cat_url,
                            #         'license_id':{'$nin':['',None]}}},
                            #     {'$group':{'_id':'','licenses':{'$addToSet':'$license_id'}}},
                            #     ])

                            # if odm_result['ok']==1 and harm_result['ok']==1:
                            if odm_result['ok']==1:
                                if len(odm_result['result'])>0:
                                    raw_formats = odm_result['result'][0]['formats']
                                    # harm_lic = harm_result['result'][0]['licenses']

                                    formats_basic=db.formats_dict_basic.find()
                                    formats_cat=db.formats_dict_catalogue.find({'cat_url':cat_url})

                                    for form in formats_basic:
                                        formats = form
                                    for form in formats_cat:
                                        formats.update(form)

                                    form_keys = []
                                    form_vals = []
                                    unescaped_stored_form_keys = []
                                    for key,val in formats.iteritems():
                                        if key!='_id':
                                            form_keys.append(key.strip().lower())
                                            unescaped_stored_form_keys.append(self.unescape_key(key,"(dot)"))
                                            form_vals.append(val)

                                    for form in raw_formats:
                                        if form in form_vals:
                                            pass
                                        elif form.strip().lower() in form_vals:
                                            try:
                                                _id=form_vals.index(form.strip().lower())
                                                data['next_harmonised'][form] = form_vals[_id]
                                            except ValueError:
                                                pass
                                        elif form in unescaped_stored_form_keys:
                                            data['dictionary'][form]=formats[self.escape_key(form,"(dot)")]
                                        else:
                                            try:
                                                _id=form_keys.index(self.escape_key(form,"(dot)").strip().lower())
                                                data['next_harmonised'][form] = formats[self.escape_key(unescaped_stored_form_keys[_id],"(dot)")]
                                            except ValueError:
                                                data['unharmonised'][form] = None
                                else:
                                    print('catalogue: %s does not exist in odm' % cat_url)


			if data['Select Rules Category']=='licenses':
                            licenses={}
                            odm_result=db_odm.aggregate([
                                {'$match':{'catalogue_url':cat_url,
                                    # 'license_id':{'$nin':['',None]}
                                    }},
                                {'$group':{'_id':'','licenses':{'$addToSet':
                                    {'$ifNull':['$license_id',
                                        {'$ifNull': ['$license','$license_title']}
                                        ]}
                                        }}},
                                {'$project': {
                                    '_id':0,
                                    # 'licenses':1,
                                    'licenses':{'$setDifference':['$licenses',[None,''] ]}
                                    }}
                                ])
                            # harm_result=db_odm_harmonised.aggregate([
                            #     {'$match':{'catalogue_url':cat_url,
                            #         'license_id':{'$nin':['',None]}}},
                            #     {'$group':{'_id':'','licenses':{'$addToSet':'$license_id'}}},
                            #     ])

                            # if odm_result['ok']==1 and harm_result['ok']==1:
                            if odm_result['ok']==1:
                                if len(odm_result['result'])>0:
                                    raw_lic = odm_result['result'][0]['licenses']
                                    # harm_lic = harm_result['result'][0]['licenses']

                                    lic_basic=db.licenses_dict_basic.find()
                                    lic_cat=db.licenses_dict_catalogue.find({'cat_url':cat_url})

                                    for lic in lic_basic:
                                        licenses = lic
                                    for lic in lic_cat:
                                        licenses.update(lic)

                                    lic_keys = []
                                    unescaped_stored_lic_keys = []
                                    for key,val in licenses.iteritems():
                                        if key!='_id':
                                            lic_keys.append(key.strip().upper())
                                            unescaped_stored_lic_keys.append(self.unescape_key(key,"(dot)"))

                                    for lic in raw_lic:
                                        if lic in unescaped_stored_lic_keys:
                                            data['dictionary'][lic]=licenses[self.escape_key(lic,"(dot)")]
                                        else:
                                            try:
                                                _id=lic_keys.index(self.escape_key(lic,"(dot)").strip().upper())
                                                data['next_harmonised'][lic] = licenses[self.escape_key(unescaped_stored_lic_keys[_id],"(dot)")]
                                            except ValueError:
                                                data['unharmonised'][lic] = None
                                else:
                                    print('catalogue: %s does not exist in odm' % cat_url)


                          #
			    # collection=db.licenses_dict_catalogue
                            # collection1=db.unharmonised_licenses
			  # collection2=db.licenses_dict_basic
			if data['Select Rules Category']=='categories_labels':
                            retrieve_keys = add_fieldsCounter_db.FieldsCounter()

                            labels=db.categories_dict_basic.find_one()
                            result_cat=db.categories_dict_catalogue.find({'cat_url':cat_url})

                            odm_fields = []
                            result_odm = db['odm'].find({'catalogue_url':cat_url},limit=10,sort=[('attrs_counter',-1)])
                            for doc in result_odm:
                                pass
                                items = retrieve_keys.flatten_dict(doc)
                                for key in items:
                                    if key not in add_fieldsCounter_db.FieldsCounter().excluded_fields \
                                        and key not in odm_fields:
                                        odm_fields.append(key)

                            data['raw_attrs'] = odm_fields

                            labels_cat = {}
                            for lb in result_cat:
                                labels_cat.update(lb)

                            for key,val in labels.iteritems():
                                if key!='_id':
                                    if key in labels_cat:
                                        data['user_mappings'][key] = self.escape_key(val,"(dot)")
                                    else:
                                        data['dictionary'][key] = self.escape_key(val,"(dot)")


			if data['Select Rules Category']=='categories_values':
                            # collection=db.categories_values_dict_catalogue
			  # collection1=db.unharmonised_category_values
			    # collection2=db.categories_values_dict_basic

                            harm_result=db_odm_harmonised.aggregate([
                                {'$match':{'catalogue_url':cat_url,
                                    }},
                                {'$group':{'_id':'','categories':{'$addToSet':
                                    {'raw':'$extras.category','harm_cat':'$category','harm_subcat':'$sub_category'}
                                        }}},
                                {'$project': {
                                    '_id':0,
                                    'categories':1,
                                    # 'licenses':{'$setDifference':['$licenses',[None] ]}
                                    }}
                                ])

                            if harm_result['ok']==1:
                                if len(harm_result['result'])>0:
                                    categories_basic=db.categories_values_dict_basic.find_one()
                                    categories_cat=db.categories_values_dict_catalogue.find({'cat_url':cat_url})

                                    categories = {}
                                    # for cat in categories_basic:
                                    #     categories = cat
                                    for cat in categories_cat:
                                        categories.update(cat)

                                    for category in harm_result['result'][0]['categories']:
                                        try:
                                            if isinstance(category['raw'],list):
                                                for raw_cat in category['raw']:
                                                    normalized=re.split('&quot;|[\"\'\[{\(\)}\];]',raw_cat)
                                                    for each_norm in normalized:
                                                        each_norm = re.sub(' & ',' and ',each_norm)
                                                        each_norm = each_norm.strip(' ,')
                                                        if each_norm in [None,'']:
                                                            continue

                                                        if 'harm_cat' in category:
                                                            for mapped_cat in category['harm_cat']:
                                                                if mapped_cat in categories:
                                                                    data['user_mappings'][each_norm]=mapped_cat
                                                                else:
                                                                    data['dictionary'][each_norm]=mapped_cat
                                                        else:
                                                            data['unharmonised'][each_norm] = None
                                            else:
                                                normalized=re.split('&quot;|[\"\'\[{\(\)}\];]',category['raw'])
                                                for each_norm in normalized:
                                                    each_norm = re.sub(' & ',' and ',each_norm)
                                                    each_norm = each_norm.strip(' ,')
                                                    if each_norm in [None,'']:
                                                        continue

                                                    if 'harm_cat' in category:
                                                        for mapped_cat in category['harm_cat']:
                                                            if mapped_cat in categories:
                                                                data['user_mappings'][each_norm]=mapped_cat
                                                            else:
                                                                data['dictionary'][each_norm]=mapped_cat
                                                    else:
                                                        data['unharmonised'][each_norm] = None
                                        except KeyError,e:
                                            print(category,e)
                                else:
                                    print('catalogue: %s does not exist in odm' % cat_url)





			if data['Select Rules Category']=='dates':
                            retrieve_keys = add_fieldsCounter_db.FieldsCounter()

                            # collection1=db.unharmonised_dates
                            labels=db.dates_dict_basic.find_one()
                            result_cat=db.dates_dict_catalogue.find({'cat_url':cat_url})

                            odm_fields = []
                            result_odm = db['odm'].find({'catalogue_url':cat_url},limit=10,sort=[('attrs_counter',-1)])
                            for doc in result_odm:
                                pass
                                items = retrieve_keys.flatten_dict(doc)
                                for key in items:
                                    if key not in add_fieldsCounter_db.FieldsCounter().excluded_fields \
                                        and key not in odm_fields:
                                        odm_fields.append(key)

                            data['raw_attrs'] = odm_fields

                            labels_cat = {}
                            for lb in result_cat:
                                labels_cat.update(lb)

                            for key,val in labels.iteritems():
                                if key!='_id':
                                    if key in labels_cat:
                                        data['user_mappings'][key] = self.escape_key(val,"(dot)")
                                    elif key in odm_fields:
                                        data['dictionary'][key] = self.escape_key(val,"(dot)")
                                        odm_fields.remove(key)




                        data['harmonise_category_selection']=data['Select Rules Category']

## All my catalogues - case
		    elif data['catalogue_selection']=="All my catalogues":
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


		    data['usernames']=user_names

		    vars = {'data': data, 'errors': errors}

		    return render('viewrules.html', extra_vars=vars)

	data['usernames']=user_names
	errors=''
	vars = {'data': data, 'errors': errors}

	return render('viewrules.html', extra_vars=vars)


  def monitor(self, data=None):
	data = data or clean_dict(dict_fns.unflatten(tuplize_dict(parse_params(request.POST))))
	print(data)
	errors=''
	user=''
	query={}

	##gather
	if 'gathered' in data.keys():
	  if data['gathered']=="Gather stage failed":
		query.update({'gathered':'Gather stage failed'})
	  if data['gathered']=="Gather stage finished":
		query.update({'gathered':{'$nin' : ['Gather stage failed',None]}})

	##fetch
	if 'fetched' in data.keys():
	  if data['fetched']=="Fetched":
		query.update({'new':{'$nin' : [None]}})
		#query.update({'updated':{'$nin' : [None]}})
	  if data['fetched']=="Not Yet":
		query.update({'new':{'$in' : [0,1,None]}})
		query.update({'updated':{'$in' : [0,1,None]}})

	##harmonisation
	if 'harmonised' in data.keys():
	  if data['harmonised']=="Harmonisation finished":
		query.update({'harmonised':'finished'})
	  if data['harmonised']=="Harmonisation started":
		query.update({'harmonised':'started'})
	  if data['harmonised']=="Harmonisation pending":
		query.update({'harmonised':{'$nin' : ['started','finished']}})

	db = client.odm
	db1 = db.jobs
	data=db1.find(query)
	if c.user=='admin':
		user=True
	else:
		user=False
	#data['harmonised']=document['harmonised']
	vars = {'data': data, 'errors': errors,'user':user}
	return render('monitor.html', extra_vars=vars)
