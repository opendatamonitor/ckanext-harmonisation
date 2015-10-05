import json
import pymongo
import bson
import configparser
import logging
import time
import HarmonisationEngine
from dictionaries import basic_category_labels_dict
from dictionaries import basic_formats_dict
from dictionaries import basic_licenses_dict
from dictionaries import basic_category_values
from dictionaries import basic_dates_dict
from datetime import datetime
from ckan.lib.base import c

# read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient = config['ckan:odm_extensions']['mongoclient']
mongoport = config['ckan:odm_extensions']['mongoport']


client = pymongo.MongoClient(str(mongoclient), int(mongoport))

log = logging.getLogger(__name__)

# create a file handler
handler = logging.FileHandler(
    '/var/local/ckan/default/pyenv/src/ckanext-harmonisation/ckanext/harmonisation/controllers/harmonisation.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(handler)
assert not log.disabled


def slave():

    # databases init
    db = client.odm
    jobs = db.jobs
    harmonise_jobs = db.harmonise_jobs
    categories_dict_basic = db.categories_dict_basic
    categories_dict_catalogue = db.categories_dict_catalogue
    categories_values_dict_basic = db.categories_values_dict_basic
    categories_values_dict_catalogue = db.categories_values_dict_catalogue
    dates_dict_basic = db.dates_dict_basic
    dates_dict_catalogue = db.dates_dict_catalogue
    licenses_dict_catalogue = db.licenses_dict_catalogue
    licenses_dict_user = db.licenses_dict_user
    licenses_dict_basic = db.licenses_dict_basic
    formats_dict_catalogue = db.formats_dict_catalogue
    formats_dict_basic = db.formats_dict_basic
    unharmonised_formats = db.unharmonised_formats
    unharmonised_licenses = db.unharmonised_licenses
    unharmonised_category_values_db = db.unharmonised_category_values
    print("Harmonisation slave. Waiting for harmonisation jobs...")

# slave while
    while 1 == 1:

        if 1 == 1:

            # find harmonisation jobs
            harmonise_job = harmonise_jobs.find_one()
            catalogue_url = harmonise_job['cat_url']


# create rules dictionaries / catalogue.

            categories_dict = basic_category_labels_dict.category_labels_dict
            search_categories_dict = categories_dict_basic.find_one()
            if search_categories_dict is None:
                categories_dict_basic.save(categories_dict)
            else:
                j = 0
                while j < len(categories_dict.keys()):
                    search_categories_dict.update(
                        {categories_dict.keys()[j]: categories_dict[categories_dict.keys()[j]]})
                    categories_dict_basic.save(search_categories_dict)
                    j += 1

            search_cat_categories = categories_dict_catalogue.find_one(
                {'cat_url': str(catalogue_url)})
            if search_cat_categories is None:
                cat_categories = {}
                cat_categories.update({'cat_url': str(catalogue_url)})
                categories_dict_catalogue.save(cat_categories)

            # search_categories_dict=categories_dict.find_one({'cat_url':str(catalogue_url)})
            # if search_categories_dict==None:
                # catalogue_categories_dict=basic_category_labels_dict.category_labels_dict
                # catalogue_categories_dict.update({'cat_url':str(catalogue_url)})
                # categories_dict.save(catalogue_categories_dict)

            categories_values_dict = basic_category_values.category_values
            search_categories_values_dict = categories_values_dict_basic.find_one()
            if search_categories_values_dict is None:
                categories_values_dict_basic.save(categories_values_dict)
            else:
                j = 0
                while j < len(categories_values_dict.keys()):
                    search_categories_values_dict.update({categories_values_dict.keys(
                    )[j]: categories_values_dict[categories_values_dict.keys()[j]]})
                    categories_values_dict_basic.save(
                        search_categories_values_dict)
                    j += 1

            search_cat_categories_values = categories_values_dict_catalogue.find_one(
                {'cat_url': str(catalogue_url)})
            if search_cat_categories_values is None:
                cat_categories_values = {}
                cat_categories_values.update({'cat_url': str(catalogue_url)})
                categories_values_dict_catalogue.save(cat_categories_values)

            # search_categories_values_dict=categories_values_dict.find_one({'cat_url':str(catalogue_url)})
            # if search_categories_values_dict==None:
                # catalogue_categories_values_dict=basic_category_values.category_values
                # catalogue_categories_values_dict.update({'cat_url':str(catalogue_url)})
                # categories_values_dict.save(catalogue_categories_values_dict)

            dates_dict = basic_dates_dict.dates_dict
            search_dates_dict = dates_dict_basic.find_one()
            if search_dates_dict is None:
                dates_dict_basic.save(dates_dict)
            else:
                j = 0
                while j < len(dates_dict.keys()):
                    search_dates_dict.update(
                        {dates_dict.keys()[j]: dates_dict[dates_dict.keys()[j]]})
                    dates_dict_basic.save(search_dates_dict)
                    j += 1

            search_cat_dates = dates_dict_catalogue.find_one(
                {'cat_url': str(catalogue_url)})
            if search_cat_dates is None:
                cat_dates = {}
                cat_dates.update({'cat_url': str(catalogue_url)})
                dates_dict_catalogue.save(cat_dates)

            # search_dates_dict=dates_dict.find_one({'cat_url':str(catalogue_url)})
            # if search_dates_dict==None:
                # catalogue_dates_dict=basic_dates_dict.dates_dict
                # catalogue_dates_dict.update({'cat_url':str(catalogue_url)})
                # dates_dict.save(catalogue_dates_dict)

            formats_dict = basic_formats_dict.formats_dict
            search_formats_dict = formats_dict_basic.find_one()
            if search_formats_dict is None:
                formats_dict_basic.save(formats_dict)
            else:
                j = 0
                while j < len(formats_dict.keys()):
                    search_formats_dict.update(
                        {formats_dict.keys()[j]: formats_dict[formats_dict.keys()[j]]})
                    formats_dict_basic.save(search_formats_dict)
                    j += 1

            search_cat_formats = formats_dict_catalogue.find_one(
                {'cat_url': str(catalogue_url)})
            if search_cat_formats is None:
                cat_formats = {}
                cat_formats.update({'cat_url': str(catalogue_url)})
                formats_dict_catalogue.save(cat_formats)

            # search_formats_dict=formats_dict.find_one({'cat_url':str(catalogue_url)})
            # if search_formats_dict==None:
                # catalogue_formats_dict=basic_formats_dict.formats_dict
                # catalogue_formats_dict.update({'cat_url':str(catalogue_url)})
                # formats_dict.save(catalogue_formats_dict)

            licenses_dict = basic_licenses_dict.licenses_dict
            search_licenses_dict = licenses_dict_basic.find_one()
            if search_licenses_dict is None:
                licenses_dict_basic.save(licenses_dict)
            else:
                j = 0
                while j < len(licenses_dict.keys()):
                    search_licenses_dict.update(
                        {licenses_dict.keys()[j]: licenses_dict[licenses_dict.keys()[j]]})
                    licenses_dict_basic.save(search_licenses_dict)
                    j += 1

            search_cat_licenses = licenses_dict_catalogue.find_one(
                {'cat_url': str(catalogue_url)})
            if search_cat_licenses is None:
                cat_licenses = {}
                cat_licenses.update({'cat_url': str(catalogue_url)})
                licenses_dict_catalogue.save(cat_licenses)

            # search_licenses_dict=licenses_dict.find_one({'cat_url':str(catalogue_url)})
            # if search_licenses_dict==None:
                # catalogue_licenses_dict=basic_licenses_dict.licenses_dict
                # catalogue_licenses_dict.update({'cat_url':str(catalogue_url)})
                # licenses_dict.save(catalogue_licenses_dict)

            try:
                job = jobs.find_one({'id': harmonise_job['id']})

                if 'harmonisation' not in job.keys():
                    job['harmonisation'] = {}
                    jobs.save(job)

                job['harmonisation'].update(
                    {"harmonisation_status": "harmonisation started"})
                jobs.save(job)
            except:
                pass

# harmonise basics
            if 'harmonised' in harmonise_job.keys():
                # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job.update({"harmonised": "started"})
                jobs.save(job)

                print('Harmonising Basics')
                cat_url = harmonise_job['cat_url']
                print(cat_url)
                HarmonisationEngine.delete_harmonised(cat_url)
                message_Copy_Odm_to_Odm_harmonised = HarmonisationEngine.Copy_Odm_to_Odm_harmonised(
                    cat_url)
                message_HarmoniseTags = HarmonisationEngine.HarmoniseTags(
                    cat_url)
                message_HarmoniseExtras = HarmonisationEngine.HarmoniseExtras(
                    cat_url)
                message_HarmoniseStringsToIntegers = HarmonisationEngine.HarmoniseStringsToIntegers(
                    cat_url)

                # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update(
                    {"message_Copy_Odm_to_Odm_harmonised": str(message_Copy_Odm_to_Odm_harmonised)})
                job['harmonisation'].update(
                    {"message_HarmoniseTags": str(message_HarmoniseTags)})
                job['harmonisation'].update(
                    {"message_HarmoniseExtras": str(message_HarmoniseExtras)})
                job['harmonisation'].update(
                    {"message_HarmoniseStringsToIntegers": str(message_HarmoniseStringsToIntegers)})
                job['harmonisation'].update({"harmonised_basic": "finished"})
                job['harmonisation'].update(
                    {"harmonised_basic_date": datetime.now()})
                jobs.save(job)


# call harmonise functions by category

# harmonise dates
            if 'dates' in harmonise_job.keys():

                print('Harmonising Dates')
                cat_url = harmonise_job['cat_url']
                message_HarmoniseDatesLabels = HarmonisationEngine.HarmoniseDatesLabels(
                    cat_url)
                message_HarmoniseReleaseDates = HarmonisationEngine.HarmoniseReleaseDates(
                    cat_url)
                message_HarmoniseUpdateDates = HarmonisationEngine.HarmoniseUpdateDates(
                    cat_url)
                message_HarmoniseMetadataCreated = HarmonisationEngine.HarmoniseMetadataCreated(
                    cat_url)
                message_HarmoniseMetadataModified = HarmonisationEngine.HarmoniseMetadataModified(
                    cat_url)

            # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update({"harmonised_dates": "finished"})
                job['harmonisation'].update(
                    {"message_HarmoniseDatesLabels": str(message_HarmoniseDatesLabels)})
                job['harmonisation'].update(
                    {"message_HarmoniseReleaseDates": str(message_HarmoniseReleaseDates)})
                job['harmonisation'].update(
                    {"message_HarmoniseUpdateDates": str(message_HarmoniseUpdateDates)})
                job['harmonisation'].update(
                    {"message_HarmoniseMetadataCreated": str(message_HarmoniseMetadataCreated)})
                job['harmonisation'].update(
                    {"message_HarmoniseMetadataModified": str(message_HarmoniseMetadataModified)})
                job['harmonisation'].update(
                    {"harmonised_dates_date": datetime.now()})
                jobs.save(job)


# harmonise resources
            if 'resources' in harmonise_job.keys():

                print('Harmonising resources')
                cat_url = harmonise_job['cat_url']

                unharm_formats, message_HarmoniseFormats = HarmonisationEngine.HarmoniseFormats(
                    cat_url)
                print(unharm_formats)

        # unharmonised formats handling
                if len(unharm_formats) > 0:

                    search_unharmonised_formats = unharmonised_formats.find_one(
                        {'cat_url': str(catalogue_url)})
                    if search_unharmonised_formats is None:
                        search_unharmonised_formats = {}

                    i = 0
                    while i < len(unharm_formats):
                        search_unharmonised_formats.update(
                            {str(unharm_formats[i]).replace('.', '(dot)'): ""})
                        i += 1

                    search_unharmonised_formats.update(
                        {'cat_url': str(catalogue_url)})
                    unharmonised_formats.save(search_unharmonised_formats)

                message_HarmoniseBadFormats = HarmonisationEngine.HarmoniseBadFormats(
                    cat_url)
                message_HarmoniseSizes = HarmonisationEngine.HarmoniseSizes(
                    cat_url)
                message_HarmoniseMimetypes = HarmonisationEngine.HarmoniseMimetypes(
                    cat_url)
                message_HarmoniseNumTagsAndResources = HarmonisationEngine.HarmoniseNumTagsAndResources(
                    cat_url)

        # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update(
                    {"harmonised_resources": "finished"})
                job['harmonisation'].update(
                    {"message_HarmoniseBadFormats": str(message_HarmoniseBadFormats)})
                job['harmonisation'].update(
                    {"message_HarmoniseMimetypes": str(message_HarmoniseMimetypes)})
                job['harmonisation'].update(
                    {"message_HarmoniseSizes": str(message_HarmoniseSizes)})
                job['harmonisation'].update(
                    {"message_HarmoniseFormats": str(message_HarmoniseFormats)})
                job['harmonisation'].update({"message_HarmoniseNumTagsAndResources": str(
                    message_HarmoniseNumTagsAndResources)})
                job['harmonisation'].update(
                    {"harmonised_resources_date": datetime.now()})
                jobs.save(job)


# harmonise licenses
            if 'licenses' in harmonise_job.keys():

                print('licenses')
                cat_url = harmonise_job['cat_url']

                (unharm_licenses, message_HarmoniseLicenses) = HarmonisationEngine.HarmoniseLicenses(
                    cat_url)
                print(unharm_licenses)
        # unharmonised licenses handling
                if len(unharm_licenses) > 0:

                    search_unharmonised_licenses = unharmonised_licenses.find_one(
                        {'cat_url': str(catalogue_url)})
                    if search_unharmonised_licenses is None:
                        search_unharmonised_licenses = {}

                    i = 0
                    while i < len(unharm_licenses):
                        search_unharmonised_licenses.update(
                            {str(unharm_licenses[i]).replace('.', '(dot)'): ""})
                        i += 1

                    search_unharmonised_licenses.update(
                        {'cat_url': str(catalogue_url)})
                    unharmonised_licenses.save(search_unharmonised_licenses)

        # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update(
                    {"harmonised_licenses": "finished"})
                job['harmonisation'].update(
                    {"message_HarmoniseLicenses": str(message_HarmoniseLicenses)})
                job['harmonisation'].update(
                    {"harmonised_licenses_date": datetime.now()})
                jobs.save(job)


# harmonise languages
            if 'languages' in harmonise_job.keys():
                print('languages')
                cat_url = harmonise_job['cat_url']
                message_HarmoniseLanguageLabels = HarmonisationEngine.HarmoniseLanguageLabels(
                    cat_url)
                message_HarmoniseLanguages = HarmonisationEngine.HarmoniseLanguages(
                    cat_url)

        # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update(
                    {"harmonised_languages": "finished"})
                job['harmonisation'].update(
                    {"message_HarmoniseLanguageLabels": str(message_HarmoniseLanguageLabels)})
                job['harmonisation'].update(
                    {"message_HarmoniseLanguages": str(message_HarmoniseLanguages)})
                job['harmonisation'].update(
                    {"harmonised_languages_date": datetime.now()})
                jobs.save(job)


# harmonise categories
            if 'categories' in harmonise_job.keys():
                print('categories')
                cat_url = harmonise_job['cat_url']
                message_HarmoniseCategoryLabels = HarmonisationEngine.HarmoniseCategoryLabels(
                    cat_url)
                message_HarmoniseCategories = HarmonisationEngine.HarmoniseCategories(
                    cat_url)

        # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update(
                    {"harmonised_categories": "finished"})
                job['harmonisation'].update(
                    {"message_HarmoniseCategoryLabels": str(message_HarmoniseCategoryLabels)})
                job['harmonisation'].update(
                    {"message_HarmoniseCategories": str(message_HarmoniseCategories)})
                job['harmonisation'].update(
                    {"harmonised_categories_date": datetime.now()})
                jobs.save(job)


# harmonise countries
            if 'countries' in harmonise_job.keys():
                print('countries')
                cat_url = harmonise_job['cat_url']
                unharmonised_category_values = HarmonisationEngine.HarmoniseCountries(
                    cat_url)
                print(unharmonised_category_values)
                if len(unharmonised_category_values) > 0:

                    search_unharmonised_category_values = unharmonised_category_values_db.find_one(
                        {'cat_url': str(catalogue_url)})
                    if search_unharmonised_category_values is None:
                        search_unharmonised_category_values = {}

                    i = 0
                    while i < len(unharmonised_category_values):
                        search_unharmonised_category_values.update(
                            {str(unharmonised_category_values[i].encode('utf-8')).replace('.', '(dot)'): ""})
                        i += 1

                    search_unharmonised_category_values.update(
                        {'cat_url': str(catalogue_url)})
                    unharmonised_category_values_db.save(
                        search_unharmonised_category_values)

        # update to jobs db
                job = jobs.find_one({'id': harmonise_job['id']})
                job['harmonisation'].update(
                    {"harmonised_countries": "finished"})
                job['harmonisation'].update(
                    {"harmonised_countries_date": datetime.now()})
                jobs.save(job)

            job = jobs.find_one({'id': harmonise_job['id']})
            job.update({"harmonised": "finished"})
            job.update({"available": True})
            job['harmonisation'].update({"harmonisation_status": "harmonised"})
            jobs.save(job)
        # harmonisation job finished - delete it
            harmonise_jobs.remove({'id': harmonise_job['id']})
            print('harmonisation finished. Waiting for next job...')
            # wait for the next one
            time.sleep(3)
        # except :
           # time.sleep(3)


slave()
