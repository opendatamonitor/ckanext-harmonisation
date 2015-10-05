import pymongo
from pymongo import errors
import sys
import json
import bson
import urllib2
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

from ckanext.harmonisation.lib.find_format import find_format

import lepl.apps.rfc3696
import custom_logging
import logging
import configparser
import dictionaries
MaxDocsInDB = 400000

# read from development.ini file all the required parameters
config = configparser.ConfigParser()
config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
mongoclient = config['ckan:odm_extensions']['mongoclient']
mongoport = config['ckan:odm_extensions']['mongoport']
log_path = config['ckan:odm_extensions']['log_path']
harmonisation_engine_log = str(
    log_path) + 'harmonisation_engine/harmonisation_engine_log.txt'

client1 = pymongo.MongoClient(str(mongoclient), int(mongoport))
db = client1.odm

text_file = open(str(harmonisation_engine_log), "w")
# the collection we ll store all datasets
collection = db.odm_harmonised
collection1 = db.jobs
collection_dict = db.dictionary
countries_table = db.countries
cities_table = db.cities
formats_dict_catalogue = db.formats_dict_catalogue
formats_dict_basic = db.formats_dict_basic
formats_dict_user = db.formats_dict_user
licenses_dict_catalogue = db.licenses_dict_catalogue
licenses_dict_basic = db.licenses_dict_basic
licenses_dict_user = db.licenses_dict_user
categories_dict_catalogue = db.categories_dict_catalogue
categories_dict_basic = db.categories_dict_basic
categories_dict_user = db.categories_dict_user
categories_values_dict_catalogue = db.categories_values_dict_catalogue
categories_values_dict_basic = db.categories_values_dict_basic
categories_values_dict_user = db.categories_values_dict_user
dates_dict_catalogue = db.dates_dict_catalogue
dates_dict_basic = db.dates_dict_basic
dates_dict_user = db.dates_dict_user

# search for new datasets


def Copy_Odm_to_Odm_harmonised(cat_url):
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - Harmonisation_engine process started.' +
                    '\n')
    counter = 0
    # the collection we wanna copy datasets
    odm_collection = db.odm
    # the collection we ll store all new datasets
    odm_harmonised_collection = db.odm_harmonised

    datasets = list(odm_collection.find({'catalogue_url': cat_url}))
    print(str(len(datasets)))
    i = 0
    while i < len(datasets):

        if 'copied' not in datasets[i].keys():
            datasets[i].update({"copied": True})
            odm_harmonised_collection.save(datasets[i])
            odm_collection.save(datasets[i])
            counter += 1

        if 'copied' in datasets[i].keys() and 'updated_dataset' in datasets[
                i].keys():
            temp_id = datasets[i]['id']
            document = odm_harmonised_collection.find_one({"id": temp_id})
            if len(document.keys()) > 1:
                odm_harmonised_collection.remove({"id": temp_id})
            odm_harmonised_collection.save(datasets[i])
            del datasets[i]['updated_dataset']
            odm_collection.save(datasets[i])
            counter += 1
        i += 1
    print(str(counter) + " new datasets found and sent to harmonisation engine.")
    message = 'There are: ' + str(counter) + \
        ' new unharmonised datasets found.' + '\n'
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    ' There are: ' +
                    str(counter) +
                    ' new unharmonised datasets found.' +
                    '\n')
    return message


def delete_harmonised(cat_url):
    i = 0

    datasets = list(collection.find({'catalogue_url': cat_url}))
    while i < len(datasets):
        try:
            del datasets[i]['harmonised']
            collection.save(datasets[i])
        except:
            pass
        i += 1

# Harmonise Tags


def HarmoniseTags(cat_url):

    i = 0
    j = 0
    counter = 0
    tagsarray = []
    point = 0
    datasets = list(collection.find({'catalogue_url': cat_url}))
    while i < len(datasets):

        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_tags' not in datasets[i]['harmonised'].keys():
            try:

                tags = datasets[i]['tags']

                if 'name' in str(tags):

                    while j < len(datasets[i]['tags']):

                        tag = datasets[i]['tags'][j]['name']
                        tagsarray.append(tag)
                        j += 1

                if len(tagsarray) > 0:
                    datasets[i].update({"tags": tagsarray})
                    collection.save(datasets[i])
                    counter += 1

                tagsarray[:] = []
                j = 0
                i += 1
                datasets[i]['harmonised'].update({'harmonised_tags': True})
                datasets[i]['harmonised'].update(
                    {'harmonised_tags_date': datetime.now()})
                collection.save(datasets[i])

            except:
                i += 1

        else:
            i += 1

    print("Tags harmonised in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    "Tags harmonised in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Tags harmonised in: " + str(counter) + " datasets" + '\n'
    return message


# Harmonise Extras
def HarmoniseExtras(cat_url):

    i = 0
    j = 0
    counter = 0
    extrasjson = []
    point = 0
    endpoint = 0,
    errorscounter = 0
    datasets = list(collection.find({'catalogue_url': cat_url}))

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_extras' not in datasets[i]['harmonised'].keys():
            try:

                extras = datasets[i]['extras']

                if 'value' in str(extras) and 'key' in str(extras):

                    extrasjson[:] = []
                    extrasjson2 = ""
                    while j < len(datasets[i]['extras']):
                        extra_key = datasets[i]['extras'][j]['key']
                        extra_value = datasets[i]['extras'][j]['value']
                        if extra_value is not None:
                            if len(extra_value) > 0:

                                c = 0
                                extra_value1 = ""

                                while c < len(extra_value):
                                    extra_value1 = extra_value1 + \
                                        extra_value[c]
                                    c += 1

                                c = 0
                                extra_value = extra_value1
                            extra = '"' + \
                                str(extra_key.encode('utf-8')) + '":' + '"' + str(extra_value.encode('utf-8')) + '"'
                            extrasjson.append(extra)

                        j += 1

                    k = 0
                    extrasjson1 = ""

                    while k < len(extrasjson):
                        extrasjson1 = extrasjson1 + extrasjson[k] + ","
                        k += 1

                    k = 0
                    j = 0

                    extrasjson1 = "{" + extrasjson1.rstrip(',') + "}"

                    try:
                        extrasjson2 = json.loads(extrasjson1)
                    except:
                        errorscounter += 1

                    if len(extrasjson) > 0:
                        datasets[i].update({"extras": extrasjson2})
                        collection.save(datasets[i])
                        counter += 1

                datasets[i]['harmonised'].update({'harmonised_extras': True})
                datasets[i]['harmonised'].update(
                    {'harmonised_extras_date': datetime.now()})
                collection.save(datasets[i])
                i += 1

            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("Extras harmonised in " + str(counter) + " datasets.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    "Extras harmonised in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Extras harmonised in: " + str(counter) + " datasets" + '\n'
    return message


# Harmonise Strings to Integers
def HarmoniseStringsToIntegers(cat_url):

    point = 0
    counter = 0
    endpoint = 0
    counter_broken_links = 0
    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_StringsToIntegers' not in datasets[
                i]['harmonised'].keys():
            try:
                resources = datasets[i]['resources']

                j = 0
                while j < len(resources):

                    if 'size' in str(datasets[i]['resources'][j]) and datasets[i]['resources'][j]['size'] != None and datasets[
                            i]['resources'][j]['size'] != "" and datasets[i]['resources'][j]['size'] != "None":
                        size = datasets[i]['resources'][j]['size']
                        try:

                            datasets[i]['resources'][
                                j].update({"size": int(size)})

                            collection.save(datasets[i])

                            counter += 1
                        except:
                            pass

                    j += 1

                j = 0
                datasets[i]['harmonised'].update(
                    {'harmonised_StringsToIntegers': True})
                datasets[i]['harmonised'].update(
                    {'harmonised_StringsToIntegers_date': datetime.now()})
                collection.save(datasets[i])
                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised string to int in " + str(counter) + " fields.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised string values to int values in: " +
                    str(counter) +
                    " fields" +
                    '\n')
    message = "Harmonised string values to int values in: " + \
        str(counter) + " fields" + '\n'
    return message


# Harmonise Date Labels
def HarmoniseDatesLabels(cat_url):
    doc = collection1.find_one({"cat_url": cat_url})
    user = doc['user']
    dates_dictionary1 = dates_dict_basic.find_one()
    dates_dictionary2 = dates_dict_user.find_one({'user': str(user)})
    if dates_dictionary2 is None:
        dates_dictionary2 = {}
    dates_dictionary3 = dates_dict_catalogue.find_one(
        {'cat_url': str(cat_url)})
    dates_dictionary = {}
    i = 0
    while i < len(dates_dictionary1):
        dates_dictionary.update(
            {dates_dictionary1.keys()[i]: dates_dictionary1[dates_dictionary1.keys()[i]]})
        i += 1
    i = 0
    while i < len(dates_dictionary2):
        dates_dictionary.update(
            {dates_dictionary2.keys()[i]: dates_dictionary2[dates_dictionary2.keys()[i]]})
        i += 1
    i = 0
    while i < len(dates_dictionary3):
        dates_dictionary.update(
            {dates_dictionary3.keys()[i]: dates_dictionary3[dates_dictionary3.keys()[i]]})
        i += 1
    count = 0
    while count < len(dates_dictionary):
        key = dates_dictionary.keys()[count]
        if '(dot)' in key:
            # print(key)
            key1 = key.replace('(dot)', '.')
            # print(key1)
            dates_dictionary.update(
                {key1: dates_dictionary[str(dates_dictionary.keys()[count])]})
            del dates_dictionary[key]
        count += 1
    datemappings_keys = dates_dictionary.keys()
    datemappings = dates_dictionary

    counter_broken_links = 0
    counter = 0
    # datemappings=dates_dict.dates_dict
    # datemappings_keys=datemappings.keys()

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_DatesLabels' not in datasets[i]['harmonised'].keys():
            try:
                extras = datasets[i]['extras']
                if extras != "":
                    extras_keys = extras.keys()
                    j = 0
                    k = 0
                    while j < len(extras_keys):

                        while k < len(datemappings_keys):

                            if extras_keys[j] == datemappings_keys[k]:
                                extras[str(datemappings[str(datemappings_keys[k])])] = extras[
                                    str(extras_keys[j])]
                                del extras[str(extras_keys[j])]
                                collection.save(datasets[i])
                                counter += 1
                                datasets[i]['harmonised'].update(
                                    {'harmonised_DatesLabels': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_DatesLabels_date': datetime.now()})
                                collection.save(datasets[i])
                            k += 1

                        k = 0
                        j += 1

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised date labels in " + str(counter) + " datasets.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised date labels in: " +
                    str(counter) +
                    " datasets." +
                    '\n')
    message = "Harmonised date labels in: " + \
        str(counter) + " datasets." + '\n'
    return message


# Harmonise Release Dates
def HarmoniseReleaseDates(cat_url):

    counter = 0
    counter1 = 0
    bad_date = ""
    counter_broken_links = 0
    c = 0

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        c += 1
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_ReleaseDates' not in datasets[i]['harmonised'].keys():
            try:
                extras = datasets[i]['extras']
                if extras != "":
                    release_date = datasets[i]['extras']['date_released']
                    try:

                        release_date1 = parse(release_date)

                        # if date parsing goes wrong
                        if release_date1 == "" or release_date1 is None:

                            bad_date = re.findall(
                                r'\d+', str(release_date.encode('utf-8')))
                            p = 0

                            while p < len(bad_date):

                                bad_date1 = bad_date[p]

                                if len(bad_date1) != 4:
                                    p += 1

                                else:
                                    bad_date1 = "1/1/" + str(bad_date1)
                                    bad_release_date = parse(bad_date1)

                                    datasets[i]['extras'].update(
                                        {"date_released": bad_release_date})
                                    datasets[i]['harmonised'].update(
                                        {'harmonised_ReleaseDates': True})
                                    datasets[i]['harmonised'].update(
                                        {'harmonised_ReleaseDates_date': datetime.now()})
                                    collection.save(datasets[i])
                                    counter += 1
                                    p = len(bad_date)

                            counter1 += 1
                        else:

                            counter += 1
                            datasets[i]['extras'].update(
                                {"date_released": release_date1})
                            datasets[i]['harmonised'].update(
                                {'harmonised_ReleaseDates': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_ReleaseDates_date': datetime.now()})
                            collection.save(datasets[i])

                    except AttributeError:
                        i = i

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("Harmonised date_released  in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised date_released in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Harmonised date_released in: " + \
        str(counter) + " datasets" + '\n'
    return message


# Harmonise Update Dates
def HarmoniseUpdateDates(cat_url):

    counter = 0
    counter1 = 0
    bad_date = ""
    counter_broken_links = 0

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_UpdateDates' not in datasets[i]['harmonised'].keys():
            try:
                extras = datasets[i]['extras']
                if extras != "":
                    update_date = datasets[i]['extras']['date_updated']

                    try:

                        update_date1 = parse(update_date)

                        # if date parsing goes wrong
                        if update_date1 == "" or update_date1 is None:

                            bad_date = re.findall(
                                r'\d+', str(update_date.encode('utf-8')))
                            p = 0

                            while p < len(bad_date):

                                bad_date1 = bad_date[p]

                                if len(bad_date1) != 4:
                                    p += 1

                                else:

                                    bad_date1 = "1/1/" + str(bad_date1)

                                    bad_update_date = parse(bad_date1)
                                    datasets[i]['extras'].update(
                                        {"date_updated": bad_update_date})
                                    datasets[i]['harmonised'].update(
                                        {'harmonised_UpdateDates': True})
                                    datasets[i]['harmonised'].update(
                                        {'harmonised_UpdateDates_date': datetime.now()})
                                    collection.save(datasets[i])
                                    counter += 1
                                    p = len(bad_date)

                            counter1 += 1
                        else:

                            counter += 1
                            datasets[i]['extras'].update(
                                {"date_updated": update_date1})
                            datasets[i]['harmonised'].update(
                                {'harmonised_UpdateDates': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_UpdateDates_date': datetime.now()})
                            collection.save(datasets[i])

                    except AttributeError:
                        i = i

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("Harmonised date_updated  in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised date updated in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Harmonised date updated in: " + \
        str(counter) + " datasets" + '\n'
    return message


def HarmoniseMetadataCreated(cat_url):

    counter = 0
    counter1 = 0
    bad_date = ""
    metadata_created = ""
    metadata_updated = ""

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if "metadata_created" in datasets[i].keys(
        ) and 'harmonised_MetadataCreated' not in datasets[i]['harmonised'].keys():
            metadata_created = datasets[i]['metadata_created']
            try:
                metadata_created1 = parser.parse(metadata_created)
                datasets[i].update({"metadata_created": metadata_created1})
                datasets[i]['harmonised'].update(
                    {'harmonised_MetadataCreated': True})
                datasets[i]['harmonised'].update(
                    {'harmonised_MetadataCreated_date': datetime.now()})
                collection.save(datasets[i])
                counter += 1
            except:
                try:
                    metadata_created = metadata_created.replace('_', '.')
                    metadata_created1 = parser.parse(metadata_created)
                    datasets[i].update({"metadata_created": metadata_created1})
                    datasets[i]['harmonised'].update(
                        {'harmonised_MetadataCreated': True})
                    datasets[i]['harmonised'].update(
                        {'harmonised_MetadataCreated_date': datetime.now()})
                    collection.save(datasets[i])
                    counter += 1
                except:
                    pass

        i += 1

    datasets[:] = []

    print("Harmonised metadata_created  in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised metadata_created in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Harmonised metadata_created in: " + \
        str(counter) + " datasets" + '\n'
    return message


def HarmoniseMetadataModified(cat_url):

    counter = 0
    counter1 = 0
    bad_date = ""
    counter_broken_links = 0
    metadata_created = ""
    metadata_updated = ""
    j = 0

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if "metadata_modified" in datasets[i].keys(
        ) and 'harmonised_MetadataModified' not in datasets[i]['harmonised'].keys():
            metadata_modified = datasets[i]['metadata_modified']
            try:
                metadata_modified1 = parser.parse(metadata_modified)
                datasets[i].update({"metadata_modified": metadata_modified1})
                datasets[i]['harmonised'].update(
                    {'harmonised_MetadataModified': True})
                datasets[i]['harmonised'].update(
                    {'harmonised_MetadataModified_date': datetime.now()})
                collection.save(datasets[i])
                counter += 1
            except:
                try:
                    metadata_modified = metadata_modified.replace('_', '.')
                    metadata_modified1 = parser.parse(metadata_modified)
                    datasets[i].update(
                        {"metadata_modified": metadata_modified1})
                    datasets[i]['harmonised'].update(
                        {'harmonised_MetadataModified': True})
                    datasets[i]['harmonised'].update(
                        {'harmonised_MetadataModified_date': datetime.now()})
                    collection.save(datasets[i])
                    counter += 1
                except:
                    pass

        i += 1

    datasets[:] = []

    print("Harmonised metadata_modified in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised metadata_modified in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Harmonised metadata_modified in: " + \
        str(counter) + " datasets" + '\n'
    return message


# Harmonise Formats
def HarmoniseFormats(cat_url):

    formats = []
    # dictionary for formats mappings.
    doc = collection1.find_one({"cat_url": cat_url})
    user = doc['user']
   #from dictionaries import basic_formats_dict
    # keys=basic_formats_dict.formats_dict.keys()
    # dictionary=str(basic_formats_dict.formats_dict).lower()

    formats_dictionary1 = formats_dict_basic.find_one()
    formats_dictionary2 = formats_dict_user.find_one({'user': str(user)})
    if formats_dictionary2 is None:
        formats_dictionary2 = {}
    formats_dictionary3 = formats_dict_catalogue.find_one(
        {'cat_url': str(cat_url)})
    #formats_dictionary={key: value for (key, value) in (formats_dictionary1.items() + formats_dictionary2.items() + formats_dictionary3.items())}
    formats_dictionary = {}
    i = 0
    while i < len(formats_dictionary1):
        formats_dictionary.update({formats_dictionary1.keys()[i]: formats_dictionary1[
                                  formats_dictionary1.keys()[i]]})
        i += 1
    i = 0
    while i < len(formats_dictionary2):
        formats_dictionary.update({formats_dictionary2.keys()[i]: formats_dictionary2[
                                  formats_dictionary2.keys()[i]]})
        i += 1
    i = 0
    while i < len(formats_dictionary3):
        formats_dictionary.update({formats_dictionary3.keys()[i]: formats_dictionary3[
                                  formats_dictionary3.keys()[i]]})
        i += 1

    count = 0
    while count < len(formats_dictionary):
        key = formats_dictionary.keys()[count]
        if '(dot)' in key:
            # print(key)
            key1 = key.replace('(dot)', '.')
            # print(key1)
            formats_dictionary.update(
                {key1: formats_dictionary[str(formats_dictionary.keys()[count])]})
            del formats_dictionary[key]
        count += 1
    keys = formats_dictionary.keys()

    # print(keys)
    dictionary = str(formats_dictionary).lower()
    # print(dictionary)

    i = 0
    counter = 0

    unharmonised_formats = []

    datasets = list(collection.find({'catalogue_url': cat_url}))

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_Formats' not in datasets[i]['harmonised'].keys():
            try:
                resources = datasets[i]['resources']
                j = 0
                while j < len(resources):
                    k = 0
                    while k < len(keys):
                        try:
                            format_normal = datasets[i]['resources'][
                                j]['format'].encode('utf-8')
                        except UnicodeDecodeError:
                            format_normal = datasets[i][
                                'resources'][j]['format']
                        try:
                            format_lower_strip = datasets[i]['resources'][
                                j]['format'].encode('utf-8').lower().strip()
                        except UnicodeDecodeError:
                            format_lower_strip = datasets[i][
                                'resources'][j]['format'].lower().strip()
                        if str(format_lower_strip) != str(format_normal):
                            datasets[i]['resources'][j].update(
                                {"format": str(format_lower_strip)})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Formats': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Formats_date': datetime.now()})
                            collection.save(datasets[i])
                        try:
                            format1 = datasets[i]['resources'][
                                j]['format'].encode('utf-8')
                        except UnicodeDecodeError:
                            format1 = datasets[i]['resources'][j]['format']
                        if str(format1) == keys[k] or str(
                                format1) == keys[k].lower():

                            datasets[i]['resources'][j].update(
                                {"format": str(formats_dictionary[str(keys[k])])})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Formats': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Formats_date': datetime.now()})
                            collection.save(datasets[i])
                            counter += 1
                        else:
                            if str(format1).lower() not in dictionary:
                                if format1 not in unharmonised_formats:
                                    unharmonised_formats.append(format1)

                        k += 1

                    k = 0

                    j += 1

                j = 0
                i += 1

            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised formats in " + str(counter) + " resourses.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised formats in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    i1 = 0
    text_file.write('Unharmonised Formats Found are: ' + '\n')
    while i1 < len(unharmonised_formats):
        text_file.write("'" +
                        str(unharmonised_formats[i1]) +
                        "':" +
                        "''," +
                        '\n')
        i1 += 1
    print(
        "The unharmonised formats are in the harmonisation_engine.log file." +
        '\n' +
        '\n' +
        '\n')
    message = "Harmonised formats in: " + str(counter) + " datasets" + '\n'
    return unharmonised_formats, message


# Harmonise Bad Formats
def HarmoniseBadFormats(cat_url):

    #from dictionaries import basic_formats_dict
    doc = collection1.find_one({"cat_url": cat_url})
    user = doc['user']
    # formats_dictionary=formats_dict.find_one({'cat_url':str(cat_url)})
    formats_dictionary1 = formats_dict_basic.find_one()
    formats_dictionary2 = formats_dict_user.find_one({'user': str(user)})
    if formats_dictionary2 is None:
        formats_dictionary2 = {}
    formats_dictionary3 = formats_dict_catalogue.find_one(
        {'cat_url': str(cat_url)})
    #formats_dictionary={key: value for (key, value) in (formats_dictionary1.items() + formats_dictionary2.items() + formats_dictionary3.items())}
    formats_dictionary = {}
    i = 0
    while i < len(formats_dictionary1):
        formats_dictionary.update({formats_dictionary1.keys()[i]: formats_dictionary1[
                                  formats_dictionary1.keys()[i]]})
        i += 1
    i = 0
    while i < len(formats_dictionary2):
        formats_dictionary.update({formats_dictionary2.keys()[i]: formats_dictionary2[
                                  formats_dictionary2.keys()[i]]})
        i += 1
    i = 0
    while i < len(formats_dictionary3):
        formats_dictionary.update({formats_dictionary3.keys()[i]: formats_dictionary3[
                                  formats_dictionary3.keys()[i]]})
        i += 1

    count = 0
    while count < len(formats_dictionary):
        key = formats_dictionary.keys()[count]
        if '(dot)' in key:
            # print(key)
            key1 = key.replace('(dot)', '.')
            # print(key1)
            formats_dictionary.update(
                {key1: formats_dictionary[str(formats_dictionary.keys()[count])]})
            del formats_dictionary[key]
            count -= 1
        count += 1
    keys = formats_dictionary.keys()

    # keys=basic_formats_dict.formats_dict.keys()

    i = 0
    counter = 0
    resources_json = []
    good_links_formats = []
    err = 0

    datasets = list(collection.find({'catalogue_url': cat_url}))

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_BadFormats' not in datasets[i]['harmonised'].keys():
            try:
                resources = datasets[i]['resources']

                j = 0

                while j < len(resources):

                    try:
                        url1 = datasets[i]['resources'][j]['url']
                    except IndexError:
                        i += 1
                        j = 0

                    try:
                        format1 = datasets[i]['resources'][j]['format']
                    except IndexError:
                        i += 1
                        j = 0
                    if (str(format1.encode('utf-8')) not in keys or str(format1.encode('utf-8')).lower() not in keys) and (
                            ', ' in str(format1.encode('utf-8')) or '(' in str(format1.encode('utf-8'))) or format1 == "":

                        format_find = find_format(url1)

                        if format_find != '' and format_find is not None and format_find != "html":
                            try:
                                datasets[i]['resources'][j].update(
                                    {"format": str(format_find).lower()})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_BadFormats': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_BadFormats_date': datetime.now()})
                                collection.save(datasets[i])

                                counter += 1
                            except IndexError:
                                err += 1

                        else:

                            if 'http:' in url1 or 'www.' in url1:
                                try:
                                    r = requests.get(url1)
                                except:
                                    print('url error')
                                    break

                                data = r.text
                                soup = BeautifulSoup(data)
                                links = soup.find_all('a')

                                l = 0

                                while l < len(links):
                                    link = links[l].get('href')
                                    if link != "":
                                        good_links_formats.append(link)
                                    if link != "" and link is not None:
                                        link_format = find_format(link)

                                        if link_format != "" and link_format is not None and link_format != "html":
                                            try:
                                                f = urllib2.urlopen(
                                                    link, timeout=1)
                                                filesize = f.headers[
                                                    "Content-Length"]
                                                mimetype = f.headers[
                                                    "Content-Type"]

                                                resource_json = {
                                                    "url": str(link),
                                                    "format": str(link_format).lower(),
                                                    "mimetype": str(mimetype),
                                                    "size": str(filesize)}
                                                resources_json.append(
                                                    resource_json)
                                            except urllib2.HTTPError as xxx_todo_changeme:
                                                urllib2.URLError = xxx_todo_changeme
                                                filesize = ""
                                            except socket.timeout as e:
                                                pass
                                                # print('timeout')
                                            except TypeError:
                                                filesize = ""

                                            except:
                                                filesize = ""

                                    l += 1

                            if len(good_links_formats) == 0:

                                formats = format1.split(', ')

                                m = 0
                                while m < len(formats):

                                    resource_json = {
                                        "url": str(
                                            url1.encode('utf-8')),
                                        "format": str(
                                            formats[m]).lower()}
                                    resources_json.append(resource_json)

                                    m += 1
                                m = 0
                            if len(resources_json) > 0:
                                datasets[i].update(
                                    {"resources": resources_json})
                                datasets[i].update(
                                    {"num_resources": len(resources_json)})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_BadFormats': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_BadFormats_date': datetime.now()})
                                collection.save(datasets[i])

                                counter += 1
                                resources_json[:] = []

                    j += 1

                j = 0
                i += 1

            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised bad formats in " + str(counter) + " resources.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised bad formats in: " +
                    str(counter) +
                    " resources" +
                    '\n')
    message = "Harmonised bad formats in: " + \
        str(counter) + " resources" + '\n'
    return message


# Handle Resource's Size
def HarmoniseSizes(cat_url):

    counter = 0
    counter_broken_links = 0
    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    # broken links counter

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_Sizes' not in datasets[i]['harmonised'].keys():
            try:
                resources = datasets[i]['resources']
                j = 0
                while j < len(resources):

                    resource_size = datasets[i]['resources'][j]['size']

                    resource_size = datasets[i]['resources'][j]['size']
                    if resource_size is None and datasets[i]['resources'][j]['format'] != 'HTML' and datasets[
                            i]['resources'][j]['format'] != '' and datasets[i]['resources'][j]['format'] != None:

                        try:
                         # print(datasets[i]['resources'][j]['url'])
                            f = urllib2.urlopen(
                                datasets[i]['resources'][j]['url'], timeout=1)
                            filesize = f.headers["Content-Length"]
                         # print('filezise: '+filesize)
                            mimetype = f.headers["Content-Type"]
                            datasets[i]['resources'][j].update(
                                {"size": str(filesize)})

                            if datasets[i]['resources'][j]['format'] == None:
                                datasets[i]['resources'][j].update(
                                    {"mimetype": str(mimetype)})

                            datasets[i]['harmonised'].update(
                                {'harmonised_Sizes': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Sizes_date': datetime.now()})
                            collection.save(datasets[i])

                            counter += 1

                        except urllib2.HTTPError as xxx_todo_changeme1:
                            urllib2.URLError = xxx_todo_changeme1
                            filesize = ""
                            #print('Url Error')
                            counter_broken_links += 1
                        except socket.timeout as e:
                            pass
                            # print('timeout')
                        except:
                            filesize = ""

                    j += 1

                j = 0
                i += 1

            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised size in " + str(counter) + " resources")
    print("Broken links found : " + str(counter_broken_links))
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised size in: " +
                    str(counter) +
                    " resources." +
                    '\n')
    message = "Harmonised size in: " + str(counter) + " resources." + '\n'
    return message


# Harmonise mimetypes
def HarmoniseMimetypes(cat_url):

    counter = 0
    counter_broken_links = 0

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    # broken links counter

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_Mimetypes' not in datasets[i]['harmonised'].keys():
            try:
                resources = datasets[i]['resources']

                j = 0
                while j < len(resources):
                    # mimetype=datasets[i]['resources'][j]['mimetype']

                    if 'mimetype' not in str(datasets[i]['resources'][j]) or datasets[
                            i]['resources'][j]['mimetype'] == None:

                        try:

                            f = urllib2.urlopen(
                                datasets[i]['resources'][j]['url'], timeout=1)
                            # print(datasets[i]['resources'][j]['url'])
                            mimetype = f.headers["Content-Type"]
                            datasets[i]['resources'][j].update(
                                {"mimetype": str(mimetype)})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Mimetypes': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Mimetypes_date': datetime.now()})
                            collection.save(datasets[i])

                            counter += 1

                        except urllib2.HTTPError as xxx_todo_changeme2:
                            urllib2.URLError = xxx_todo_changeme2
                            filesize = ""
                            counter_broken_links += 1
                        except socket.timeout as e:
                            pass
                            # print('timeout')
                        except:
                            filesize = ""
                            counter_broken_links += 1

                    j += 1

                j = 0
                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised mimetype in " + str(counter) + " resources")
    print("Broken links found : " + str(counter_broken_links))
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised resources in: " +
                    str(counter) +
                    " resources." +
                    '\n')
    message = "Harmonised mimetypes in: " + str(counter) + " resources." + '\n'
    return message


# Harmonise num tags and num resources
def HarmoniseNumTagsAndResources(cat_url):

    counter = 0
    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):

        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_NumTagsAndResources' not in datasets[
                i]['harmonised'].keys():
            try:
                resources = datasets[i]['resources']
                num_resources = len(resources)
                tags = datasets[i]['tags']
                try:
                    num_tags = len(tags)
                except:
                    num_tags = 0

                if 'num_tags' not in str(datasets[i]):
                    datasets[i].update({"num_tags": num_tags})
                    datasets[i]['harmonised'].update(
                        {'harmonised_NumTagsAndResources': True})
                    datasets[i]['harmonised'].update(
                        {'harmonised_NumTagsAndResources_date': datetime.now()})
                    collection.save(datasets[i])
                    counter += 1

                if 'num_resources' not in str(datasets[i]):
                    datasets[i].update({"num_resources": num_resources})
                    datasets[i]['harmonised'].update(
                        {'harmonised_NumTagsAndResources': True})
                    datasets[i]['harmonised'].update(
                        {'harmonised_NumTagsAndResources_date': datetime.now()})
                    collection.save(datasets[i])
                    counter += 1

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print(
        "harmonised num tags and num resources in " +
        str(counter) +
        " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised num tags and num resources in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    message = "Harmonised num tags and num resources in: " + \
        str(counter) + " datasets" + '\n'
    return message


# Harmonise catalogue url
def HarmoniseCatalogueUrl():
    step = 40000
    point = 0
    counter = 0
    endpoint = 0
    counter_broken_links = 0
    jobs = list(collection1.find())
    jobs1 = {}
    ct = 0
    url2 = ""
    url3 = ""
    url1 = ""
    test = []
    i = 0
    while i < len(jobs):

        if 'http://' in jobs[i]['cat_url']:
            url_main = jobs[i]['cat_url'][7:]
            url_main1 = url_main[0:url_main.find('/')]
            if 'www.' in url_main1:
                url_main1 = url_main1[4:]
            jobs1.update({str(url_main1): str(
                jobs[i]['cat_url'].encode('utf-8'))})
        if 'https://' in jobs[i]['cat_url']:
            url_main = jobs[i]['cat_url'][8:]
            url_main1 = url_main[0:url_main.find('/')]
            if 'www.' in url_main1:
                url_main1 = url_main1[4:]
            jobs1.update({str(url_main1): str(
                jobs[i]['cat_url'].encode('utf-8'))})
        i += 1
    keys = jobs1.keys()

    while endpoint < MaxDocsInDB:
        endpoint = point + step
        datasets = list(collection.find()[point:endpoint])
        i = 0

        while i < len(datasets):

            found = False

            if 'ckan_url' in datasets[i]:
                try:
                    url2 = datasets[i]['ckan_url']
                    if url2 != "" and url2 is not None:

                        if 'https' in url2:
                            url2 = url2[8:]
                        else:
                            url2 = url2[7:]
                        if '/' in url2:
                            url2 = url2[0:url2.find('/')].replace('_', '.')
                        else:
                            url2 = url2.replace('_', '.')
                        if 'www.' in url2:
                            url2 = url2[4:]
                        if "_" in url2:
                            print(url2)

                except KeyError:
                    nothing = ""

            try:
                url3 = datasets[i]['url']

                if url3 != "" and url3 is not None:
                    if 'https' in url3:
                        url3 = url3[8:]
                    else:
                        url3 = url3[7:]
                    if '/' in url3:
                        url3 = url3[0:url3.find('/')].replace('_', '.')
                    else:
                        url3 = url3.replace('_', '.')
                    if 'www.' in url3:
                        url3 = url3[4:]

            except KeyError:
                nothing = ""

            try:
                url1 = datasets[i]['catalogue_url']

                if url1 != "" and url1 is not None:
                    if 'https' in url1:
                        url1 = url1[8:]
                    else:
                        url1 = url1[7:]
                    if '/' in url1:
                        url1 = url1[0:url1.find('/')].replace('_', '.')
                    else:
                        url1 = url1.replace('_', '.')
                    if 'www.' in url1:
                        url1 = url1[4:]

            except KeyError:
                nothing = ""

            if url1 != "" and url1 is not None and found == False:

                k = 0
                while k < len(keys):
                    if str(keys[k].encode('utf-8')
                           ) == str(url1.encode('utf-8')):

                        cat_url = str(jobs1[str(keys[k])])

                        if 'http://' in cat_url:
                            cat_url = cat_url[7:]
                            cat_url = cat_url[0:cat_url.find('/')]
                            cat_url = 'http://' + cat_url
                        if 'https://' in cat_url:
                            cat_url = cat_url[8:]
                            cat_url = cat_url[0:cat_url.find('/')]
                            cat_url = 'https://' + cat_url

                        datasets[i].update({"catalogue_url": str(cat_url)})
                        collection.save(datasets[i])
                        found = True
                        counter += 1
                    k += 1

            if url2 != "" and url2 is not None and found == False:

                k = 0
                while k < len(keys):
                    if str(keys[k].encode('utf-8')
                           ) == str(url2.encode('utf-8')):

                        cat_url = str(jobs1[str(keys[k])])

                        if 'http://' in cat_url:
                            cat_url = cat_url[7:]
                            cat_url = cat_url[0:cat_url.find('/')]
                            cat_url = 'http://' + cat_url
                        if 'https://' in cat_url:
                            cat_url = cat_url[8:]
                            cat_url = cat_url[0:cat_url.find('/')]
                            cat_url = 'https://' + cat_url

                        datasets[i].update({"catalogue_url": str(cat_url)})
                        collection.save(datasets[i])
                        found = True
                        counter += 1
                    k += 1

            if url3 != "" and url3 is not None and found == False:
                k = 0

                while k < len(keys):
                    if str(keys[k].encode('utf-8')
                           ) == str(url3.encode('utf-8')):

                        cat_url = str(jobs1[str(keys[k])])

                        if 'http://' in cat_url:
                            cat_url = cat_url[7:]
                            cat_url = cat_url[0:cat_url.find('/')]
                            cat_url = 'http://' + cat_url
                        if 'https://' in cat_url:
                            cat_url = cat_url[8:]
                            cat_url = cat_url[0:cat_url.find('/')]
                            cat_url = 'https://' + cat_url

                        datasets[i].update({"catalogue_url": str(cat_url)})
                        collection.save(datasets[i])
                        found = True
                        counter += 1
                    k += 1

            i += 1

        point = endpoint
    datasets[:] = []
    print("modified: " + str(counter))


# Harmonise platform
def HarmonisePlatform():
    step = 40000
    point = 0
    counter = 0
    endpoint = 0
    counter_broken_links = 0
    bad_urls = []

    jobs = list(collection1.find())
    jobs1 = {}
    i = 0
    while i < len(jobs):

            # url_main=jobs[i]['cat_url'][7:]
            # url_main1=url_main[0:url_main.find('/')]
            # if 'www.' in url_main1:
        #	url_main1=url_main[4:]
           # if url_main1[-1]=='/':
        #	url_main1=url_main1[:-1]
        jobs1.update({str(jobs[i]['cat_url']): str(jobs[i]['type'])})

        # if 'https://' in jobs[i]['cat_url']:
        # url_main=jobs[i]['cat_url'][8:]
        # url_main1=url_main[0:url_main.find('/')]
        # if 'www.' in url_main1:
        #	url_main1=url_main[4:]
        # if url_main1[-1]=='/':
        #	url_main1=url_main1[:-1]
        # jobs1.update({str(url_main1):str(jobs[i]['type'])})
        i += 1

    keys = jobs1.keys()

    while endpoint < MaxDocsInDB:
        endpoint = point + step
        datasets = list(collection.find()[point:endpoint])
        i = 0

        while i < len(datasets):

            try:

                if 'platform' not in str(datasets[i].keys()):
                    # counter+=1
                    url1 = datasets[i]['catalogue_url']

                   # if 'https://' in url1:
                #	url1=url1[8:]
                   # if 'http://' in url1:
                #	url1=url1[7:]
                   # if '/' in url1:
                #	url1=url1[0:url1.find('/')]

                   # if 'www.' in url1:
                #	url1=url1[4:]
                    k = 0

                    while k < len(keys):
                        if str(url1) in str(
                                keys[k]) or str(url1) == str(
                                keys[k]) or str(
                                keys[k]) in str(url1):

                            datasets[i].update(
                                {"platform": str(jobs1[str(keys[k])])})
                            collection.save(datasets[i])
                            counter += 1
                        else:
                            pass
                            print(url1)

                        k += 1

                i += 1
            except KeyError:
                i += 1
        point = endpoint
        datasets[:] = []
    print("modified: " + str(counter))


# Harmonise Licenses
def HarmoniseLicenses(cat_url):

    point = 0
    counter = 0
    counter2 = 0
    counter_broken_links = 0
    licenses1 = []
    license1 = ""
    license2 = ""

    #from dictionaries import basic_licenses_dict

    doc = collection1.find_one({"cat_url": cat_url})
    try:
        user = doc['user']
    except:
        user = ''
    # licenses_dictionary=licenses_dict.find_one({'cat_url':str(cat_url)})
    licenses_dictionary1 = licenses_dict_basic.find_one()
    licenses_dictionary2 = licenses_dict_user.find_one({'user': str(user)})
    if licenses_dictionary2 is None:
        licenses_dictionary2 = {}
    licenses_dictionary3 = licenses_dict_catalogue.find_one(
        {'cat_url': str(cat_url)})
    licenses_dictionary = {}
    i = 0
    while i < len(licenses_dictionary1):
        licenses_dictionary.update({licenses_dictionary1.keys()[
                                   i]: licenses_dictionary1[licenses_dictionary1.keys()[i]]})
        i += 1
    i = 0
    while i < len(licenses_dictionary2):
        licenses_dictionary.update({licenses_dictionary2.keys()[
                                   i]: licenses_dictionary2[licenses_dictionary2.keys()[i]]})
        i += 1
    i = 0
    while i < len(licenses_dictionary3):
        licenses_dictionary.update({licenses_dictionary3.keys()[
                                   i]: licenses_dictionary3[licenses_dictionary3.keys()[i]]})
        i += 1

    count = 0
    while count < len(licenses_dictionary):
        key = licenses_dictionary.keys()[count]
        # print(key)
        if '(dot)' in key:
            # print(key)
            key1 = key.replace('(dot)', '.')
            # print(key1)
            licenses_dictionary.update({key1: licenses_dictionary[key]})
            del licenses_dictionary[key]
            count -= 1
        count += 1
    keys = licenses_dictionary.keys()

    # keys=basic_licenses_dict.licenses_dict.keys()
    dictionary = str(licenses_dictionary).lower()
    unharmonised_licenses = []

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        counter2 += 1
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_Licenses' not in datasets[i]['harmonised'].keys():
            try:
                found = False
                license = ""
                license1 = ""
                license2 = ""

                try:
                    license = datasets[i]['license_title']
                except KeyError:
                    license = ""
                try:
                    license1 = datasets[i]['license_id']

                except KeyError:
                    license1 = ""
                try:
                    license2 = datasets[i]['license']
                except KeyError:
                    license2 = ""

                if license != ""and license is not None and found == False:
                    try:
                        license = license.encode('utf-8')
                    except:
                        license = ""
                    j = 0
                    while j < len(keys):
                        if unicode(
                                license,
                                'utf-8').lower().strip() == keys[j].lower().strip():
                            datasets[i].update(
                                {"license_id": str(licenses_dictionary[keys[j]].encode('utf-8'))})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Licenses': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Licenses_date': datetime.now()})
                            collection.save(datasets[i])
                            found = True
                            counter += 1
                        if str(license).lower().strip() not in dictionary:

                            if str(license).lower().strip(
                            ) not in unharmonised_licenses:
                                unharmonised_licenses.append(
                                    str(license).lower().strip())
                        j += 1

                if license1 != ""and license1 is not None and found == False:
                    try:
                        license1 = license1.encode('utf-8')
                    except:
                        license1 = ""
                    j = 0
                    while j < len(keys):

                        if unicode(license1,
                                   'utf-8').lower().strip().replace('  ',
                                                                    ' ') == keys[j].lower().strip():
                            datasets[i].update(
                                {"license_id": str(licenses_dictionary[keys[j]].encode('utf-8'))})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Licenses': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Licenses_date': datetime.now()})
                            collection.save(datasets[i])
                            found = True
                            counter += 1
                        if str(license1).lower().strip() not in dictionary:

                            if str(license1).lower().strip(
                            ) not in unharmonised_licenses:
                                unharmonised_licenses.append(
                                    str(license1).lower().strip())

                        j += 1

                if license2 != ""and license2 is not None and found == False:
                    try:
                        license2 = license2.encode('utf-8')
                    except:
                        license2 = ""
                    j = 0
                    while j < len(keys):
                        if unicode(
                                license2,
                                'utf-8').lower().strip() == keys[j].lower().strip():
                            datasets[i].update(
                                {"license_id": str(licenses_dictionary[keys[j]])})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Licenses': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Licenses_date': datetime.now()})
                            collection.save(datasets[i])
                            found = True
                            counter += 1
                        if str(license2).lower().strip() not in dictionary:

                            if str(license2).lower().strip(
                            ) not in unharmonised_licenses:
                                unharmonised_licenses.append(
                                    str(license2).lower().strip())
                        j += 1

                if "license_id" not in str(datasets[i]):

                    try:
                        license1 = datasets[i]['license_title']
                    except KeyError:
                        try:
                            license1 = datasets[i]['license']
                        except KeyError:
                            license1 = ""
                    try:
                        license1 = license1.encode('utf-8')
                    except:
                        license1 = ""
                    j = 0

                    if license1 != "" and license1 is not None:
                        datasets[i].update({"license_id": str(license1)})
                        datasets[i]['harmonised'].update(
                            {'harmonised_Licenses': True})
                        datasets[i]['harmonised'].update(
                            {'harmonised_Licenses_date': datetime.now()})
                        collection.save(datasets[i])
                        license2 = datasets[i]['license_title']

                    if license2 != "":
                        while j < len(keys):
                            if str(license1) == keys[j] or str(
                                    license1).lower() == keys[j].lower():
                                datasets[i].update(
                                    {"license_id": str(licenses_dictionary[str(keys[j])])})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_Licenses': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_Licenses_date': datetime.now()})
                                collection.save(datasets[i])

                            j += 1

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    # licenses1.sort()
    # k=0
    # while k<len(licenses1):
    # print('"'+str(licenses1[k].encode('utf-8'))+'":"'+str(licenses1[k].encode('utf-8')+'",'))
    # k+=1

    print('\n' + '\n' + "harmonised license in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised license in: " +
                    str(counter) +
                    " datasets" +
                    '\n')
    i1 = 0
    text_file.write(str(datetime.now()) +
                    'The Unharmonised Licenses are: ' + '\n')
    while i1 < len(unharmonised_licenses):
        text_file.write("'" +
                        str(unharmonised_licenses[i1]) +
                        "':" +
                        "''," +
                        '\n')
        i1 += 1
    print(
        "The unharmonised licenses are in the harmonisation_engine_log.txt file." +
        '\n' +
        '\n' +
        '\n')
    message = "Harmonised license in: " + str(counter) + " datasets" + '\n'
    return unharmonised_licenses, message


# function that harmonises the language labels found in extras according
# to the existing dictionary in dictinaries.py file
def HarmoniseLanguageLabels(cat_url):

    counter = 0
    counter_broken_links = 0
    from dictionaries import languages_labels_dict
    languagemappings = languages_labels_dict.languages_labels_dict
    languagemappings_keys = languagemappings.keys()

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_LanguageLabels' not in datasets[i]['harmonised'].keys():
            try:
                extras = datasets[i]['extras']
                if extras != "":

                    extras_keys = extras.keys()
                    j = 0
                    k = 0
                    while j < len(extras_keys):

                        while k < len(languagemappings_keys):

                            if extras_keys[j] == languagemappings_keys[k]:

                                extras[str(languagemappings[str(languagemappings_keys[k])])] = extras[
                                    str(extras_keys[j])]
                                del extras[str(extras_keys[j])]
                                datasets[i]['harmonised'].update(
                                    {'harmonised_LanguageLabels': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_LanguageLabels_date': datetime.now()})
                                collection.save(datasets[i])
                                counter += 1
                            k += 1

                        k = 0
                        j += 1

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonise language labels in " + str(counter) + " datasets.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised language labels in: " +
                    str(counter) +
                    " datasets." +
                    '\n')
    message = "Harmonised language labels in: " + \
        str(counter) + " datasets." + '\n'
    return message


def HarmoniseLanguages(cat_url):

    counter = 0
    counter_broken_links = 0
    from dictionaries import languages_dict
    languagemappings = languages_dict.languages_dict
    languagemappings_keys = languagemappings.keys()

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_Languages' not in datasets[i]['harmonised'].keys():
            try:
                extras = datasets[i]['extras']
                if extras != "":
                    language = datasets[i]['extras']['language']
                    j = 0
                    while j < len(languagemappings_keys):
                        if str(language.encode('utf-8')) == languagemappings_keys[j] or str(
                                language.encode('utf-8')).lower() == languagemappings_keys[j].lower():
                            datasets[i]['extras'].update(
                                {"language": str(languagemappings[str(languagemappings_keys[j])])})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Languages': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Languages_date': datetime.now()})
                            collection.save(datasets[i])
                            counter += 1
                        j += 1

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised language in " + str(counter) + " datasets.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised language in: " +
                    str(counter) +
                    " datasets." +
                    '\n')
    message = "Harmonised language in: " + str(counter) + " datasets." + '\n'
    return message


def HarmoniseCategoryLabels(cat_url):

    counter = 0
    counter_broken_links = 0
    from dictionaries import basic_category_labels_dict
    doc = collection1.find_one({"cat_url": cat_url})
    try:
        user = doc['user']
    except:
        user = ''
    # categories_dictionary=categories_dict.find_one({'cat_url':str(cat_url)})
    categories_dictionary1 = categories_dict_basic.find_one()
    categories_dictionary2 = categories_dict_user.find_one({'user': str(user)})
    if categories_dictionary2 is None:
        categories_dictionary2 = {}
    categories_dictionary3 = categories_dict_catalogue.find_one(
        {'cat_url': str(cat_url)})
    categories_dictionary = {}
    i = 0
    while i < len(categories_dictionary1):
        categories_dictionary.update({categories_dictionary1.keys(
        )[i]: categories_dictionary1[categories_dictionary1.keys()[i]]})
        i += 1
    i = 0
    while i < len(categories_dictionary2):
        categories_dictionary.update({categories_dictionary2.keys(
        )[i]: categories_dictionary2[categories_dictionary2.keys()[i]]})
        i += 1
    i = 0
    while i < len(categories_dictionary3):
        categories_dictionary.update({categories_dictionary3.keys(
        )[i]: categories_dictionary3[categories_dictionary3.keys()[i]]})
        i += 1

    count = 0
    while count < len(categories_dictionary):
        key = categories_dictionary.keys()[count]
        if '(dot)' in key:
            # print(key)
            key1 = key.replace('(dot)', '.')
            # print(key1)
            categories_dictionary.update({key1: categories_dictionary[key]})
            del categories_dictionary[key]
            count -= 1
        count += 1

    categorymappings = categories_dictionary
    categorymappings_keys = categories_dictionary.keys()

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0

    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_CategoryLabels' not in datasets[i]['harmonised'].keys():

            try:
                extras = datasets[i]['extras']

                if extras != "":
                    extras_keys = extras.keys()
                    j = 0
                    k = 0
                    while j < len(extras_keys):

                        while k < len(categorymappings_keys):

                            if extras_keys[j] == categorymappings_keys[k]:

                                extras[
                                    unicode(
                                        categorymappings[
                                            unicode(
                                                categorymappings_keys[k])])] = extras[
                                    unicode(
                                        extras_keys[j])]
                                del extras[unicode(extras_keys[j])]
                                datasets[i]['harmonised'].update(
                                    {'harmonised_CategoryLabels': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_CategoryLabels_date': datetime.now()})
                                collection.save(datasets[i])
                                counter += 1
                            k += 1

                        k = 0
                        j += 1

                i += 1
            except KeyError:
                i += 1
        else:
            i += 1

    datasets[:] = []
    print("harmonised category labels in " + str(counter) + " datasets")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised category labels in: " +
                    str(counter) +
                    " datasets." +
                    '\n')
    message = "Harmonised category labels in: " + \
        str(counter) + " datasets." + '\n'
    return message


def HarmoniseCategories(cat_url):

    i = 0
    j = 0
    counter = 0
    tagsarray = []
    c = 0
    dictionary1 = []
    gs = goslate.Goslate()

    datasets = list(collection.find({'catalogue_url': cat_url}))
    i = 0
    while i < len(datasets):
        if 'harmonised' not in datasets[i].keys():
            datasets[i].update({'harmonised': {}})
            collection.save(datasets[i])
        if 'harmonised_Categories' not in datasets[i]['harmonised'].keys():
            c += 1
            found = False
            try:
                    # print(i)

                try:
                    category = datasets[i]['extras']['category'].replace(
                        '[',
                        '').replace(
                        ']',
                        '').replace(
                        '{',
                        '').replace(
                        '}',
                        '').replace(
                        '"',
                        '').replace(
                        '(',
                        '').replace(
                            ')',
                            '').replace(
                                '&',
                                ',').replace(
                                    'quot ;',
                                    '').replace(
                                        'Quot;',
                        '')

                except:
                    category = ""

                # print(category)
                # counter+=1
                try:
                    language_detection = gs.detect(str(category))
                except urllib2.HTTPError as xxx_todo_changeme4:
                    urllib2.URLError = xxx_todo_changeme4
                    language_detection = 'en'
                except:
                    language_detection = 'en'
                if language_detection != 'en':

                    j = 0
                    dictionary = list(collection_dict.find())
                    k = 0
                    dictionary1[:] = []

                    while k < len(dictionary):
                        if "id" in str(
                                dictionary[k].keys()[0].encode('utf-8')):
                            temp_word = dictionary[k].keys()[1].encode('utf-8')
                        else:
                            temp_word = dictionary[k].keys()[0].encode('utf-8')

                        if temp_word not in dictionary1:
                            dictionary1.append(temp_word)
                        k += 1
                    if category in dictionary1:
                        while j < len(dictionary):

                            if category in dictionary[j]:

                                translation = dictionary[j][
                                    category.encode('utf-8')]
                                print(
                                    category.encode('utf-8') +
                                    ' --> ' +
                                    translation.encode('utf-8'))
                                datasets[i]['extras'].update(
                                    {"category": translation.encode('utf-8')})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_Categories': True})
                                datasets[i]['harmonised'].update(
                                    {'harmonised_Categories_date': datetime.now()})
                                found = True
                                try:
                                    collection.save(datasets[i])
                                    counter += 1
                                except:
                                    print('storage error')

                            j += 1

                    if found == False and category.encode(
                            'utf-8') not in dictionary1:

                        try:
                            category_en = gs.translate(str(category), 'en')
                        except urllib2.HTTPError as xxx_todo_changeme3:
                            urllib2.URLError = xxx_todo_changeme3
                            print("translation error")
                        except:
                            print("translation error")
                        if category_en.encode('utf-8') != "":
                            try:
                                collection_dict.save(
                                    {category.encode('utf-8'): category_en.encode('utf-8')})
                                # counter+=1
                            except:
                                print('storage error')
                            datasets[i]['extras'].update(
                                {"category": str(category_en.encode('utf-8'))})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Categories': True})
                            datasets[i]['harmonised'].update(
                                {'harmonised_Categories_date': datetime.now()})
                            print(
                                category.encode('utf-8') +
                                ' -//-> ' +
                                category_en.encode('utf-8'))
                            try:
                                collection.save(datasets[i])
                                counter += 1
                            except:
                                print('storage error')
                        else:
                            print('translation failed!')

                i += 1

            except KeyError:
                i += 1

        else:
            i += 1

    datasets[:] = []

    print("harmonised category in " + str(counter) + " datasets.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine - ' +
                    " Harmonised category value in: " +
                    str(counter) +
                    " datasets." +
                    '\n')
    message = "Harmonised category value in: " + \
        str(counter) + " datasets." + '\n'
    return message


def country_names(dataset={}):
    email_validator = lepl.apps.rfc3696.Email()

    # tld (top level domain) check in catalogue_url
    match = dataset['catalogue_url'][7:]
    tokens = re.findall('\w+', match)
    country_name = ''
    for tt in tokens:
        tt = '.' + tt
        result = countries_table.find_one({'tld': tt})
        if result:
            country_name = result['ISO']
            # break
            return country_name

    if not country_name:
        # tld (top level domain) check in author_email field
        if ('author_email' in dataset.keys()
                and email_validator(dataset['author_email'])):
            match = dataset['author_email'].split('.')
            result = countries_table.find_one({'tld': '.' + match[-1]})
            if result:
                country_name = result['ISO']
                return country_name
    if not country_name:
        # tld (top level domain) check in maintainer_email field
        if ('maintainer_email' in dataset.keys()
                and email_validator(dataset['maintainer_email'])):
            match = dataset['maintainer_email'].split('.')
            result = countries_table.find_one({'tld': '.' + match[-1]})
            if result:
                country_name = result['ISO']
                return country_name
    if not country_name:
        # tld (top level domain) check in extras.contact-email field
        if ('contact-email' in dataset['extras'].keys()
                and email_validator(dataset['extras']['contact-email'])):
            match = dataset['extras']['contact-email'].split('.')
            result = countries_table.find_one({'tld': '.' + match[-1]})
            if result:
                country_name = result['ISO']
                return country_name
    if not country_name:
        if 'eu_country' in dataset['extras'].keys() and dataset[
                'extras']['eu_country']:
            match = dataset['extras']['eu_country']
            result = countries_table.find_one(
                {'$or': [{'fips': match.upper()}, {'ISO': match.upper()}]})
            if result:
                country_name = result['ISO']
                return country_name
                # continue
    if not country_name:
        if 'harvest_catalogue_url' in dataset['extras'].keys() and dataset['extras'][
                'harvest_catalogue_url']:
            match = dataset['extras']['harvest_catalogue_url'][7:]
            tokens = re.findall('\w+', match)
            # tokens=str.split('.')
            for tt in tokens:
                tt = '.' + tt
                result = countries_table.find_one({'tld': tt})
                if result:
                    country_name = result['ISO']
                    # break
                    return country_name
    if not country_name:
        if 'url' in dataset.keys() and dataset['url']:
            match = dataset['url']
            tokens = re.findall('\w+', match)
            for tt in tokens:
                tt = '.' + tt
                result = countries_table.find_one({'tld': tt})
                if result:
                    country_name = result['ISO']
                    # break
                    return country_name
    if not country_name:
        if 'country' in dataset['extras'].keys() and dataset[
                'extras']['country']:
            match = dataset['extras']['country']
            tokens = re.split('[\(\),;:\d]', match)
            tokens = filter(None, tokens)
            tokens = [x.strip() for x in tokens]
            name_found = False
            counter = cities_table.find({'name': {'$in': tokens}}).count()
            if counter == 1:
                result = cities_table.find_one({'name': {'$in': tokens}})
                for tt in tokens:
                    if result['name'] == tt:
                        country_name = result['country code']
                        result = countries_table.find_one(
                            {'$or': [{'fips': country_name}, {'ISO': country_name}]})
                        if result:
                            country_name = result['ISO']
                            return country_name
                        else:
                            print(
                                'Could not find country for %s, for dataset with _id: ObjectId(\'%s\')' %
                                (country_name, dataset['_id']))
                            country_name = ''
                        break
            elif counter > 1:
                try:
                    results = cities_table.find({'name': {'$in': tokens}})
                    list_country_names = []
                    related_tokens = []
                    for result in results:
                        for tt in tokens:
                            if result['name'] == tt:
                                found = countries_table.find_one(
                                    {'$or': [{'fips': result['country code']}, {'ISO': result['country code']}]})
                                if found:
                                    list_country_names.append(found['ISO'])
                                    related_tokens.append(tt)
                                    break

                    list_country_names = list(set(list_country_names))
                    if len(list_country_names) == 1:
                        country_name = list_country_names[0]
                        return country_name
                    else:
                        print(
                            'Different country names found %s for tokens: %s, for dataset with _id: ObjectId(\'%s\')' %
                            (', '.join(
                                str(x) for x in list_country_names), ', '.join(
                                str(x) for x in related_tokens), dataset['_id']))
                except TypeError as e:
                    print(e, dataset['_id'])
    if not country_name:
        if 'bbox-west-long' in dataset['extras'].keys() and \
                'bbox-south-lat' in dataset['extras'].keys() and \
                'bbox-east-long' in dataset['extras'].keys() and \
                'bbox-north-lat' in dataset['extras'].keys() and \
                dataset['extras']['bbox-west-long']:
            bbox = BoundingBox([(float(dataset['extras']['bbox-west-long']),
                                 float(dataset['extras']['bbox-south-lat'])),
                                (float(dataset['extras']['bbox-east-long']),
                                 float(dataset['extras']['bbox-north-lat']))])
            list_country_names = [
                c.name for c in country_subunits_containing_point(
                    lon=bbox.center[0], lat=bbox.center[1])]
            if len(list_country_names) == 1:
                country_name = list_country_names[0]
                return country_name
            else:
                print(list_country_names)
                results = cities_table.find(
                    {'name': {'$in': list_country_names}})
                list_country_names = []
                related_tokens = []
                for result in results:
                    found = countries_table.find_one(
                        {'$or': [{'fips': result['country code']}, {'ISO': result['country code']}]})
                    if found:
                        list_country_names.append(found['ISO'])
                        related_tokens.append(tt)
                        break

                list_country_names = list(set(list_country_names))
                if len(list_country_names) == 1:
                    country_name = list_country_names[0]
                    return country_name
                else:
                    print(
                        'Different country names found %s for tokens: %s, for dataset with _id: ObjectId(\'%s\')' %
                        (', '.join(
                            str(x) for x in list_country_names), ', '.join(
                            str(x) for x in related_tokens), dataset['_id']))
    if not country_name:
        if 'notes' in dataset.keys() and dataset['notes']:
            match = dataset['notes']
            tokens = re.findall('[a-zA-Z_]+', match)
            tokens = list(set(tokens))
            name_found = False
            counter = cities_table.find({'name': {'$in': tokens}}).count()
            if counter == 1:
                result = cities_table.find_one({'name': {'$in': tokens}})
                for tt in tokens:
                    if result['name'] == tt:
                        country_name = result['country code']
                        result = countries_table.find_one(
                            {'$or': [{'fips': country_name}, {'ISO': country_name}]})
                        if result:
                            country_name = result['ISO']
                            return country_name
                        else:
                            print(
                                'Could not find country for %s, for dataset with _id: ObjectId(\'%s\')' %
                                (country_name, dataset['_id']))
                            country_name = ''
                        break
            elif counter > 1:
                try:
                    results = cities_table.find({'name': {'$in': tokens}})
                    list_country_names = []
                    related_tokens = []
                    for result in results:
                        for tt in tokens:
                            if result['name'] == tt:
                                found = countries_table.find_one(
                                    {'$or': [{'fips': result['country code']}, {'ISO': result['country code']}]})
                                if found:
                                    list_country_names.append(found['ISO'])
                                    related_tokens.append(tt)
                                    break

                    list_country_names = list(set(list_country_names))
                    if len(list_country_names) == 1:
                        country_name = list_country_names[0]
                        return country_name
                    else:
                        print(
                            'Different country names found %s for tokens: %s, for dataset with _id: ObjectId(\'%s\')' %
                            (', '.join(
                                str(x) for x in list_country_names), ', '.join(
                                str(x) for x in related_tokens), dataset['_id']))
                except TypeError as e:
                    print(e, dataset['_id'])
                # results=cities_table.find({'name':{'$in':tokens}})
                # for result in results:
                #     print(result)

            # print ('harvest_catalogue_url does not exist in dataset\'s extras with _id: ObjectId(%s) ' % dataset['_id'] )
            # continue

    if not country_name:
        print (
            'Still cannot find country name for dataset with _id: ObjectId(\'%s\') ' %
            dataset['_id'])
        # continue

    return country_name


def HarmoniseCountries(cat_url):

    no_countriesNotFound = 0
    no_categoriesNotHarmonized = 0
    no_datasetWithoutCategory = 0
    total_counter = 0
    conn_delay = 1
    more_datasets = True
    db = collection
    unharmonised_category_values = []
    custom_logging.setup_logger('log1', r'uncaught_categories.log')
    logger = logging.getLogger('log1')
    # geo=Geography(countries_table,cities_table)

    while(more_datasets):
        try:
            conn_delay = 1

            datasets = list(collection.find({'catalogue_url': cat_url}))
            # print(datasets)
            # datasets=db.find({'_id':ObjectId('53d0e67cce2e3b31d8149263')})
            for dataset in datasets:
                if 'harmonised' not in dataset:
                    dataset.update({'harmonised': {}})
                    collection.save(dataset)
                if 'harmonised_Countries' not in dataset['harmonised'].keys():
                    total_counter = total_counter + 1
                    country_name = country_names(dataset)

                    if country_name:
                            # if (total_counter % 1000) ==0:
                            #     print ('%i country names are harmonized' % total_counter)
                        db.update({'_id': dataset['_id']}, {
                                  '$set': {'country': country_name}})
                        dataset['harmonised'].update(
                            {'harmonised_Categories': True})
                        dataset['harmonised'].update(
                            {'harmonised_Categories_date': datetime.now()})
                        db.save(dataset)
                    else:
                        no_countriesNotFound = no_countriesNotFound + 1

                    # categories harmonization
                    try:
                                # categories harmonization
                        if 'extras' in dataset.keys() and 'category' in dataset[
                                'extras'].keys():
                            category, sub_category, non_empty = HarmoniseCategoryValues(
                                cat_url, logger, dataset)
                            if non_empty and category:
                                db.update({'_id': dataset['_id']}, {
                                          '$set': {'category': category, 'sub_category': sub_category}})
                            elif non_empty:
                                # print('%s dataset with category \'%s\' is not harmonized' % (dataset['_id'],dataset['extras']['category']))
                                no_categoriesNotHarmonized = no_categoriesNotHarmonized + 1
                                if 'category' in dataset['extras']:
                                    print('unharmonised:' + '\n' +
                                          str(dataset['extras']['category']))
                        else:
                            no_datasetWithoutCategory = no_datasetWithoutCategory + 1

                    except AttributeError as e:
                            # print(e,dataset)
                        if dataset['extras'][
                                'category'] not in unharmonised_category_values:
                            unharmonised_category_values.append(
                                dataset['extras']['category'])

                    if (total_counter % 1000) == 0:
                        print ('%i datasets were parsed...' % total_counter)

            print('\n')
            if no_countriesNotFound > 0:
                print (
                    '%i country names where not possible to be found\n' %
                    no_countriesNotFound)
            if no_categoriesNotHarmonized > 0:
                print(
                    '%i datasets with category field are not harmonized' %
                    no_categoriesNotHarmonized)
            if no_datasetWithoutCategory > 0:
                print(
                    '%i datasets has no category field\n' %
                    no_datasetWithoutCategory)
            if (total_counter % 1000) > 0:
                print ('%i datasets were parsed\n' % total_counter)

            more_datasets = False

        except errors.AutoReconnect as e:
            print(e)
            time.sleep(conn_delay)
            conn_delay = conn_delay**2
            if(conn_delay > 128):
                print(
                    'connection dropped because there could be some different error of what believed...')
                more_datasets = False
    return unharmonised_category_values


def HarmoniseCategoryValues(cat_url, logger, dataset={}):
    mapped_categories = []
    mapped_subcategories = []
    # category_values=categories_values_dict.find_one({'cat_url':str(cat_url)})
    doc = collection1.find_one({"cat_url": cat_url})
    try:
        user = doc['user']
    except:
        user = ''
    categories_values_dictionary1 = categories_values_dict_basic.find_one()
    categories_values_dictionary2 = categories_values_dict_user.find_one({
                                                                         'user': str(user)})
    if categories_values_dictionary2 is None:
        categories_values_dictionary2 = {}
    categories_values_dictionary3 = categories_values_dict_catalogue.find_one(
        {'cat_url': str(cat_url)})
    category_values = {}
    i = 0
    while i < len(categories_values_dictionary1):
        category_values.update({categories_values_dictionary1.keys()[
                               i]: categories_values_dictionary1[categories_values_dictionary1.keys()[i]]})
        i += 1
    i = 0
    while i < len(categories_values_dictionary2):
        category_values.update({categories_values_dictionary2.keys()[
                               i]: categories_values_dictionary2[categories_values_dictionary2.keys()[i]]})
        i += 1
    i = 0
    while i < len(categories_values_dictionary3):
        category_values.update({categories_values_dictionary3.keys()[
                               i]: categories_values_dictionary3[categories_values_dictionary3.keys()[i]]})
        i += 1

    count = 0
    while count < len(category_values):
        key = category_values.keys()[count]
        if '(dot)' in key:
            key1 = key.replace('(dot)', '.')
            category_values.update(
                {key1: category_values[str(category_values.keys()[count])]})
            del category_values[key]
        count += 1
     # datemappings_keys=category_values.keys()
    is_not_available = False
    non_empty = False

    if bool(dataset):
        try:
            u_theme = dataset['extras']['category'].encode('utf-8')
            normalized = re.split('&quot;|[\"\'\[{\(\)}\],;]', u_theme)
            for a in normalized:
                if a.strip() not in ['', None, 'quot']:
                    try:
                        non_empty = True
                        group_id = category_values[a.lower().strip()] \
                            if a.lower().strip() in category_values.keys() \
                            else dictionaries.odalisk_category_values[a.lower().strip()]
                        if group_id == 'n.a.':
                            is_not_available = True
                        else:
                            try:
                                mapped_categories.index(
                                    dictionaries.category_groups[group_id][0])
                            except ValueError:
                                mapped_categories.append(
                                    dictionaries.category_groups[group_id][0])
                                mapped_subcategories.append(
                                    dictionaries.category_groups[group_id][1])
                    except KeyError:
                        logger.warning(
                            'search term category:\'%s\'is not handled' %
                            (a.lower().strip()))
                        #print('search term category:\'%s\'is not handled' % (a.lower().strip()))
        except UnicodeDecodeError as e:
            print(e)

    if len(mapped_categories) == 0 and is_not_available:
        mapped_categories.append('n.a.')
        mapped_subcategories.append('n.a.')

    return (mapped_categories, mapped_subcategories, non_empty)


def FinishHarmonisationEngine():

    i = 0
    j = 0
    counter = 0
    tagsarray = []
    step = 40000
    point = 0
    endpoint = 0
    prob = 0
    while endpoint < MaxDocsInDB:
        endpoint = point + step
        datasets = list(collection.find()[point:endpoint])
        i = 0
        while i < len(datasets):

            try:

                datasets[i].update({"Harmonization_finished": True})
                collection.save(datasets[i])
                counter += 1

                i += 1

            except:
                prob += 1

        point = endpoint
        datasets[:] = []

    print("modified " + str(counter) + " datasets.")
    text_file.write(str(datetime.now()) +
                    ' - harmonisation_engine  - ' +
                    " Harmonation engine Finished.")


def IdentifyLanguageFromNotes():

    i = 0
    j = 0
    counter = 0
    tagsarray = []
    step = 40000
    point = 0
    c = 0
    endpoint = 0
    dictionary1 = []
    gs = goslate.Goslate()
    while endpoint < MaxDocsInDB:
        endpoint = point + step
        datasets = list(collection.find()[point:endpoint])
        i = 0
        while i < len(datasets):
            if 'Harmonization_finished' not in datasets[
                    i].keys() and 'language' not in str(datasets[i]['extras']):

                try:
                    notes = datasets[i]['notes'].encode('utf-8')
                    print(notes)
                except:
                    notes = ""

                    # print(category)
                    # counter+=1
                try:
                    language_detection = gs.detect(notes)
                    print(language_detection)
                except urllib2.HTTPError as xxx_todo_changeme5:
                    urllib2.URLError = xxx_todo_changeme5
                    language_detection = ''
                    print('HTTP ERROR')
                    # except:
                    #	language_detection=''
                if language_detection != '':

                    datasets[i]['extras'].update(
                        {'language': str(language_detection)})
                    collection.save(datasets[i])
                    counter += 1
                i += 1

            else:
                i += 1
        point = endpoint
        datasets[:] = []

    print("identified language in " + str(counter) + " datasets.")


def mails():

    i = 0
    j = 0
    counter = 0
    tagsarray = []
    step = 40000
    point = 0
    c = 0
    endpoint = 0
    mails_collection = db.author_email
    text_file_mails = open(
        '/var/local/ckan/default/pyenv/src/harmonisation_engine/emails.txt', "a")
    while endpoint < MaxDocsInDB:
        endpoint = point + step
        datasets = list(mails_collection.find()[point:endpoint])
        i = 0
        while i < len(datasets):
            try:
                mail = datasets[i]['emails'].encode('utf-8')
            except:
                mail = ''
            try:
                name = datasets[i]['names'].encode('utf-8')
            except:
                name = ''
            cat_url = datasets[i]['catalogue'].encode('utf-8')
            try:
                countrycode = datasets[i]['countrycode'].encode('utf-8')
            except:
                countrycode = ''
            text_file_mails.write(
                "'" +
                str(mail) +
                "'," +
                "'" +
                str(name) +
                "'," +
                "'" +
                str(cat_url) +
                "'" +
                '\n')

            i += 1

        point = endpoint
        datasets[:] = []


# Copy_Odm_to_Odm_harmonised()
# HarmoniseTags()
# HarmoniseExtras()
# HarmoniseDatesLabels()
# HarmoniseReleaseDates()
# HarmoniseUpdateDates()
# HarmoniseMetadataCreated()
# HarmoniseMetadataModified()
# HarmoniseFormats()
# HarmoniseBadFormats()
# HarmoniseSizes()
# HarmoniseMimetypes()
# HarmoniseNumTagsAndResources()
# HarmoniseCatalogueUrl()
# HarmonisePlatform()
# HarmoniseLicenses()
# HarmoniseStringsToIntegers()
# HarmoniseLanguageLabels()
# HarmoniseLanguages()
# HarmoniseCategoryLabels()
# HarmoniseCategories()
# HarmoniseCountries()
# IdentifyLanguageFromNotes()
# FinishHarmonisationEngine()
# mails()
