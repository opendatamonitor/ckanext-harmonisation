import urllib2
import requests
import re
import difflib
import json
import logging

FORMATS = [
'jsp', 'pdf', 'rdf', 'xls', 'htm', 'csv', 'kml', 'xml', 'json',
'api', 'gzip', 'viz', 'tsv', 'zip', 'shp', 'ods', 'wms', 'kmz',
'doc', 'shape', 'txt', '7z', 'wfs', 'turtle', 'gml', 'geojson',
'odt', 'aspx', 'ppt', 'rtf' ]

def find_format(url):
  # check if link contains a known type of file
    url_temp = url.lower()

    for fmt in FORMATS:
        if '.{f}'.format(f=fmt) in url_temp:
            return fmt

    return ""