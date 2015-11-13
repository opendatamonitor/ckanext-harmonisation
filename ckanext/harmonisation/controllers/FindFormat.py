import urllib2
import requests
import re
import difflib
import json
import logging



def FindFormat(url_temp):
  #check if link contains a known type of file
		type1=""
		if '.jsp' in url_temp:
			type1='jsp'
		if '.pdf' in url_temp:
			type1='pdf' 
		if '.rdf' in url_temp:
			type1='rdf'	
		if '.xls' in url_temp: 
			type1='xls'
		if '.htm' in url_temp:
			type1='html'
		if '.csv' in url_temp:
			type1='csv'
		if '.kml' in url_temp:
			type1='kml'
		if '.xml' in url_temp:
			type1='xml'
		if '.json' in url_temp:
			type1='json'
		if '.api' in url_temp:
			type1='api'
		if '.gzip' in url_temp:
			type1='gzip'
		if '.viz' in url_temp:
			type1='viz'
		if '.tsv' in url_temp:
			type1='tsv'
		if '.zip' in url_temp:
			type1='zip'
		if '.shp' in url_temp:
			type1='shp'
		if '.ods' in url_temp:
			type1='ods'
		if '.wms' in url_temp:
			type1='wms'	
		if '.kmz' in url_temp:
			type1='kmz'
		if '.doc' in url_temp:
			type1='doc'
		if '.shape' in url_temp:
			type1='shape'
		if '.txt' in url_temp:
			type1='txt'	
		if '.7z' in url_temp:
			type1='7z'
		if '.wfs' in url_temp:
			type1='wfs'
		if '.turtle' in url_temp:
			type1='turtle'
		if '.gml' in url_temp:
			type1='gml'
		if '.geojson' in url_temp:
			type1='geojson'
		if '.odt' in url_temp:
			type1='odt'
		if '.aspx' in url_temp:
			type1='aspx'
		if '.ppt' in url_temp:
			type1='ppt'
		if '.rtf' in url_temp:
			type1='rtf'
		
		
		if 'jsp' not in url_temp and 'pdf' not in url_temp and 'rdf' not in url_temp and 'xls' not in url_temp and 'htm' not in url_temp and 'csv' not in url_temp and 'kml' not in url_temp and 'xml' not in url_temp and 'json' not in url_temp and 'api' not in url_temp and 'gzip' not in url_temp and 'viz' not in url_temp and 'tsv' not in url_temp and 'zip' not in url_temp and 'shp' not in url_temp and 'ods' not in url_temp and 'wms' not in url_temp and 'kmz' not in url_temp and 'doc' not in url_temp and 'shape' not in url_temp and 'txt' not in url_temp and '7z' not in url_temp and 'wfs' not in url_temp and 'turtle' not in url_temp and 'gml' not in url_temp and 'geojson' not in url_temp and 'odt' not in url_temp and 'aspx' not in url_temp and 'ppt' not in url_temp and 'rtf' not in url_temp:
			type1=""
		
		## get filesize from url
		
		

		return type1
