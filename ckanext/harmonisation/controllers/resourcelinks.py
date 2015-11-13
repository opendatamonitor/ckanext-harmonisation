import sys
from pymongo import MongoClient,errors
from bson.objectid import ObjectId
from bson.errors import InvalidStringData
import re
from socket import error as SocketError
import errno
import time
import requests
import warnings
from requests.packages.urllib3 import exceptions
import os,signal
import hashlib
from custom_logging import Logger
import logging
# import TLSv1Fix
# import urllib
import httplib
from urllib3 import PoolManager, Timeout,exceptions
import urllib2
import urlparse
import mimetypes
# import certifi

# ## InsecurePlatformWarning
# import urllib3.contrib.pyopenssl
# urllib3.contrib.pyopenssl.inject_into_urllib3()

import eventlet
eventlet.monkey_patch(thread=False)

from pyasn1.type import error as pyasn1Error
import OpenSSL

import magic

class TooBig(requests.exceptions.RequestException):
    """The response was way too big."""
    # print('The response was way too big')


class Resources:
    MAX_PATH = 255

    def __init__(self):
        self.total_counter = 0


    def get_format_type(self,map_mime_format,mime=None,dataset_mime=None,filename=None):
        format_type = None
        mime_type = None

        if mime and mime in map_mime_format:
            format_type = map_mime_format[mime]
        elif dataset_mime and dataset_mime in map_mime_format:
            format_type = map_mime_format[dataset_mime]
        elif filename:
            mime = magic.Magic(mime=True)
            mime_type = mime.from_file(filename.encode('utf8'))
            if mime_type in map_mime_format:
                format_type = map_mime_format[mime_type]

        return format_type,mime_type


    def get_link_info(self,dataset,client,mappings=[],logger = None):
        if 'resources' in dataset.keys():
            try:
                # start = time.time()
                ## find broken resources
                for resource_id in range(0,len(dataset['resources'])):
                    try:
                        (status_code,file_size,mime_type,md5hash,format_type,file_name)=self.get_status_code(mappings[0],dataset['catalogue_url'],dataset['resources'][resource_id],
                                dataset['_id'],logger,False)
                        if status_code:
                            if 'size' not in dataset['resources'][resource_id] or \
                                    dataset['resources'][resource_id]['size'] in ['',None,0]:
                                if (mime_type is not None) and \
                                    ('mimetype' not in dataset['resources'][resource_id] or dataset['resources'][resource_id]['mimetype'] in ['',None]):
                                    client.update({'_id':dataset['_id']},
                                        {'$set':{'resources.'+str(resource_id)+'.status_code':status_code,
                                            'resources.'+str(resource_id)+'.file_hash':md5hash,
                                            'resources.'+str(resource_id)+'.file_size':int(file_size) if file_size is not None else None,
                                            'resources.'+str(resource_id)+'.size':int(file_size) if file_size is not None else None,
                                            'resources.'+str(resource_id)+'.mimetype':mime_type,
                                            'resources.'+str(resource_id)+'.format':format_type,
                                            'resources.'+str(resource_id)+'.file_name':file_name,
                                            }}
                                        )
                                else:
                                    client.update({'_id':dataset['_id']},
                                        {'$set':{'resources.'+str(resource_id)+'.status_code':status_code,
                                            'resources.'+str(resource_id)+'.file_hash':md5hash,
                                            'resources.'+str(resource_id)+'.file_size':int(file_size) if file_size is not None else None,
                                            'resources.'+str(resource_id)+'.size':int(file_size) if file_size is not None else None,
                                            'resources.'+str(resource_id)+'.format':format_type,
                                            'resources.'+str(resource_id)+'.file_name':file_name,
                                            }}
                                        )
                            else:
                                if (mime_type is not None) and \
                                    ('mimetype' not in dataset['resources'][resource_id] or dataset['resources'][resource_id]['mimetype'] in ['',None]):
                                    client.update({'_id':dataset['_id']},
                                        {'$set':{'resources.'+str(resource_id)+'.status_code':status_code,
                                            'resources.'+str(resource_id)+'.file_hash':md5hash,
                                            'resources.'+str(resource_id)+'.file_size':int(file_size) if file_size is not None else None,
                                            'resources.'+str(resource_id)+'.mimetype':mime_type,
                                            'resources.'+str(resource_id)+'.format':format_type,
                                            'resources.'+str(resource_id)+'.file_name':file_name,
                                            }}
                                        )
                                else:
                                    client.update({'_id':dataset['_id']},
                                        {'$set':{'resources.'+str(resource_id)+'.status_code':status_code,
                                            'resources.'+str(resource_id)+'.file_hash':md5hash,
                                            'resources.'+str(resource_id)+'.file_size':int(file_size) if file_size is not None else None,
                                            'resources.'+str(resource_id)+'.format':format_type,
                                            'resources.'+str(resource_id)+'.file_name':file_name,
                                            }}
                                        )

                        # end = time.time() - start
                        # if end>2:
                        #     print("time: %f for ObjectId('%s')" % (end,dataset['_id']))
                    except KeyError as e:
                        print('KeyError: %s for ObjectId(\'%s\')' % (e,dataset['_id']))
                    except ValueError as e:
                        print('ValueError: %s for ObjectId(\'%s\')' % (e,dataset['_id']))


                    time.sleep(0.5)
            except errors.CursorNotFound as e:
                print(e)

    def get_tmp_file(self,url,_id):
        baseFile = os.path.basename(url)
        if not baseFile:
            # print('no basefile for %s, ObjectId(\'%s\')' % (url,_id))
            baseFile='custom.html'
        elif len(baseFile) > self.MAX_PATH:
            baseFile = 'too_long'

        #move the file to a more unique path
        os.umask(0002)
        temp_path = "/tmp/"

        return os.path.join(temp_path,baseFile)


    def filename_from_url(self,url):
        return os.path.basename(urlparse.urlsplit(url)[2])

    def download_file(self,url):
        """Create an urllib2 request and return the request plus some useful info"""
        name = self.filename_from_url(url)
        r = urllib2.urlopen(urllib2.Request(url))
        info = r.info()
        if 'Content-Disposition' in info:
            # If the response has Content-Disposition, we take filename from it
            name = info['Content-Disposition'].split('filename=')[1]
            if name[0] == '"' or name[0] == "'":
                name = name[1:-1]
        elif r.geturl() != url:
            # if we were redirected, take the filename from the final url
            name = filename_from_url(r.geturl())
        content_type = None
        if 'Content-Type' in info:
            content_type = info['Content-Type'].split(';')[0]
        # Try to guess missing info
        if not name and not content_type:
            name = 'unknown'
        elif not name:
            name = 'unknown' + mimetypes.guess_extension(content_type) or ''
        elif not content_type:
            content_type = mimetypes.guess_type(name)[0]

        return r, name, content_type


    def get_status_code(self,mime_format_map,cat_url,resource,dataset_id,logger,get_resource_size=False):
        url = resource['url']

        TOO_BIG = 1024 * 1024 * 500 # 500MB
        CHUNK_SIZE = 1024 * 128
        connect_timeout = 15.0
        read_timeout = 8.0
        status_code=None
        file_size=None
        md5hash=None
        mimetype=None
        formatype = None
        file_name = None

        if url.startswith('ftp://'):
            try:
                req,filename,mimetype = self.download_file(url)
                status_code = 200
            except urllib2.URLError,e :
                print(e.args,dataset_id)
                status_code = 404
        else:
            try:
                # http = PoolManager(
                #     cert_reqs='CERT_REQUIRED', # Force certificate check.
                #     ca_certs=certifi.where(),  # Path to the Certifi bundle.
                # )

                # start=time.time()
                s = requests.Session()
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36"}
                # s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36'
                #http://{0}?".format(url))
                resp = None
                with eventlet.Timeout(15):
                    try:
                        resp = s.get(url,headers=headers,stream=True,timeout=(connect_timeout,read_timeout),verify=True)
                    except (
                            requests.exceptions.MissingSchema
                            ,requests.exceptions.InvalidURL
                            # ,requests.exceptions.InvalidSchema
                    ) as e:
                        # cat_url = cat_url[:4]+'s'+cat_url[4:]
                        url = cat_url + url
                        # print(e,url)
                        resp = s.get(url,headers=headers,stream=True,timeout=(connect_timeout,read_timeout),verify=True)

                    # print(resp.headers)
                # try:
                    # resp = s.get(url, stream=True,timeout=(connect_timeout,read_timeout))
                # except requests.exceptions.SSLError as e:
                #     # print (e,dataset_id,'test')
                #     # s.mount(url,MyAdapter())
                #     # resp = s.get(url, stream=True,timeout=(connect_timeout,read_timeout),verify=False)
                #     print(time.time()-start)
                #     print("trying silent unverified")
                #     resp = self.silent_unverified_get(url,stream=True,timeout=(connect_timeout,read_timeout))

                status_code = resp.raise_for_status()
                status_code = resp.status_code
                # print(time.time()-start)

                if resp.ok:
                    # md5hash=self.md5_for_file(url)
                    md5 = hashlib.md5()
                    md5.update(url.encode('utf8'))
                    md5hash=md5.hexdigest()

                    # print(resp.headers)
                    try:
                        file_size=resp.headers['content-length']
                        mimetype=resp.headers['content-type']
                        formatype,_ = self.get_format_type(mime_format_map,mimetype,resource['mimetype'])
                        try:
                            file_name = re.search(r'\"(.+?)\"',resp.headers['content-disposition']).group(0)
                            if file_name.startswith('"') and file_name.endswith('"'):
                                file_name = file_name[1:-1]
                                if isinstance(file_name,unicode):
                                    file_name = file_name.encode('utf8')
                                elif isinstance(file_name,str):
                                    file_name = file_name.decode('latin1')
                        except AttributeError,e:
                            pass
                        except ValueError as e:
                            print(e,dataset_id,file_name)
                            logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
                            file_name = None

                    except KeyError as e:
                        try:
                            # print('download file for stats')
                            tmp_file = self.get_tmp_file(url,dataset_id)
                            with open(tmp_file, 'wb') as f:
                                # Set the signal handler and a 5-second alarm
                                # signal.signal(signal.SIGALRM, self.handler)
                                # signal.alarm(5)
                                content_length=0
                                try:
                                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                                        if chunk: # filter out keep-alive new chunks
                                            content_length = content_length + CHUNK_SIZE
                                            if content_length > TOO_BIG:
                                                raise TooBig(response=resp)

                                            f.write(chunk)
                                            f.flush()
                                            # signal.alarm(0)          # Disable the alarm
                                except httplib.IncompleteRead as e:
                                    print(e,dataset_id)
                                    f.write(e.partial)
                                    f.flush()

                            file_size=os.stat(tmp_file).st_size
                            formatype,mimetype = self.get_format_type(mime_format_map,filename=tmp_file)

                        except IOError as e:
                            print(e,dataset_id)
                            logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
                        except TooBig:
                            pass
                        finally:
                            ## try to delete file ##
                            try:
                                os.remove(tmp_file)
                            except OSError, e:  ## if failed, report it back to the user ##
                                print ("OSError: %s - %s - ObjectId(\'%s\')."
                                        % (e.filename,e.strerror,dataset_id))
                            except UnboundLocalError as e:
                                print ("UnboundLocalError: %s - ObjectId(\'%s\')."
                                        % (e,dataset_id))

            except (requests.HTTPError) as e:
                # print ('HTTP ERROR "%s" occured from dataset ObjectId(\'%s\')' % (e,dataset_id))
                status_code=e.response.status_code
            except requests.exceptions.ChunkedEncodingError as e:
                resp = e.partial
                # print(e,dataset_id)
                logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except requests.exceptions.SSLError as e:
                print ("SSLError: %s for %s" % (e,dataset_id))
                # print(e.errno)
                logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (requests.exceptions.MissingSchema
                    ,requests.exceptions.InvalidURL
                    ,requests.packages.urllib3.exceptions.LocationParseError
                    ,requests.exceptions.InvalidSchema
                    ,requests.exceptions.TooManyRedirects
                    ) as e:
                print (e,dataset_id)
                try:
                    logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
                except UnicodeEncodeError as e:
                    print(e,dataset_id)
            except UnicodeEncodeError as e:
                print ('UnicodeEncodeError: %s, ObjectId(\'%s\')' % (e,dataset_id))
            except ValueError as e:
                print(e,dataset_id,url)
                logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (requests.exceptions.ReadTimeout) as e:
                print ("ReadTimeout: %s for %s" % (e,dataset_id))
                # print(resp.status_code)
                status_code = 598
                logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (requests.exceptions.ConnectTimeout) as e:
                print ("ConnectTimeout: %s for %s" % (e,dataset_id))
                # print(resp.status_code)
                status_code = 408
                logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (requests.exceptions.ConnectionError) as e:
                print ("ConnectError: %s for %s" % (e,dataset_id))
                # print(resp.status_code)
                status_code = 503
                logger.error('error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (eventlet.timeout.Timeout) as e:
                print ("Timeout: %s for %s" % (e,dataset_id))
                status_code = 408
                logger.error('eventlet error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (pyasn1Error.ValueConstraintError) as e:
                print ("pyASN1Error: %s for %s" % (e,dataset_id))
                logger.error('ValueConstraint error: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))
            except (OpenSSL.SSL.SysCallError) as e:
                print(e)
                logger.error('SysCallError: %s for dataset: ObjectId(\'%s\')' % (e,dataset_id))


        return (status_code,file_size,mimetype,md5hash,formatype,file_name)


    def silent_unverified_get(self,*args, **kwargs):
        kwargs['verify'] = False
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", exceptions.InsecureRequestWarning)
            return requests.get(*args, **kwargs)


    def md5_for_file(self,filename,block_size=2**20):
        md5 = hashlib.md5()
        with open(filename,'rb') as f:
            data = f.read(block_size)
            while len(data) > 0:
                md5.update(data)
                data = f.read(block_size)
        return md5.hexdigest()

    def handler(self,signum, frame):
        print 'Signal handler called with signal', signum
        raise IOError("Couldn't open device!")


def test(db,collection):
    conn = MongoClient('127.0.0.1',27017)

    mappings_mimetypes_to_formats = {}
    client = conn[db]['mappings_for_mimetypes_formats']
    # get list of mappings in mimetypes and formats
    results = client.find()
    mappings = []
    for doc in results:
        mappings_mimetypes_to_formats[doc["MIME Type / Internet Media Type"]] = str(doc["File Extension"])[1:]
    mappings.append(mappings_mimetypes_to_formats)

    client = conn[db][collection]

    total_counter=0
    skip=0
    conn_delay=1
    more_datasets=True

    resource_links = Resources()

    regex=re.compile('open.nrw')

    # custom_logging.setup_logger('log1', r'link_checking.log')
    # logger = logging.getLogger('log1')
    with Logger('log1', r'links.log'):
        while(more_datasets):
            try:
                conn_delay=1
                EXCLUDE_COUNTRIES=re.compile('geodata\.gov\.gr')

                # datasets=db.find({'$and':[{'category':{'$in':['',None]}},{'extras.category':{'$exists':True}}]})
                # datasets=client.find({'_id':ObjectId('559e7afb9daf0925163e75ee')})
                # datasets=client.find().sort('_id').skip(skip).limit(31-skip)
                # datasets=client.find({'resources.status_code':{'$exists':False}},timeout=False).sort('_id').skip(skip)
                # datasets = client.find({'_id':ObjectId("560f356f9daf099d02b06ef2")})
                # datasets=client.find().sort('_id').skip(skip)
                # datasets=client.find({'catalogue_url':regex},timeout=False).sort('_id').skip(skip)
                # datasets=client.find({'catalogue_url':regex,
                #     'resources.file_size':{'$exists':False}},timeout=False).sort('_id').skip(skip)
                # datasets=client.find({'platform':'socrata',
                #     'resources.file_size':{'$exists':False}},timeout=False).sort('_id').skip(skip)
                # datasets=client.find({'$or':[{'resources.file_size':{'$exists':False}},
                #     {'resources.size':{'$in':['',None]}}]},
                #     timeout=False).sort('_id').skip(skip)
                # datasets=client.find({'resources.status_code':{'$exists':False}},
                #     timeout=False).sort('_id').skip(skip)
                # datasets=client.find({'catalogue_url':{'$in':[
                #     'http://geodata.gov.gr',
                #     # 'http://opendatagortynia.gr/',
                #     # 'http://data.gov.gr/'
                #     ]}},timeout=False).sort('_id')
                datasets=client.find({'resources.status_code':{'$exists':False},'catalogue_url':{'$not':EXCLUDE_COUNTRIES}},
                    timeout=False).sort('_id')
                datasets=client.find({'_id': {'$gt': ObjectId('55e828a49daf099c5491337f')}, 'resources.size':{'$in':['',None] },'catalogue_url':{'$not':EXCLUDE_COUNTRIES}},
                    timeout=False).sort('_id')
                # datasets=client.find({'_id': ObjectId('55e829a09daf099c549133be')},
                #     timeout=False).sort('_id')
                # datasets=client.find({'catalogue_url': { '$in':['http://datos.gob.es','http://opendata.aragon.es/catalogo/']}},
                #     timeout=False).sort('_id')


                # datasets.sort( '_id' ) # recommended to use time or other sequential parameter.
                # datasets.skip( skip )

                for dataset in datasets:
                    total_counter+=1

                    resource_links.get_link_info(dataset,client,mappings,logging.getLogger('log1'))

                    if (total_counter % 10000) == 0:
                        print ('%i datasets were parsed...' % total_counter)
                        # raise errors.OperationFailure('cursor id blp blp not valid at server')

                    skip += 1

                more_datasets=False

            except errors.OperationFailure as e:
                msg = str(e)
                if not ( msg.startswith( "cursor id" ) and msg.endswith( "not valid at server" ) ):
                    raise
                else:
                    print(msg)
                    print(skip)
            except errors.AutoReconnect as e:
                print(e)
                time.sleep(conn_delay)
                conn_delay=conn_delay**2
                if(conn_delay>128):
                    print('connection dropped because there could be some different error of what believed...')
                    more_datasets=False
            finally:
                del datasets


if __name__=='__main__':
    db_name='odm'
    db_collection='odm_harmonised'
    test(db_name,db_collection)
