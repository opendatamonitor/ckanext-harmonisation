from __future__ import print_function
from collections import defaultdict
# import pysolr
import processor
# from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import hashlib
import jellyfish
from string import punctuation

class Searcher:
    def __init__(self,db='odm',collection='dedups',ngram_size=4,sig_size=1):
        # create a connection to a solr server
        # self.s = pysolr.Solr('http://localhost:8983/solr-dedup',timeout=10)
        # conn=self.mongo_conn(host,port)
        # if (conn!=1):
        #     self.db_conn = self.mongo_collection(conn,collection)
        self.minhash = processor.MinHashSignature(sig_size)
        self.shingles = processor.Shingles()
        self.ngram_size = ngram_size

        self.db = db
        self.collection = collection

    # def mongo_conn(self,host,port):
    #     try:
    #         return MongoClient(host,port)
    #     except errors.ConnectionFailure as e:
    #         print(e)
    #         return 1
    #
    # def mongo_collection(self,conn,collection,db='odm'):
    #     return conn[db][collection]

    def md5hasher(self,data):
        data=' '.join(filter(None, (word.strip(punctuation) \
                for word in data.split())))
        # print('searcher: ',data)
        md5hash = hashlib.md5()
        md5hash.update(data.encode('utf8'))
        return md5hash.hexdigest()

    def find_near_dups_by_minhash(self,mongo_data,db_conn,max_dist=2):
        try:
            content = None
            try:
                if mongo_data['title'] != None and mongo_data['notes'] != None:
                    content = unicode(mongo_data['title']).lower() + ' ' + unicode(mongo_data['notes']).lower()
                else:
                    content = unicode(mongo_data['title']).lower()
            except KeyError as e:
                # print('Error message: %s' % e.message)
                if e.message == 'notes':
                    content = unicode(mongo_data['title']).lower()
                elif e.message == 'title':
                    print('KeyError: %s for %s' % (e,mongo_data['_id']))
                    return dict()
            if content is None:
                return dict()
            mhash_content = self.minhash.sign(self.shingles.shingle(content,self.ngram_size))
            md5 = self.md5hasher(content)
            # print(md5)
            return self.near_dup_search(mongo_data,max_dist,content,md5,mhash_content,db_conn)
        except TypeError as e:
            print('TypeError: %s, ObjectId(\'%s\')' % (e,mongo_data['_id']))
            return dict()

    def near_dup_search(self,data,max_dist,content,md5,query,db_conn):
        q=''
        for mh in query:
            q+=str(mh)
        # results = self.s.search(q='*:*',fq='content_sg:\"'+q+'\"')
        results = db_conn[self.db][self.collection].find({'content_sg':q,
            '_id':{'$gt':ObjectId(data['_id'])},
            'catalogue_url':{'$ne':data['catalogue_url']},
            # 'dupl':{'$ne':True}
            })

        matches = defaultdict(list)
        # Just loop over it to access the results.
        for result in results:
            # print("The title is '{0}'.".format(result['content'].encode('utf8')))
            if md5 == result['md5_hash']:
                # matches.append(result)
                matches['Exact'].append(result)
            elif jellyfish.levenshtein_distance(content,
                    result['content']) < max_dist*len(content):
                matches['Approximate'].append(result)

        # if len(matches) > 0:
        #     print('Dups for _id:%s found: ' % data['_id'],end='')
        #     # for match in matches:
        #     print(','.join([str(match['_id']) for match in matches]))
        if all (k in matches for k in ('Exact' and 'Approximate')):
            del matches['Approximate']

        return matches
