# import pysolr
import hashlib
import processor
# from pymongo import MongoClient, errors
from bson.objectid import ObjectId
import sys

class Indexer:
    def __init__(self,db='odm',collection='dedups',ngram_size=4,sig_size=1):
        # create a connection to a solr server
        # self.s = pysolr.Solr('http://localhost:8983/solr-dedup',timeout=10)
        # conn = self.mongo_conn(host,port)
        # if (conn!=1):
        #     self.db_conn=self.mongo_collection(conn,collection=collection)
        self.minhash = processor.MinHashSignature(sig_size)
        self.shingles = processor.Shingles()
        self.ngram_size = ngram_size

        self.db = db
        self.collection = collection


    def md5hasher(self,data):
        # print(data)
        md5hash = hashlib.md5()
        md5hash.update(data.encode('utf8'))
        return md5hash.hexdigest()

    def build_index(self,mongo_data,db_conn):
        counter=0
        # add a document to the index
        md5hash = hashlib.md5()
        content=[]
        for data in mongo_data:
            # self.minhash.sign(self.shingles.shingle(data['content'],self.ngram_size))
            # try:
                # md5hash.update(data['content'])
                doc = {
                    '_id':data['_id'],
                    'catalogue_url':data['catalogue_url'],
                    'content':data['content'],
                    'md5_hash':self.md5hasher(data['content']),
                    # 'num_words':data['content'],
                    # 'content_ng':data['content_ng'],
                    'content_sg': self.minhash.sign(self.shingles.shingle(data['content'],self.ngram_size))
                }
                content.append(doc)

                counter+=1
                if (counter % 10000 == 0):
                    print('Processing records {0} so far...'.format(counter))
            # except UnicodeEncodeError as e:
            #     print('ObjectId(%s): %s' % (data['_id'],e))
            # except:
            #     e = sys.exc_info()[0]
            #     print('ObjectId(%s): %s' % (data['_id'],data['content']))
            #     return

        # self.s.add(content)
        # print('Solr indexing finished')

        for d in content:
            content_sg=''
            for mh in d['content_sg']:
                content_sg+=str(mh)
            # print(d['md5_hash'])
            db_conn[self.db][self.collection].update({'_id':ObjectId(d['_id'])},
                    {'$set': {
                        'catalogue_url':d['catalogue_url'],
                        'content':d['content'],
                        'md5_hash':d['md5_hash'],
                        'content_sg' : content_sg
                        }},True
                    )

        # s_ids=set()
        # print("Collecting all ids from odm_harmonised collection...")
        # results=db_conn[self.db]['odm_harmonised'].find({},{'_id':1})
        # for _id in results:
        #     s_ids.add(_id['_id'])
        # print("ids collected.")
        #
        # s_dedup_ids=set()
        # print("Collecting all ids from dedups collection...")
        # results=db_conn[self.db][self.collection].find({},{'_id':1})
        # for _id in results:
        #     s_dedup_ids.add(_id['_id'])
        # print("ids collected.")
        #
        # s_dedup_ids.difference_update(s_ids)
        # if len(s_dedup_ids)>0:
        #     query={'_id':{'$in':list(s_dedup_ids)}}
        #     # print(query)
        #     res=db_conn[self.db][self.collection].remove(query)
        #
        # print("removed %d docs from dedups" % len(s_dedup_ids))

        db_conn[self.db][self.collection].ensure_index('content_sg')

