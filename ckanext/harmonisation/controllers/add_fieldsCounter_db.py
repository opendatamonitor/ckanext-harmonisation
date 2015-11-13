from pymongo import MongoClient
from bson.objectid import ObjectId


class FieldsCounter:
    excluded_fields = [
            'attrs_counter',
            '_id',
            'type',
            'isopen',
            'harmonised',
            'revision_id',
            'num_tags',
            'num_resources',
            'title',
            'notes',
            'ckan_url',
            'owner_org',
            'maintainer',
            'private',
            'platform',
            'id',
            'metadata_created',
            'metadata_modified',
            'updated_dataset',
            'author',
            'author_email',
            'license_id',
            'license_title',
            'license',
            'tags',
            'catalogue_url',
            'metadata_updated',
            'isopen',
            'url',
            'resources',
            'revision_id',
            'copied',
            ]

    def run(self,client,db='odm',coll='odm_harmonised'):
        conn = client[db][coll]
        # result=conn.find({'attrs_counter':{'$exists':False}})
        # result=conn.find({'_id':ObjectId("5603dd408964914b29671a73")})
        result=conn.find({})

        for doc in result:
            unique_keys = []
            items = self.flatten_dict(doc)
            for key in items:
                if key not in self.excluded_fields:
                    unique_keys.append(key)

            conn.update({'_id':doc['_id']},{'$set':{'attrs_counter':len(unique_keys)}},upsert=True)

        conn.ensure_index('attrs_counter')

    # def iterate_object(self,doc,counter=0):
    #     if isinstance(doc,list):
    #         for key in range(0,len(doc)):
    #             self.iterate_object(doc[key],counter)
    #     elif isinstance(doc,dict):
    #         for key in doc:
    #             self.iterate_object(doc[key],counter)
    #     else:
    #         counter+=1
    #         print(doc)
    #         return (counter,doc)

    def flatten_dict(self,d,depth=0):
        def expand(key, value, depth=0):
            if isinstance(value, dict):
                if depth > 1:
                    return [ (key, value) ]
                else:
                    return [ (key + '.' + k, v) for k, v in self.flatten_dict(value,depth+1).items() ]
            else:
                return [ (key, value) ]

        items = [ item for k, v in d.items() for item in expand(k, v, depth+1) ]

        return dict(items)


if __name__=="__main__":
    calc_attrs = FieldsCounter()
    calc_attrs.run(MongoClient('localhost',27017),'odm_clean','test')
