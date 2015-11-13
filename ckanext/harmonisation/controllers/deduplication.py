# -*- coding: utf8 -*-
import sys
from collections import defaultdict
from pymongo import MongoClient, errors,cursor_manager,ASCENDING
from bson.objectid import ObjectId
import hashlib
import logging
from datetime import datetime as dt,date
# from dateutil.parser import parse as parse_date
from sets import Set
from functools import reduce
import csv
from termcolor import colored,cprint

import indexer
# print indexer.__file__
import searcher

from string import punctuation
import pprint

class Logger:
# logging.DEBUG
# logging.INFO
# logging.WARNING
# logging.ERROR
# logging.CRITICAL
    def __init__(self,logger_name, log_file, level=logging.INFO):
        l = logging.getLogger(logger_name)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.fileHandler = logging.FileHandler(log_file, mode='w')
        self.fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(self.fileHandler)
        # l.addHandler(streamHandler)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.fileHandler.close()

class DeDuplication:
    def __init__(self):
        self.THRES = 0.7

        self.NEWEST = 'NEWEST'
        self.OLDEST = 'OLDEST'
        self.UNKNOWN = 'UNKNOWN'
        self.UNDEFINED = 'undefined'

        self.content = []

        # setup_logger('log1', r'partialOrdering.log')
        # setup_logger('log2', r'dup_errors.log')
        # # logger=logging.basicConfig(filename='dublicates.log', filemode='w', level=logging.DEBUG)
        # self.logger = logging.getLogger('log1')
        # # lhStdout = logger.handlers[0]  # stdout is the only handler initially
        # # logger.removeHandler(lhStdout)
        # self.logger_errors = logging.getLogger('log2')
        #
    def toposort2(self,data):
        """Dependencies are expressed as a dictionary whose keys are items
        and whose values are a set of dependent items. Output is a list of
        sets in topological order. The first set consists of items with no
        dependences, each subsequent set consists of items that depend upon
        items in the preceeding sets.

        >>> print '\\n'.join(repr(sorted(x)) for x in toposort2({
        ...     2: set([11]),
        ...     9: set([11,8]),
        ...     10: set([11,3]),
        ...     11: set([7,5]),
        ...     8: set([7,3]),
        ...     }) )
        [3, 5, 7]
        [8, 11]
        [2, 9, 10]

        """
        # Ignore self dependencies.
        for k, v in data.items():
            v.discard(k)
        # Find all items that don't depend on anything.
        extra_items_in_deps = reduce(set.union, data.itervalues()) - set(data.iterkeys())
        # Add empty dependences where needed
        data.update({item:set() for item in extra_items_in_deps})
        while True:
            ordered = set(item for item, dep in data.iteritems() if not dep)
            if not ordered:
                break
            yield ordered
            data = {item: (dep - ordered)
                    for item, dep in data.iteritems()
                        if item not in ordered}
        assert not data, "Cyclic dependencies exist among these items:\n%s" % '\n'.join(repr(x) for x in data.iteritems())
        # assert not data, "A cyclic dependency exists amongst %r" % data

    def parse_csv(self,input_file,delimiter='|'):
        data = defaultdict(set)
        try:
            with open(input_file) as fp:
                csv.field_size_limit(sys.maxsize)
                # input_file=csv.DictReader((row for row in fp if not row.startswith('#')),delimiter=file_sep)
                cf = csv.reader(fp,delimiter=delimiter)
                for row in cf:
                    if (len(row)==2):
                        cat_list=row[1].split(',')
                        data[row[0]]=set(cat_list)
                    else:
                        print("Warning: an error exists in partialOrder.csv in line: %s" % row)
        except KeyError as e:
            print(e)

        return data


    def duplicates_check(self,result,conn,client,partial_ordered_data,logger = None,logger_errors = None,logger_candidates = None):
        # results=conn.aggregate([
        #     # {'$match':{'id':{'$exists':False}}},
        #     { '$group': {
        #         '_id': { 'firstField': "$title", 'secondField': "$title" },
        #         'uniqueIds': { '$addToSet': "$_id" },
        #         'catalogues': {'$push': '$catalogue_url'},
        #         'count': { '$sum': 1 }
        #         }},
        #     { '$match': {
        #         'count': { '$lt': 2 }
        #         }}
        #     ])
        # if results['ok']==1:
        #     no_distinct=len(results['result'])
        # else:
        #     print('conn error')
        #     return
        #
        # # check dublicates with title
        # results=conn.aggregate([
        #     # {'$match':{'id':{'$exists':False}}},
        #     { '$group': {
        #         '_id': { 'firstField': "$title", 'secondField': "$title" },
        #         'uniqueIds': { '$addToSet': "$_id" },
        #         'catalogues': {'$push': '$catalogue_url'},
        #         'count': { '$sum': 1 }
        #         }},
        #     { '$match': {
        #         'count': { '$gt': 1 }
        #         }}
        #     ])

        dups_search=searcher.Searcher()
        # results.rewind()
        # checked_ids=Set()
        is_duplicate=False
        # duplicate_ids=[]
        dups_found=dups_search.find_near_dups_by_minhash(result,conn,0.03)
        for k_match,v in dups_found.iteritems():
            dups_ids=[]
            for dup in v:
                dups_ids.append(dup['_id'])
            dups_candidates=client.find({'_id':{'$in':dups_ids}})
            # print(dups_ids)
            copied_id={}
            # original_id={}
            for dup in dups_candidates:
                status=self.criteria_for_duplication(result,dup,logger_errors)
                candidate_found=False
                # print(k_match,status)
                if status == self.NEWEST:
                    # duplicate_ids.append(dup['_id'])
                    candidate_found=True
                    if str(result['_id']) not in copied_id:
                        # for key in original_id:
                        #     del original_id[key]
                        original_id={str(result['_id']):result['catalogue_url']}
                        # copied_id=dup['_id']
                    copied_id[str(dup['_id'])]=dup['catalogue_url']
                    # else:
                    #     copied_id.append({dup['_id']:dup['catalogue_url']})

                    if k_match == 'Exact':
                        print('Duplicate ObjectId - catalogue_url found: \n' \
                                '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                (result['_id'],result['catalogue_url'],dup['_id'],dup['catalogue_url']))
                    else:
                        print('Candidate ObjectId - catalogue_url found: \n' \
                                '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                (result['_id'],result['catalogue_url'],dup['_id'],dup['catalogue_url']))
                elif status == self.OLDEST:
                    # duplicate_ids.append(result['_id'])
                    candidate_found=True
                    if str(dup['_id']) not in copied_id:
                        # for key in original_id:
                        #     del original_id[key]
                        original_id={str(dup['_id']):dup['catalogue_url']}
                        # copied_id=result['_id']
                    copied_id[str(result['_id'])]=result['catalogue_url']
                    # else:
                    #     copied_id.append({result['_id']:result['catalogue_url']})

                    if k_match == 'Exact':
                        is_duplicate = True
                        print('Candidate ObjectId - catalogue_url found:\n' \
                                '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                            (dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']))
                    else:
                        print('Candidate ObjectId - catalogue_url found:\n' \
                                '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                            (dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']))
                elif status == self.UNKNOWN:
                    result_lvl=-1
                    dup_lvl=-1
                    level=0
                    # partial_ordering = self.toposort2(data)
                    for ordered in self.toposort2(partial_ordered_data):
                        if result['catalogue_url'] in ordered:
                            result_lvl=level
                        if dup['catalogue_url'] in ordered:
                            dup_lvl=level
                        if dup_lvl!=-1 and result_lvl!=-1:
                            break;
                        level+=1

                    if dup_lvl!=-1 and result_lvl!=-1:
                        if result_lvl < dup_lvl:
                            # duplicate_ids.append(dup['_id'])
                            candidate_found=True
                            if str(result['_id']) not in copied_id:
                                # for key in original_id:
                                #     del original_id[key]
                                original_id={str(result['_id']):result['catalogue_url']}
                                # copied_id=dup['_id']
                            copied_id[str(dup['_id'])]=dup['catalogue_url']
                            # else:
                            #     copied_id.append({dup['_id']:dup['catalogue_url']})

                            if k_match == 'Exact':
                                # duplicate_ids.append(dup['_id'])
                                print('Duplicate ObjectId - catalogue_url found:\n' \
                                        '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                        (result['_id'],result['catalogue_url'],dup['_id'],dup['catalogue_url']))
                            else:
                                print('Candidate ObjectId - catalogue_url found:\n' \
                                        '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                        (result['_id'],result['catalogue_url'],dup['_id'],dup['catalogue_url']))
                        elif result_lvl > dup_lvl:
                            # duplicate_ids.append(result['_id'])
                            candidate_found=True
                            if str(dup['_id']) not in copied_id:
                                # for key in original_id:
                                #     del original_id[key]
                                original_id={str(dup['_id']):dup['catalogue_url']}
                                # copied_id=result['_id']
                            copied_id[str(result['_id'])]=result['catalogue_url']
                            # else:
                            #     copied_id.append({result['_id']:result['catalogue_url']})

                            if k_match == 'Exact':
                                is_duplicate = True
                                print('Duplicate ObjectId - catalogue_url found:\n' \
                                        '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                        (dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']))
                            else:
                                print('Candidate ObjectId - catalogue_url found:\n' \
                                        '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                        (dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']))
                    # if dup_lvl==-1 or result_lvl==-1:
                    else:
                        cprint('Warning: %s candidate found but needs order rule to be applied in between catalogues relation (_id - catalogue_url):\n' \
                                '\tObjectId("%s") - %s and ObjectId("%s") - %s' %
                                (k_match,dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']),
                                'red',attrs=['bold'])
                        logger.warning('Candidate found but needs order rule to be applied in ' \
                                'between catalogues relation (_id - catalogue_url): ' \
                                'ObjectId("%s") - %s and ObjectId("%s") - %s' %
                                (dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']))

                # if is_duplicate == False and len(duplicate_ids) > 0:
            if candidate_found:
                if k_match=='Exact':
                    g_id = original_id.iterkeys().next()
                    res2=client.update({'_id':ObjectId(g_id)},
                            # {'$set':{'is_duplicate':is_duplicate},
                            {'$set':{'is_duplicate':False,
                                'duplicates':copied_id,
                                },
                                # '$addToSet':{'duplicates':{'$each':copied_id}}
                                # '$addToSet':{'duplicates':copied_id}
                                },True
                            )
                    # for _id in duplicate_ids:
                    for _id in copied_id:
                        res1=client.update({'_id':ObjectId(_id)},
                                {'$set':{'is_duplicate':True,
                                    'duplicates':original_id,
                                    },
                                    # '$addToSet':{'duplicates':original_id}
                                    },upsert=True
                                )
                    # print(res1,copied_id)
                    cprint ('Duplicate rules applied for ObjectId("%s").' % g_id,'green')
                else:
                    logger_candidates.warning('Candidate pair found that needs to be checked ' \
                            '(_id - catalogue_url): ' \
                            'ObjectId("%s") - %s and ObjectId("%s") - %s' %
                            (dup['_id'],dup['catalogue_url'],result['_id'],result['catalogue_url']))

    #             # for i in range(0,len(match_list_docs)):
    #         for i in range(0,len(match_list_docs[0])):
    #             for j in match_list_docs[i+1:]:
    #                 if 'resources' in j.keys() and 'resources' in match_list_docs[i].keys():
    #                     if len(match_list_docs[i]['resources'])!=len(j['resources']):
    #                         if match_list_docs[i]['catalogue_url']!=j['catalogue_url']:
    #                             logger.info('ObjectId - catalogue_url: %s - %s, has diff number of resources with ObjectId - catalogue_url: %s - %s',
    #                                     match_list_docs[i]['_id'],match_list_docs[i]['catalogue_url'],
    #                                         j['_id'],j['catalogue_url']
        #                                     )
        #                             no_outdated=no_outdated+1
        #                         else:
        #                             logger_errors.error('ObjectId: %s, is probably the same with ObjectId: %s, but diff size of resources',
        #                                     match_list_docs[i]['_id'],j['_id']
        #                                     )
        #                             no_same=no_same+1
        #                     else:
        #                         bIdentical=False
        #                         for k in match_list_docs[i]['resources']:
        #                             hash_found=False
        #                             candidate_hash=hashlib.md5(k['url'].encode('utf-8')).hexdigest()
        #                             for l in j['resources']:
        #                                 if candidate_hash==hashlib.md5(l['url'].encode('utf-8')).hexdigest():
        #                                     bIdentical=True
        #                                     hash_found=True
        #                                     break
        #
        #                             if not hash_found:
        #                                 if match_list_docs[i]['catalogue_url']!=j['catalogue_url']:
        #                                     logger.info('ObjectId - catalogue_url: %s - %s, has diff checksum of resource with ObjectId - catalogue_url: %s - %s',
        #                                         match_list_docs[i]['_id'],match_list_docs[i]['catalogue_url'],
        #                                             j['_id'],j['catalogue_url']
        #                                         )
        #                                     no_outdated=no_outdated+1
        #                                     break
        #                                 # else:
        #                                 #     logger_errors.error('ObjectId: %s, is probably the same with ObjectId: %s, but diff md5 checksum of resources',
        #                                 #             match_list_docs[i]['_id'],j['_id']
        #                                 #             )
        #                                     # no_same=no_same+1
        #                                 # break
        #                             # else:
        #                             #     if match_list_docs[i]['catalogue_url']!=j['catalogue_url']:
        #                             #         logger.info('ObjectId - catalogue_url: %s - %s, at least one resource has diff checksum with ObjectId - catalogue_url: %s - %s',
        #                             #             match_list_docs[i]['_id'],match_list_docs[i]['catalogue_url'],
        #                             #                 j['_id'],j['catalogue_url']
        #                             #             )
        #                             #         no_outdated=no_outdated+1
        #                             #     else:
        #                             #         logger_errors.error('ObjectId: %s, is probably the same with ObjectId: %s, but diff md5 checksum of resources',
        #                             #                 match_list_docs[i]['_id'],j['_id']
        #                             #                 )
        #                                     # no_same=no_same+1
        #                                 # break
        #                         if bIdentical:
        #                             if match_list_docs[i]['catalogue_url']!=j['catalogue_url']:
        #                                 logger.warning('ObjectId - catalogue_url: %s - %s, is dublicate with ObjectId - catalogue_url: %s - %s',
        #                                     match_list_docs[i]['_id'],match_list_docs[i]['catalogue_url'],
        #                                         j['_id'],j['catalogue_url']
        #                                     )
        #                                 no_dublicates=no_dublicates+1
        #                             else:
        #                                 logger_errors.error('ObjectId: %s, is probably the same with ObjectId: %s',
        #                                         match_list_docs[i]['_id'],j['_id']
        #                                         )
        #                                 no_same=no_same+1
        #
        #                 elif 'resources' not in j.keys() and 'resources' not in match_list_docs[i].keys():
        #                     if match_list_docs[i]['catalogue_url']!=j['catalogue_url']:
        #                         logger.warning('ObjectId - catalogue_url: %s - %s, is dublicate with ObjectId - catalogue_url: %s - %s',
        #                                 match_list_docs[i]['_id'],match_list_docs[i]['catalogue_url'],
        #                                     j['_id'],j['catalogue_url']
        #                                 )
        #                         no_dublicates=no_dublicates+1
        #                     else:
        #                         logger_errors.error('ObjectId: %s, is probably the same with ObjectId: %s',
        #                                 match_list_docs[i]['_id'],j['_id']
        #                                 )
        #                         no_same=no_same+1
        #
        #                 else:
        #                     # logger.info('ObjectId: %s, and ObjectId: %s has no resources, so further checkin is needed',
        #                     #         match_list_docs[i]['_id'],j['_id']
        #                     #         )
        #                     if match_list_docs[i]['catalogue_url']!=j['catalogue_url']:
        #                         logger.info('ObjectId - catalogue_url: %s - %s, has diff number of resources with ObjectId - catalogue_url: %s - %s',
        #                                 match_list_docs[i]['_id'],match_list_docs[i]['catalogue_url'],
        #                                     j['_id'],j['catalogue_url']
        #                                 )
        #                         no_outdated=no_outdated+1
        #                     else:
        #                         logger_errors.error('ObjectId: %s, is probably reharvest, but diff number of resources, with ObjectId: %s',
        #                                 match_list_docs[i]['_id'],j['_id']
        #                                 )
        #                         no_same=no_same+1
        #
        #
        #         counter=counter+1
        #         if (counter%1000)==0:
        #             print('%i candidate dublicates checked so far...' % counter)
        #
        #     else:
        #         conn.update({'_id':ObjectId(result['_id'])},
        #                 {'$set': {'is_duplicate':False}},
        #                 True)
        #
        #     if (counter%1000)>0:
        #         print('%i last candidate dublicates checked' % counter)
        #
        #     no_doubles=no_datasets-(no_distinct+len(results['result']))
        #     all_dublicates=no_datasets-no_distinct
        #     logger.info('\n\n=========================================================================\n\n')
        #     logger.info('ckan dublicates found: %i in %i datasets, percentage: %f%%\n', no_doubles, no_datasets,(no_doubles*100.00)/no_datasets)
        #     logger.info('Dublicate datasets: %i/%f%%',no_dublicates,(no_dublicates*100.00)/all_dublicates)
        #     logger.info('Outdated datasets: %i/%f%%',no_outdated,(no_outdated*100.00)/all_dublicates)
        #     logger.info('Multiple inserts of same datasets: %i/%f%%',no_same,(no_same*100.00)/all_dublicates)
        #
        # else:
        #     logger.info('no ckan dublicates found')
        #
        #check dublicates with name - html

    def criteria_for_duplication(self,ref_data,dup,logger,exact_match=True):
        score = {'similarity':0,'selected':0}
        try:
            dup_dt=dt(1900,1,1)
            result_dt=dt(1901,1,1)
            try:
                dup_dt=dup['extras']['date_updated']
            except KeyError:
                dup_dt=dup['extras']['date_released']

            try:
                result_dt=ref_data['extras']['date_updated']
            except KeyError:
                result_dt=ref_data['extras']['date_released']

            if dup_dt < result_dt:
                score['similarity'] = 0.1
                score['selected'] = 1
            elif dup_dt > result_dt:
                score['similarity'] = 0.1
                score['selected'] = -1
            elif dup_dt == result_dt:
                score['similarity'] = 0.4
        except TypeError:
            score['similarity']=0.2
            logging.getLogger('log_err').error('TypeError: ObjectId("%s"): at least one arg must be a datetime.date' %
                (dup['_id']))
        except KeyError:
            score['similarity']=0.2

        dup_list_resources=[]
        result_list_resources=[]
        dup_list_res=[]
        result_list_res=[]
        dup_list_filenames=[]
        result_list_filenames=[]
        if 'resources' in dup.keys() or 'resources' in ref_data.keys():
            if 'resources' in dup.keys():
                for res in dup['resources']:
                    try:
                        if 'url' in res and res['url'] not in ['',None]:
                            dup_list_resources.append(res['url'].lower())
                        if 'size' in res and res['size'] not in ['',None]:
                            dup_list_res.append({'size':res['size'],'format':res['format']})
                        if 'file_name' in res and res['file_name'] not in ['',None]:
                            dup_list_filenames.append(res['file_name'].lower())
                    except KeyError,e:
                        print('%s for dup: ObjectId("%s")' % (e,dup['_id']))

            if 'resources' in ref_data.keys():
                for res in ref_data['resources']:
                    try:
                        if 'url' in res and res['url'] not in ['',None]:
                            result_list_resources.append(res['url'].lower())
                        if 'size' in res and res['size'] not in ['',None]:
                            result_list_res.append({'size':res['size'],'format':res['format']})
                        if 'file_name' in res and res['file_name'] not in ['',None]:
                            result_list_filenames.append(res['file_name'].lower())
                    except KeyError,e:
                        print('%s for ref_data: ObjectId("%s")' % (e,ref_data['_id']))

        if (len(dup_list_res)>0 and len(result_list_res)>0) or \
                (len(dup_list_resources)>0 and len(result_list_resources)>0):
            score_url = 0
            score_size = 0
            score_filename = 0

            try:
# calc url score
                score_url = 2*len(list(set(dup_list_resources) & set(result_list_resources)))/(len(dup_list_resources)+len(result_list_resources))*0.6
            except ZeroDivisionError:
                pass

            try:
# calc filename score
                score_filename = 2*len(list(set(dup_list_filenames) & set(result_list_filenames)))/(len(dup_list_filenames)+len(result_list_filenames))*0.6
            except ZeroDivisionError:
                pass

            try:
# calc size score
                sim_found = 0
                for elem in result_list_res:
                    if elem in dup_list_res:
                        sim_found += 1

                score_size = 2*sim_found/(len(dup_list_res)+len(result_list_res))*0.6
            except ZeroDivisionError:
                pass


            score['similarity'] = score['similarity'] + max(score_url,score_size,score_filename)
        else:
            score['similarity'] = score['similarity'] + 0.3

        # if score['similarity'] >=0.6:
        #     print(score,ref_data['_id'],ref_data['catalogue_url'],dup['_id'],dup['catalogue_url'])
        if score['similarity'] >= self.THRES:
            if score['selected'] > 0:
                return self.NEWEST
            elif score['selected'] < 0:
                return self.OLDEST
            else:
                return self.UNKNOWN
        else:
            return self.UNDEFINED


    def prepare_data_for_indexing(self,data):
        if 'title' not in data.keys() or data['title'] == None:
            print('title does not exist in doc or is null for _id: ObjectId(\'%s\')' % data['_id'])
        else:
            data_for_idx={}
            data_for_idx['_id']=data['_id']
            data_for_idx['catalogue_url']=data['catalogue_url']
            if 'notes' not in data.keys() or data['notes']==None:
                data_for_idx['content']=unicode(data['title']).lower()
            else:
                data_for_idx['content']=unicode(data['title']).lower() + ' ' + unicode(data['notes']).lower()

            data_for_idx['content']=' '.join(filter(None, (word.strip(punctuation) \
                    for word in data_for_idx['content'].split())))
            self.content.append(data_for_idx)

    def index_data(self,conn,db):
        idx=indexer.Indexer(db=db)
        idx.build_index(self.content,conn)


    def delete_dups_in_ckan(self,conn,match_field='$id'):
        results=conn.aggregate([
            {'$match':{'platform':'ckan'
                # ,'id':'035c3b2e-3704-4afc-ad2d-6eeb80ec411e'
                }},
            { '$group': {
                '_id': { 'firstField': match_field, 'secondField': match_field },
                'uniqueIds': { '$addToSet': "$_id" },
                'catalogues': {'$addToSet': '$catalogue_url'},
                'count': { '$sum': 1 }
                }},
            { '$match': {
                'count': { '$gt': 1 },
                'catalogues': { '$size': 1}
                }}
            ])

        if results['ok']:
            counter = 0
            rem_ids = []

            for res in results['result']:
                ids_list = []
                for _id in res['uniqueIds']:
                    ids_list.append(_id)
                dupl_docs = conn.find({'_id': { '$in':ids_list }})
                metadata_modified = dt(1900,1,1)
                prev_ref_id = None
                for doc in dupl_docs:
                    if doc['metadata_modified'] > metadata_modified:
                        metadata_modified = doc['metadata_modified']
                        if prev_ref_id:
                            rem_ids.append(prev_ref_id)
                            counter += 1

                        prev_ref_id = doc['_id']

                    else:
                        rem_ids.append(doc['_id'])
                        counter += 1

            if rem_ids:
                # print(rem_ids)
                conn.remove({'_id': { '$in': rem_ids }})

            print('Deleted %d' % counter)
        else:
            print(results)


    def delete_dups_in_general(self,conn,match_field='$id'):
        results=conn.aggregate([
            { '$match':{
                # ,'id':'035c3b2e-3704-4afc-ad2d-6eeb80ec411e'
                }},
            { '$group': {
                '_id': { 'firstField': match_field, 'secondField': match_field },
                'uniqueIds': { '$addToSet': "$_id" },
                'catalogues': {'$addToSet': '$catalogue_url'},
                'count': { '$sum': 1 }
                }},
            { '$match': {
                'count': { '$gt': 1 },
                'catalogues': { '$size': 1}
                }}
            ])

        if results['ok']:
            pass
            # for res in results['result']:
            #     ids_list = []
            #     for _id in res['uniqueIds']:
            #         ids_list.append(_id)
            #     dupl_docs = conn.find({'id': { '$in':ids_list }})
            #     metadata_modified = dt(1900,1,1)
            #     rem_ids = []
            #     for doc in dupl_docs:
            #         if doc['metadata_modified'] > metadata_modified:
            #             metadata_modified = doc['metadata_modified']
            #
            #         else:
            #             rem_ids.append(doc['_id'])
            #
            #     if rem_ids:
            #         print(rem_ids)
                    # conn.remove({'_id': { 'in': rem_ids }})
        else:
            print(results)


    def delete_dups(self,conn):
        self.delete_dups_in_ckan(conn)
        self.delete_dups_in_ckan(conn,'$ckan_url')
        self.delete_dups_in_general(conn)

    def updated_dedup_func(self,conn,partial_ordering):
        undefined_partial_maps = []
        # self.identify_duplicates_in_ckan(conn,partial_ordering)
        self.identify_duplicates_in_general(conn,partial_ordering,undefined_partial_maps)

        unknown_hierarchies = {}
        for cat_pair in undefined_partial_maps:
            new_tupl = (min(cat_pair),max(cat_pair))
            if new_tupl not in unknown_hierarchies:
                unknown_hierarchies[new_tupl] = 0
            unknown_hierarchies[new_tupl] = unknown_hierarchies[new_tupl] + 1

        if unknown_hierarchies:
            print('\n\n')
            # pp = pprint.PrettyPrinter(indent=4)
            # pp.pprint(unknown_hierarchies)
            d_view = [ (v,k) for k,v in unknown_hierarchies.iteritems() ]
            d_view.sort(reverse=True) # natively sort tuples by first element
            for v,k in d_view:
                print "%s: %d" % (k,v)
            print('\n')


    def identify_duplicates_in_ckan(self,conn,partial_ordering):
        results=conn.aggregate([
            {'$match':{'platform':'ckan'
                # ,'id':'035c3b2e-3704-4afc-ad2d-6eeb80ec411e'
                }},
            { '$group': {
                '_id': { 'firstField': '$id', 'secondField': '$id' },
                'uniqueIds': { '$addToSet': "$_id" },
                'catalogues': {'$addToSet': '$catalogue_url'},
                'count': { '$sum': 1 }
                }},
            { '$match': {
                'count': { '$gt': 1 }
                }}
            ])

        if results['ok']:
            for doc in results['result']:
                # exclude dupls in the same catalogue
                if len(doc['catalogues'])>1:
                    docs_to_check = []
                    for i in doc['uniqueIds']:
                        docs_to_check.append(conn.find_one(ObjectId(i)))

                    self.apply_duplicate_rules(conn,partial_ordering,docs_to_check)
        else:
            print(results)


    def identify_duplicates_in_general(self,conn,partial_ordering,undefinied_partial_mappings=[]):
        results=conn.aggregate([
            { '$match':{
                'is_duplicate': { '$ne': True }
                # , '_id':{'$in':[ObjectId("55f1ffa69daf09ab8fc820ea"),ObjectId("55a682d69daf096b24b0028c")]}
                # ,'title':u'Use of public libraries, Visits to museums and galleries, and Engagement in the arts'
                }},
            { '$group': {
                '_id': { 'firstField': '$title', 'secondField': '$title' },
                'uniqueIds': { '$addToSet': "$_id" },
                'catalogues': {'$addToSet': '$catalogue_url'},
                'count': { '$sum': 1 }
                }},
            { '$match': {
                'count': { '$gt': 1 }
                }}
            ])

        if results['ok']:
            for doc in results['result']:
                # exclude dupls in the same catalogue
                if len(doc['catalogues'])>1:
                    docs_to_check = []
                    for i in doc['uniqueIds']:
                        docs_to_check.append(conn.find_one(ObjectId(i)))

                    self.apply_duplicate_rules(conn,partial_ordering,docs_to_check,undefinied_partial_mappings,False)
        else:
            print(results)


    def apply_duplicate_rules(self,conn,partial_ordered_data,docs=[],not_found_mapping=[],check_further_criteria=False):
        candidate_found = False
        copied_id={}
        original_id={}

        results = set()
        results.add(0)
        for _index in range(1,len(docs)):
            resIds_to_remove = set()
            resIds_to_add = set()
            for result in results:
                if check_further_criteria:
                    # print(result['_id'],docs[_index]['_id'])
                    status=self.criteria_for_duplication(docs[result],docs[_index],None)
                else:
                    status = self.UNKNOWN

                if status == self.NEWEST:
                    candidate_found=True
                    # if result['_id'] not in copied_id:
                        # original_id={str(result['_id']):result['catalogue_url']}
                    original_id[str(docs[result]['_id'])] = docs[result]['catalogue_url']
                    if str(docs[result]['_id']) not in copied_id:
                        copied_id[str(docs[result]['_id'])] = {}
                    copied_id[str(docs[result]['_id'])][str(docs[_index]['_id'])] = docs[_index]['catalogue_url']

                    # copied_id[str(docs[_index]['_id'])]=docs[_index]['catalogue_url']

                    print('Duplicate ObjectId - catalogue_url found: \n' \
                            '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                            (docs[result]['_id'],docs[result]['catalogue_url'],docs[_index]['_id'],docs[_index]['catalogue_url']))

                elif status == self.OLDEST:
                    candidate_found=True
                    # if result['_id']) in copied_id:
                        # original_id={str(docs[_index]['_id']):docs[_index]['catalogue_url']}
                    original_id[str(docs[_index]['_id'])] = docs[_index]['catalogue_url']
                    if str(docs[_index]['_id']) not in copied_id:
                        copied_id[str(docs[_index]['_id'])] = {}
                    copied_id[str(docs[_index]['_id'])][str(docs[result]['_id'])] = docs[result]['catalogue_url']
                    if str(docs[result]['_id']) in copied_id:
                        for key,val in copied_id[str(docs[result]['_id'])].iteritems():
                            # try:
                            #     copied_id[str(docs[_index]['_id'])].index(_id)
                            # except ValueError:
                            copied_id[str(docs[_index]['_id'])][key] = val
                        del copied_id[str(docs[result]['_id'])]
                        del original_id[str(docs[result]['_id'])]

                    # copied_id[str(result['_id'])]=result['catalogue_url']

                    print('Duplicate ObjectId - catalogue_url found:\n' \
                            '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                        (docs[_index]['_id'],docs[_index]['catalogue_url'],docs[result]['_id'],docs[result]['catalogue_url']))

                    # result = docs[_index]
                    resIds_to_remove.add(result)
                    resIds_to_add.add(_index)

                elif status == self.UNKNOWN:
                    result_lvl=-1
                    dup_lvl=-1
                    level=0
                    # partial_ordering = self.toposort2(data)
                    for ordered in self.toposort2(partial_ordered_data):
                        if docs[result]['catalogue_url'] in ordered:
                            result_lvl=level
                        if docs[_index]['catalogue_url'] in ordered:
                            dup_lvl=level
                        if dup_lvl!=-1 and result_lvl!=-1:
                            break;
                        level+=1

                    if dup_lvl != result_lvl and result_lvl!=-1:
                        if result_lvl < dup_lvl:
                            candidate_found=True
                            # if str(result['_id']) not in copied_id:
                                # original_id={str(result['_id']):result['catalogue_url']}
                            original_id[str(docs[result]['_id'])] = docs[result]['catalogue_url']
                            if str(docs[result]['_id']) not in copied_id:
                                copied_id[str(docs[result]['_id'])] = {}
                            copied_id[str(docs[result]['_id'])][str(docs[_index]['_id'])] = docs[_index]['catalogue_url']

                            # copied_id[str(docs[_index]['_id'])]=docs[_index]['catalogue_url']

                            print('Duplicate ObjectId - catalogue_url found:\n' \
                                    '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                    (docs[result]['_id'],docs[result]['catalogue_url'],docs[_index]['_id'],docs[_index]['catalogue_url']))

                        elif result_lvl > dup_lvl:
                            candidate_found=True
                            # if str(docs[_index]['_id']) not in copied_id:
                                # original_id={str(docs[_index]['_id']):docs[_index]['catalogue_url']}
                            original_id[str(docs[_index]['_id'])] = docs[_index]['catalogue_url']
                            if str(docs[_index]['_id']) not in copied_id:
                                copied_id[str(docs[_index]['_id'])] = {}
                            copied_id[str(docs[_index]['_id'])][str(docs[result]['_id'])] = docs[result]['catalogue_url']
                            if str(docs[result]['_id']) in copied_id:
                                for key,val in copied_id[str(docs[result]['_id'])].iteritems():
                                    # try:
                                    #     copied_id[str(docs[_index]['_id'])].index(_id)
                                    # except ValueError:
                                    copied_id[str(docs[_index]['_id'])][key] = val
                                del copied_id[str(docs[result]['_id'])]
                                del original_id[str(docs[result]['_id'])]

                            # copied_id[str(result['_id'])]=result['catalogue_url']

                            print('Duplicate ObjectId - catalogue_url found:\n' \
                                    '\tObjectId("%s") - %s -> ObjectId("%s") - %s' %
                                    (docs[_index]['_id'],docs[_index]['catalogue_url'],docs[result]['_id'],docs[result]['catalogue_url']))

                            # result = docs[_index]
                            resIds_to_remove.add(result)
                            resIds_to_add.add(_index)

                    else:
                        if docs[_index]['catalogue_url'] != docs[result]['catalogue_url']:
                            not_found_mapping.append((docs[_index]['catalogue_url'],docs[result]['catalogue_url']))

                            cprint('Warning: Candidate found but needs order rule to be applied in between catalogues relation (_id - catalogue_url):\n' \
                                    '\tObjectId("%s") - %s and ObjectId("%s") - %s' %
                                    (docs[_index]['_id'],docs[_index]['catalogue_url'],docs[result]['_id'],docs[result]['catalogue_url']),
                                    'red',attrs=['bold'])
                            logging.getLogger('log_order').warning('Candidate found but needs order rule to be applied in ' \
                                    'between catalogues relation (_id - catalogue_url): ' \
                                    'ObjectId("%s") - %s and ObjectId("%s") - %s' %
                                    (docs[_index]['_id'],docs[_index]['catalogue_url'],docs[result]['_id'],docs[result]['catalogue_url']))
                elif status == self.UNDEFINED:
                    resIds_to_add.add(_index)

            for _res in resIds_to_add:
                results.add(_res)
            for _res in resIds_to_remove:
                # results[:] = [d for d in results if d.get('_id') != _res]
                results.remove(_res)

        if candidate_found:
            for key,val in original_id.iteritems():
                # print('\n========================')
                # print(g_id)
                # print(copied_id[g_id])
                # print('========================\n')

            # g_id = original_id.iterkeys().next()
                res2=conn.update({'_id':ObjectId(key)},
                        {'$set':{'is_duplicate':False,
                            'duplicates':copied_id[key],
                            },
                            # '$addToSet':{'duplicates':{'$each':copied_id}}
                            # '$addToSet':{'duplicates':copied_id}
                            },True
                        )
                # for _id in duplicate_ids:
                for _id in copied_id[key]:
                    res1=conn.update({'_id':ObjectId(_id)},
                            {'$set':{'is_duplicate':True,
                                'duplicates':{key:val},
                                },
                                # '$addToSet':{'duplicates':original_id}
                                },upsert=True
                            )
                cprint ('Duplicate rules applied for ObjectId("%s").' % key,'green')


class DataDoc:
    def __init__(self,data):
        self.doc=data

    def getID(self):
        return self.doc


class Test():
    def perform_dedup(self,db,coll):
        dups = DeDuplication()
        partial_ordered_data = dups.parse_csv('partialOrder.csv')

        conn = MongoClient('127.0.0.1',27017)
        client = conn[db][coll]

        with Logger('log_order', r'partialOrdering.log'),Logger('log_err', r'dup_errors.log') \
                ,Logger('log_candidate',r'candidates.log'):
            dups.updated_dedup_func(client,partial_ordered_data)
            # dups.delete_dups(client)

        # # results=client.find({'_id':ObjectId("55b372a59daf092e5e600e45")})
        # # results=client.find(limit=0,skip=60000,timeout=False)
        # # results=client.find({'$or':[{'is_duplicate':{'$exists':False}},{'is_duplicate':False}]},timeout=False)
        #
        # # results=client.find({'_id':ObjectId("55f0b15f9daf09ab8fc7e7ad")},timeout=False).sort('_id',ASCENDING)
        # # results=client.find({'_id':ObjectId("55e7b6d89daf096448828db4")},timeout=False).sort('_id',ASCENDING)
        # # results=client.find({'_id':ObjectId("55ec08dc9daf092c92ed4d7c")}).sort('_id',ASCENDING)
        # # results=client.find({},timeout=False).sort('_id',ASCENDING)
        # results=client.find({'is_duplicate':{'$ne':True}},timeout=False).sort('_id',ASCENDING)
        # # results=client.find({'catalogue_url':{'$in':["https://www.govdata.de/ckan/","http://www.daten.rlp.de/"]}},timeout=False).sort('_id',ASCENDING)
        #
        # for result in results:
        #     dups.prepare_data_for_indexing(result)
        # dups.index_data(conn,db)
        #
        # counter = 0
        # with Logger('log1', r'partialOrdering.log'),Logger('log2', r'dup_errors.log') \
        #         ,Logger('log3',r'candidates.log'):
        #     results.rewind()
        #     for result in results:
        #         dups.duplicates_check(result,conn,client,partial_ordered_data,
        #                 logging.getLogger('log1'),logging.getLogger('log2'),logging.getLogger('log3'))
        #
        #         counter=counter+1
        #         if (counter%10000)==0:
        #             print('%i candidates checked so far...' % counter)
        # del results
        #
        print('Deduplication finished successfully')

if __name__=='__main__':
    test = Test()
    test.perform_dedup('odm','odm_harmonised')
