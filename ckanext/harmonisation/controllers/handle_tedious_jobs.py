from pymongo import MongoClient,ASCENDING
import configparser
import logging
import HarmonisationEngine
from datetime import datetime as dt
# from multiprocessing.pool import Pool
import multiprocessing
from functools import partial
from contextlib import closing
import itertools
from itertools import repeat


class ExecuteJobs():
    CPUS_USED = 2

    def init_logging(self):
        log = logging.getLogger(__name__)
# create a file handler
        handler = logging.FileHandler('tedious_jobs.log')
        handler.setLevel(logging.INFO)

# create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
# add the handlers to the logger
        log.addHandler(handler)
        assert not log.disabled

        return log

    def init(self):
##read from development.ini file all the required parameters
        config = configparser.ConfigParser()
        config.read('/var/local/ckan/default/pyenv/src/ckan/development.ini')
        mongoclient=config['ckan:odm_extensions']['mongoclient']
        mongoport=config['ckan:odm_extensions']['mongoport']

        client = MongoClient(str(mongoclient), int(mongoport))
        db = client['odm']

        log = self.init_logging()

        # return client
        return (mongoclient,mongoport)

        # # remaining_jobs = db['harmonise_tedious_jobs'].find_one(sort=[('priority',ASCENDING),('_id',ASCENDING)])
        # remaining_jobs = [l for l in db['harmonise_tedious_jobs'].find(sort=[('priority',ASCENDING),('_id',ASCENDING)])]
        # # .sort([('priority',ASCENDING),('_id',ASCENDING)])
        # # remaining_job = db['harmonise_tedious_jobs'].find_one()
        #
        # # partial_jobs=partial(exec_jobs.running_jobs,remaining_jobs=remaining_jobs)
        # print(remaining_jobs)
        # with Pool(2) as p:
        #     p.map(self.running_jobs,remaining_jobs)
        #
        #


def run(remaining_job,mongoip,mongoport):
    # (ip,port)=ExecuteJobs().init()
    client = MongoClient(str(mongoip), int(mongoport))
    db = client['odm']
    # while (remaining_job!=None):
    catalogue_url = remaining_job['cat_url']

    result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonisation.harmonisation_status':'harmonisation started'}})
    # print(result)
    if result['n']>0:
        result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonised':'started'}})

        for job_to_run in remaining_job['for_running']:
            if job_to_run=='duplicates':
                HarmonisationEngine.deduplicate_metadata(catalogue_url,remaining_job['all_datasets'],remaining_job['id'],conn=client,db='odm')

                result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonisation.deduplication':'finished'}})
                result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonisation.deduplication_date':dt.now()}})

                print('\nDuplicate module successfully run')
            elif job_to_run=='resources':
                HarmonisationEngine.resources_info(catalogue_url,remaining_job['all_datasets'],remaining_job['id'],conn=client,db='odm')

                result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonisation.resources':'finished'}})
                result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonisation.resources_date':dt.now()}})

                print('\nResources module successfully run')

        result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonisation.harmonisation_status':'harmonised'}})
        result = db['jobs'].update({'id':remaining_job['id']},{'$set':{'harmonised':'finished'}})
    else:
        print('job with id: %d could not be found' % remaining_job['id'])

    db['harmonise_tedious_jobs'].remove({'_id':remaining_job['_id']})
    # remaining_job = db['harmonise_tedious_jobs'].find_one(sort=([('priority',ASCENDING),('_id',ASCENDING)]))


def start_process():
    print 'Starting', multiprocessing.current_process().name

def main(mongoip,mongoport):
    client = MongoClient(str(ip), int(port))
    db = client['odm']

    remaining_jobs = [l for l in db['harmonise_tedious_jobs'].find(sort=[('priority',ASCENDING),('_id',ASCENDING)])]
    # print(remaining_jobs)
    # .sort([('priority',ASCENDING),('_id',ASCENDING)])

    partial_jobs=partial(run,mongoip=mongoip,mongoport=mongoport)

    # with Pool(3) as p:
    with closing(multiprocessing.Pool(
        processes=ExecuteJobs().CPUS_USED,
            initializer=start_process,
            )) as p:
        # p.map(partial_jobs,remaining_jobs)
        p.map(partial_jobs,remaining_jobs)
        p.terminate()


if __name__ == '__main__':
    # exec_jobs = ExecuteJobs()
    # db = exec_jobs.init()

    (ip,port)=ExecuteJobs().init()
    main(ip,port)
