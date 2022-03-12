"""
Heavens
tool_data_table_config_path: /cvmfs/data.galaxyproject.org/byhand/location/tool_data_table_conf.xml,/cvmfs/data.galaxyproject.org/managed/location/tool_data_table_conf.xml
what a difference.

Some experimental "Bring code to data" to avoid privacy issues
Includes fake data generation to test the plot code
ross lazarus March 12 2022
pip3 install pandas matplotlib seaborn numpy sklearn sqlalchemy psychopg2-binary
need python3-tk if you want to use remote xwindows for images
so that wants tk
ah. postgres listens on localhost which is docker
we live on the host so with bjoern's rna workbench, need to open 5432 or whatever you use on the container
"""
from datetime import datetime
import logging
import os
import random
import time

from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
from sklearn.manifold import MDS
from sklearn.metrics.pairwise import manhattan_distances, euclidean_distances
from sqlalchemy import create_engine

NTOOL = 100
NUSERID = 1000
NGROUPS = 5


plt.switch_backend('TkAgg') # for x over ssh


def pgjobs(CHUNKSIZE = 1000,
    POSTGRES_ADDRESS = '127.0.0.1',
    POSTGRES_PORT = '5432',
    POSTGRES_USERNAME = 'galaxy',
    POSTGRES_PASSWORD = 'galaxy',
    POSTGRES_DBNAME = 'galaxy',
    DSTART = '2020-01-01 00:00:01',
    DFINISH = '2022-06-01 00:00:01'):
    """
    Extract table userid rows by tool invocation counts
    normalise to total jobs to remove effects of user total volume of analyses
    User with 10 bwa jobs out of 10 total jobs is a long distance from a user with 10 bwa jobs out of 100000 jobs
    """
    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME,
    password=POSTGRES_PASSWORD,
    ipaddress=POSTGRES_ADDRESS,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DBNAME))
    # Create the connection
    cnx = create_engine(postgres_str)
    squery = '''SELECT user_id, tool_id, COUNT(*) as nruns from job WHERE create_time>= '{}'::timestamp AND create_time < '{}'::timestamp GROUP BY user_id, tool_id ;'''
    log.info('squery=%s' % squery.format(DSTART, DFINISH))
    dfs = []
    for chunk in pd.read_sql(squery.format(DSTART, DFINISH), con=cnx, chunksize=CHUNKSIZE):
        dfs.append(chunk)
    jobs = pd.concat(dfs)
    wjobs = jobs.pivot(index='user_id', columns='tool_id', values='nruns')
    # Could be done with postgres crosstable pivot - ugly and varies with PG version :( but is easy in pandas
    wjobs = wjobs.fillna(0)
    rjobs = wjobs.div(wjobs.sum(axis=1), axis=0)
    # scale user tool nruns into a fraction of their total work - i.e. scaled to remove effects of uninteresting total work volumes
    return rjobs


def fakejobs():
    # synthesise NGROUPS obviously different users
    # to test mds plot code without real data
    sjob = []
    for userid in range(NUSERID):
        srow = []
        for toolid in range(NTOOL):
            srow.append(random.randint(0,1000))
        group = (userid % NGROUPS)
        for r in range(group, NTOOL, NGROUPS):
            srow[r] = 5*srow[r] # fake strong group bias
        # scale so tool frequencies sum to 1
        fsum = float(sum(srow))
        nrow = [x/fsum for x in srow]
        nrow.insert(0,userid)
        sjob.append(nrow)
        job = pd.DataFrame(sjob)
        job = job.drop(job.columns[[0]], axis=1)
    return job


def stresstest(jobs):
    dist_euclid = euclidean_distances(jobs)
    stress = []
    # Max value for n_components
    max_range = 20
    for dim in range(1, max_range):
        mds = MDS(n_components=dim, random_state=0)
        pts = mds.fit_transform(dist_euclid)
        stress.append(mds.stress_)
    # Plot stress vs. n_components
    plt.plot(range(1, max_range), stress)
    plt.xticks(range(1, max_range, 2))
    plt.xlabel('n_components')
    plt.ylabel('stress')
    plt.show()

def plotjobs(j):
    jobs = pd.DataFrame(j)
    #jobarray = euclidean_distances(jobs) # precompute - returns numpy array
    jobarray = jobs.to_numpy(na_value=0)
    mds = MDS(random_state=0)
    jobs_transform = mds.fit_transform(jobarray)
    size = [5]
    plt.scatter(jobs_transform[:,0], jobs_transform[:,1], s=size)
    plt.title('Users in tool usage space')
    plt.savefig('user_in_toolspace_mds.pdf')

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger()
started = time.time()
log.info('plotusermds.py starting %s' % datetime.today())
# e.g. for a one month test
# jobs = pgjobs(DSTART = '2022-01-01 00:00:01', DFINISH = '2022-01-31 23:59:59')
jobs = pgjobs()
mstarted = time.time()
nr = len(jobs)
log.info('Retrieving jobs took %f sec and returned %d rows' % (mstarted - started,nr))
if nr > 2:
    plotjobs(jobs)
    log.info('MDS took %f sec' % (time.time() - mstarted))
else:
    log.warning('1 or less rows in query result - check that the time interval is sane?')
log.info('plotusermds.py finished %s' % datetime.today())
