"""
Some experimental "Bring code to data" to avoid privacy issues

Couple of generic functions with a specific mds function

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
from scipy.cluster import hierarchy
from scipy.spatial.distance import squareform
from sklearn.manifold import MDS
from sklearn.metrics.pairwise import euclidean_distances
from sqlalchemy import create_engine

NTOOL = 100
NUSERID = 1000
NGROUPS = 5
NCPU = 2  # allowable mds parallel processes, -1 = all !


plt.switch_backend("TkAgg")  # for x over ssh

# override with local values - these are Bjoern's docker defaults.
def pg_cnx(
    POSTGRES_ADDRESS="127.0.0.1",
    POSTGRES_PORT="5432",
    POSTGRES_USERNAME="galaxy",
    POSTGRES_PASSWORD="galaxy",
    POSTGRES_DBNAME="galaxy",
):
    """
    generic get a connection to postgres
    """
    postgres_str = (
        "postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}".format(
            username=POSTGRES_USERNAME,
            password=POSTGRES_PASSWORD,
            ipaddress=POSTGRES_ADDRESS,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DBNAME,
        )
    )
    cnx = create_engine(postgres_str)
    return cnx


def pg_query(cnx, sql=None, CHUNKSIZE=1000):
    """
    generic run a chunked sql query
    """
    log.info("sql=%s" % sql)
    dfs = []
    for chunk in pd.read_sql(sql, con=cnx, chunksize=CHUNKSIZE):
        dfs.append(chunk)
    res = pd.concat(dfs)
    return res


def mds_query(cnx, DSTART="2000-01-01 00:00:01", DFINISH="2022-06-01 00:00:01"):
    # forever may be too long on main!!
    DODENDRO = True
    # WARNING!! this will take a huge amount of time for a big dataset :-(
    # twice as long as the mds for the faked 1000x100 data
    # it's another way to look at the results...

    def fakejobs(NTOOL=100, NUSERID=1000, NGROUPS=5):
        # synthesise NGROUPS obviously different users
        # to test mds plot code without real data
        sjob = []
        for userid in range(NUSERID):
            srow = []
            for toolid in range(NTOOL):
                srow.append(random.randint(0, 1000))
            group = userid % NGROUPS
            for r in range(group, NTOOL, NGROUPS):
                srow[r] = 5 * srow[r]  # fake strong group bias
            # scale so tool frequencies sum to 1
            fsum = float(sum(srow))
            nrow = [x / fsum for x in srow]
            nrow.insert(0, userid)
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
            mds.fit_transform(dist_euclid)
            stress.append(mds.stress_)
        # Plot stress vs. n_components
        plt.plot(range(1, max_range), stress)
        plt.xticks(range(1, max_range, 2))
        plt.xlabel("n_components")
        plt.ylabel("stress")
        plt.show()

    def heatdendro(dm, dat):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 8))
        dm = (dm + dm.T) / 2
        np.fill_diagonal(dm, 0)
        dist_linkage = hierarchy.ward(squareform(dm))
        dendro = hierarchy.dendrogram(dist_linkage, ax=ax1, leaf_rotation=90)
        dendro_idx = np.arange(0, len(dendro["ivl"]))
        ax2.imshow(dm[dendro["leaves"], :][:, dendro["leaves"]])
        ax2.set_xticks(dendro_idx)
        ax2.set_yticks(dendro_idx)
        ax2.set_xticklabels(dendro["ivl"], rotation="vertical")
        ax2.set_yticklabels(dendro["ivl"])
        fig.tight_layout()
        plt.savefig("heatdendro.pdf")

    def plotjobs(j):
        jobs = pd.DataFrame(j)
        mds = MDS(random_state=0, n_jobs=NCPU)
        jobs_transform = mds.fit_transform(jobs)
        size = [5]
        plt.scatter(jobs_transform[:, 0], jobs_transform[:, 1], s=size)
        plt.title("Users in tool usage space")
        plt.savefig("user_in_toolspace_mds.pdf")
        # heatmap(mds.dissimilarity_matrix_,j)
        log.info("stress=%f" % mds.stress_)
        return mds

    squery = """SELECT user_id, tool_id, COUNT(*) as nruns from job WHERE create_time >= '{}'::timestamp AND create_time < '{}'::timestamp GROUP BY user_id, tool_id  ;"""
    sql = squery.format(DSTART, DFINISH)
    started = time.time()
    jobs = pg_query(cnx, sql=sql)
    log.info("Query took %f seconds" % (time.time() - started))
    wjobs = jobs.pivot(index="user_id", columns="tool_id", values="nruns")
    # too hairy to do in SQL !!! Postgres crosstab is horrid - trivial in pandas.
    wjobs = wjobs.fillna(0)
    rjobs = wjobs.div(wjobs.sum(axis=1), axis=0)
    # scale user tool nruns into a fraction of their total work - remove uninteresting total work volume
    mstarted = time.time()
    nr = len(rjobs)
    log.info(
        "Retrieving jobs took %f sec and returned %d rows" % (mstarted - started, nr)
    )
    if nr > 2:
        mds = plotjobs(rjobs)
        log.info("MDS with %d CPU took %f sec" % (NCPU, time.time() - mstarted))
        if DODENDRO:
            hstarted = time.time()
            heatdendro(mds.dissimilarity_matrix_, rjobs)
            log.info("heat/dendro plot took %f sec" % (time.time() - hstarted))
    else:
        log.warning(
            "1 or less rows in query result - check that the time interval is sane?"
        )


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger()
log.info("plotusermds.py starting %s" % datetime.today())
cnx = pg_cnx()
mds_query(cnx, DSTART="2000-01-01 00:00:01", DFINISH="2022-06-01 00:00:01")
# forever - might be too big to cope with on main!
log.info("plotusermds.py finished %s" % datetime.today())
