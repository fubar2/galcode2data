"""
Some experimental "Bring code to data" to avoid privacy issues

Couple of generic functions with a specific mds function

Includes fake data generation to test the plot code
ross lazarus March 12 2022
pip3 install pandas matplotlib numpy sklearn sqlalchemy
need python3-tk if you want to use remote xwindows for images
so that wants tk
ah. postgres listens on localhost which is docker
we live on the host so with bjoern's rna workbench, need to open 5432 or whatever you use on the container
"""
from datetime import datetime
import logging
import os
import time

from matplotlib import pyplot as plt
import pandas as pd
from sklearn.manifold import MDS
from sqlalchemy import create_engine


NCPU = 2  # allowable mds parallel processes, -1 = all !


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


class autoflocker():

    def __init__(self, DSTART="2000-01-01 00:00:01", DFINISH="2022-06-01 00:00:01"):
        # forever may be too long on main!!
        self.cnx = pg_cnx()
        squery = """SELECT user_id, tool_id, COUNT(*) as nruns from job WHERE create_time >= '{}'::timestamp AND create_time < '{}'::timestamp GROUP BY user_id, tool_id  ;"""
        sql = squery.format(DSTART, DFINISH)
        started = time.time()
        jobs = pg_query(self.cnx, sql=sql)
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
            self.plotjobs(rjobs)
            log.info("MDS with %d CPU took %f sec" % (NCPU, time.time() - mstarted))
        else:
            log.warning(
                "1 or less rows in query result - check that the time interval is sane?"
            )

    def plotjobs(self, j):
        jobs = pd.DataFrame(j)
        mds = MDS(random_state=0, n_jobs=NCPU)
        jobs_transform = mds.fit_transform(jobs)
        size = [5]
        plt.scatter(jobs_transform[:, 0], jobs_transform[:, 1], s=size)
        plt.title("Users in tool usage space")
        plt.savefig("user_in_toolspace_mds.pdf")
        log.info("stress=%f" % mds.stress_)


logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger()
log.info("galumds.py starting %s" % datetime.today())
autoflocker(DSTART="2000-01-01 00:00:01", DFINISH="2022-06-01 00:00:01")
# forever - might be too big to cope with on main!
log.info("galumds.py finished %s" % datetime.today())
