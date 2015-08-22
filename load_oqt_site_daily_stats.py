import numpy as np
import pandas as pd
import sys
from datetime import datetime, timedelta
import os
import MySQLdb
from pandas.io import sql
import argparse

from framework import Nexus
nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
bidder_conn = dbm.connection("mysql.bidder")
api_conn = dbm.connection("mysql.api")
common_conn = dbm.connection("mysql.common")
opt_conn = dbm.connection("mysql.optimization")
vert_conn = dbm.connection("vertica.wutang")
local_conn = dbm.connection("mysql.localhost")

#  query to generate the table
'''
create table oqt_site_daily_stats (
             ymd varchar(10),
             seller_member_id int(11),
             site_id int(11) NOT NULL,
             in_oqt int(2),
             imps int(11) DEFAULT 0,
             perf_imps int(11) DEFAULT 0,
             perf_managed_imps int(11) DEFAULT 0,
             perf_xnet_imps int(11) DEFAULT 0,
             learn_imps int(11) DEFAULT 0,
             learn_managed_imps int(11) DEFAULT 0,
             learn_xnet_imps int(11) DEFAULT 0,
             revenue double DEFAULT 0,
             perf_rev double DEFAULT 0,
             perf_managed_rev double DEFAULT 0,
             perf_xnet_rev double DEFAULT 0,
             learn_rev int(11) DEFAULT 0,
             learn_managed_rev int(11) DEFAULT 0,
             learn_xnet_rev int(11) DEFAULT 0,
             PRIMARY KEY (site_id,ymd)
             );
'''

# load daily data into the table
def retrieve_data(ymd):
    total_imps_q="""
        SELECT
            seller_member_id,
            site_id,
            sum(imps)::bigint as imps,
            sum(case when revenue_type in (3,4) then imps else 0 end)::bigint as perf_imps,
            sum(case when revenue_type in (3,4) and imp_type=5 then imps else 0 end)::bigint as perf_managed_imps,
            sum(case when revenue_type in (3,4) and imp_type=6 then imps else 0 end)::bigint as perf_xnet_imps,
            sum( case when imp_type in (5,6) and predict_type_rev in (0,1) and revenue_type in (3, 4) then imps
                      ELSE 0
                 END ) as learn_imps,
            sum( case when imp_type =5 and predict_type_rev in (0,1) and revenue_type in (3, 4) then imps
                      ELSE 0
                 END ) as learn_managed_imps,
            sum( case when imp_type =6 and predict_type_rev in (0,1) and revenue_type in (3, 4) then imps
                      ELSE 0
                 END ) as learn_xnet_imps,
            sum(booked_revenue_dollars + reseller_revenue) as revenue,
            sum(case when revenue_type in (3,4) then booked_revenue_dollars + reseller_revenue
              else 0 end) as perf_rev,
            sum(case when revenue_type in (3,4) and imp_type=5 then booked_revenue_dollars + reseller_revenue
              else 0 end) as perf_managed_rev,
            sum(case when revenue_type in (3,4) and imp_type=6 then booked_revenue_dollars + reseller_revenue
              else 0 end) as perf_xnet_rev,
            sum(case when imp_type in (5,6) and predict_type_rev in (0,1) and revenue_type in (3, 4) then booked_revenue_dollars
                      ELSE 0
                 END ) as learn_rev,
            sum(case when imp_type =5 and predict_type_rev in (0,1) and revenue_type in (3, 4) then booked_revenue_dollars
                      ELSE 0
                 END ) as learn_managed_rev,
            sum(case when imp_type =6 and predict_type_rev in (0,1) and revenue_type in (3, 4) then booked_revenue_dollars
                      ELSE 0
                 END ) as learn_xnet_rev
        FROM agg_dw_advertiser_publisher_analytics_adjusted
        WHERE
            ymd = '{ymd}'
            and imp_type not in (7,9,10,11)
        GROUP BY seller_member_id, site_id
        having sum(case when revenue_type in (3,4) then imps else 0 end) > 100;
        """.format(ymd = ymd )
    
    total_site_df =  vert_conn.select_dataframe(total_imps_q)
    
    is_oqt_q = """
                select site_id 
                from  site_marketplace_map
                where deleted = 0
                and performance = 1
            """
    
    oqt_site_list = list( common_conn.select_dataframe(is_oqt_q).site_id )
    total_site_df['in_oqt'] = [ int(x in oqt_site_list) for x in total_site_df.site_id]
    total_site_df['ymd'] = ymd
    
    total_site_df = total_site_df[['ymd', 'seller_member_id','site_id', 'in_oqt', 'imps', u'perf_imps',
                    'perf_managed_imps', u'perf_xnet_imps',
                    'learn_imps', u'learn_managed_imps', u'learn_xnet_imps', u'revenue', 
                    'perf_rev', u'perf_managed_rev', u'perf_xnet_rev', u'learn_rev',
                    'learn_managed_rev', u'learn_xnet_rev']]
    return total_site_df


# write data to my localhost mysql
'grant all on oqt.* to kcai@*'

def write_data(total_site_df):
    con = MySQLdb.connect(host='localhost',user='kcai',passwd='kcai',db='oqt')
    #data=data.where(pd.notnull(data), None)
    sql.write_frame(total_site_df, con=con, name='oqt_site_daily_stats', if_exists='append', flavor='mysql')

def get_args():
    """
    Returns the command line arguments which allows you to override the start date and end date
    """
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--ymd', type=str, help='the date to load data for')
    return parser.parse_args()

if __name__ == "__main__":
    _args = get_args()

    if not _args.ymd is None :
        ymd = _args.ymd
    else:
        ymd = str((datetime.today() - timedelta(days=1)).date())
    
    print 'today is', datetime.today()
    print 'the date to run the model:', ymd    
    total_site_df = retrieve_data(ymd)
    write_data(total_site_df)


