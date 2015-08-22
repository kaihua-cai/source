import numpy as np
import pandas as pd
import sys
import os

from framework import Nexus
nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
bidder_conn = dbm.connection("mysql.bidder")
api_conn = dbm.connection("mysql.api")
common_conn = dbm.connection("mysql.common")
opt_conn = dbm.connection("mysql.optimization")
vert_conn = dbm.connection("vertica.wutang")


def fetch_reporting_learn_percent(site_list, lookback_window, 
                                   width='all', height='all', geo_country='all', hourly=False):
    # this is from reporting, not OQT
    site_str = ','.join(map(str, site_list))
    if hourly:
        time_scale = 'ymdh'
    else:
        time_scale = 'ymd'
    total_imps_q = """
    SELECT
        {time_scale},
        site_id,
        geo_country,
        width,
        height,
        sum(imps)::bigint as imps,    
        sum(CASE
                WHEN (imp_type = 5 and revenue_type in (3, 4) and predict_type_rev in (0,1)) THEN imps
                WHEN (imp_type = 7 and payment_type in (1, 2) and predict_type_rev in (0,1)) THEN imps
                ELSE 0
            END)::bigint as learn_imps
    FROM agg_dw_advertiser_publisher_analytics_adjusted 
    WHERE 
        site_id in ({site_str})
        and ymd >= now() - interval '{lookback_window} days'
        and imp_type not in (7,9,10,11)
        {conditions}
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 6 DESC
    """
    # size
    if ((width == 'all') & (height == 'all')):
        conditions = '' 
    else:
        conditions = """    and width = {width}
        and height = {height}
        """.format(width = width, height = height)
    
    # geo country
    if (geo_country != 'all'):
        conditions += """ 
        and geo_country in ('{geo_list}')
        """.format(geo_list = '\',\''.join(map(str, geo_country)))
    
    total_imps_q = total_imps_q.format(time_scale = time_scale, 
                                       site_str = site_str,
                                       lookback_window = lookback_window,
                                       conditions = conditions)

    return  vert_conn.select_dataframe(total_imps_q).sort(time_scale)


