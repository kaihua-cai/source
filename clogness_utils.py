import numpy as np
import pandas as pd
import sys
from datetime import datetime, timedelta

import os
import matplotlib.pyplot as plt

from oqt.perf_monitor_utils import get_ozone_from_site, get_cr_size_map, get_site_from_ozone
from pie.validation import geo_group_dict

from framework import Nexus
nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
bidder_conn = dbm.connection("mysql.bidder")

api_conn = dbm.connection("mysql.api")
common_conn = dbm.connection("mysql.common")
opt_conn = dbm.connection("mysql.optimization")
vert_conn = dbm.connection("vertica.wutang")

#Dictionary
#Blank:
#psa: public service annoucement (when no accepted bid)
#default: nothing serves, no accepted bids, default creative
#default error: nothing serves due to bidder timeout
#Ext imps: imp trackers
#Ext Click: click trackers
imp_type_table = pd.DataFrame({'imp_type': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
       'imp_type_name': 
         ['Blank', 'PSA', 'Default Error', 'Default', 'Kept', 'Resold', 'RTB', 'PSA Error', 'Ext Imp', 'Ext Click', 'FB']})

rev_type_table = pd.DataFrame({'revenue_type': [-1, 0, 1, 2, 3, 4, 5, 6],
       'rev_type_name': ['No Payment', 'Flat CPM', 'Cost plus CPM', 'Cost plus Margin', 'CPC', 'CPA', 'Revshare', 'Flat Fee']})
#no more 2,3,4, 6,7
#bid type not really used
#None: 
predict_type_rev_table = pd.DataFrame( {'predict_type_rev': [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                                        'predict_type_name': ['None', 'Base', 'Learn Give-Up', 'Learn', 
                                        'Throttled', 'Optimized_0', 'Biased_1', 'Optimized_1', 
                                        'Biased_2', 'Optimized_2', 'Optimized_Give_up', 'Base Give-Up'],
                                        'bid_type': ['-', 'Manual', 'Learn Give-Up', 'Learn', 'Optimized', 
                                                     'Optimized', 'Optimized','Optimized', 'Optimized', 
                                                     'Optimized', 'Optimized Give-Up', 'Manual Give-Up']}
                                      )
opt_state_table = pd.DataFrame({'opt_state': [0,1,2,3,4,5],
                                'opt_state_nm':['Test','Pass','Fail','Suspended','Frozen','Eligible']}
                               )

def get_full_site_list(site_list):
    ''' input: a list of site_list
	output: a list of site_list, satuated for ozones
    '''
    ozone_id = list(get_ozone_from_site(site_list).ozone_id)
    return get_site_from_ozone(ozone_id)

#  https://git.corp.appnexus.com/cgit/user/stzeng/metrics.git/tree/oqt/oqt_performance_graphing_modules.py
def clogged_managed_nodes(site_list, lookback_d):
    ''' input should be a full site_id list
    '''    
    site_string = ','.join(map(str, site_list ))
    
    vert_q = """
    SELECT
        campaign_group_id,
        site_id,
        geo_country,
        width, 
        height,
        sum(imps::bigint) as imps
    FROM agg_dw_advertiser_publisher_analytics_adjusted
    WHERE
        ymd > now() - interval'{lookback} days'
        AND imp_type = 5
        AND revenue_type in (3, 4)
        AND site_id in ({site_string})
        AND predict_type_rev in (0, 1)
    GROUP BY campaign_group_id, 
             site_id, 
             geo_country, 
             width, 
             height
    """.format(lookback = lookback_d, site_string = site_string)
    nodes = vert_conn.select_dataframe(vert_q)
    
    site_ozone_map = get_ozone_from_site(site_list)
    ozone_string = ','.join(map(str, site_ozone_map.ozone_id.unique()))
    
    ## Pull in testing nodes from opt mysql
    testing_q = """
    SELECT 
        campaign_group_id, 
        ozone_member_id,
        ozone_id,
        country_group_id,
        creative_size_bucket_id
    FROM offer_quick_test_offers
    WHERE 
        deleted = 0
        AND opt_state = 0
        AND ozone_id in ({ozone_string})
    """.format(ozone_string = ozone_string)
    testing_nodes = opt_conn.select_dataframe(testing_q)
    cg_mem = bidder_conn.select_dataframe( 
    'select id, member_id from campaign_group where id in (%s)' % 
    ','.join(map(str, set(testing_nodes.campaign_group_id) ))   
    )
    managed_testing = testing_nodes.merge(cg_mem, left_on=['campaign_group_id','ozone_member_id'],
                                                  right_on=['id', 'member_id' ] )
    cr_size_map = get_cr_size_map()
    
    nodes['country_group_id'] = [geo_group_dict[geo]  for geo in nodes.geo_country]
    nodes = nodes.merge(site_ozone_map, on='site_id').merge(cr_size_map, on=['width', 'height'])
  
    reporting_mapped = nodes.groupby(['campaign_group_id',
                                 'country_group_id', 
                                 'creative_size_bucket_id',
                                 'ozone_id'], as_index = False).agg({'imps':np.sum})
    
    # Merge the two tables (opt_mysql-all nodes merge with table signifying there are imps in last 30 days)
    clog_data = managed_testing.merge(reporting_mapped, how='left').fillna(0)
    
    # calculate the number of offers per sell side key
    total_offers = clog_data.groupby(['country_group_id', 
                                      'creative_size_bucket_id',
                                      'ozone_id'
                                      ],as_index=False).agg({'campaign_group_id': 
                                                              lambda x : sum(x>0)})
    total_offers.rename(columns={'campaign_group_id': 'number_of_offers'}, inplace=True)
    
    # Calc number of offers WITH IMPS per sell side key
    offers_w_imps = clog_data.groupby(['ozone_id', 
                       'country_group_id', 
                       'creative_size_bucket_id'],as_index=False).agg({'imps': lambda x : sum(x>0)  })
    offers_w_imps.rename(columns={'imps': 'number_of_offers_w_imps'}, inplace=True)

    # merge the 2 tables
    clog = total_offers.merge(offers_w_imps)
    clog['clogged_offers'] = clog['number_of_offers'] - clog['number_of_offers_w_imps']
    clog = clog.sort('clogged_offers', ascending = 0)
    return clog


