import numpy as np
import pandas as pd
from framework import Nexus

pd.options.mode.chained_assignment = None

nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
bidder_conn = dbm.connection("mysql.bidder")
opt_conn = dbm.connection("mysql.optimization")
vert_conn = dbm.connection("vertica.wutang")

def read_camp_perf(LI_data, ymdh_s, ymdh_e):    
    LI_str = '(' + str(list(LI_data.LI))[1:-1]+ ')'
    pixel_str = str(list(LI_data.pixel_id))[1:-1]
    
    vert_q = """
    SELECT
        campaign_group_id, 
        sum(imps) as imps, 
        sum(clicks) as clicks, 
        sum(post_click_convs) as pcc, 
        sum(post_view_convs) as pvc,
        sum(booked_revenue_adv_curr) as booked_rev, 
        sum(booked_revenue_dollars) as booked_rev_dol 
    FROM 
        agg_dw_advertiser_publisher_analytics_adjusted
    WHERE 
        campaign_group_id in %s AND
        ymdh >= '%s' AND 
        ymdh <= '%s' AND 
        imp_type in (5, 7, 11) AND
        pixel_id in (0, %s) 
    GROUP BY 
        campaign_group_id;
    """ %(LI_str, ymdh_s, ymdh_e, pixel_str)
    
    return vert_conn.select_dataframe(vert_q)
 
def fetch_daily_camp_perf(LI_data, start_date, end_date):    
    # start_date = '2015-04-08 00:00:00' or '2015-04-08'
    # end_date = '2015-04-09' 
    LI_str = '(' + str(list(LI_data.LI))[1:-1] + ')'
    pixel_str = str(list(set(LI_data.pixel_id)))[1:-1]    
    vert_q = """
    SELECT
        ymdh,
        campaign_group_id,
        sum(imps) as imps, 
        sum(clicks) as clicks, 
        sum(post_click_convs) as pcc, 
        sum(post_view_convs) as pvc,
        sum(booked_revenue_adv_curr) as booked_rev, 
        sum(booked_revenue_dollars) as booked_rev_dol 
    FROM 
        agg_dw_advertiser_publisher_analytics_adjusted
    WHERE 
        campaign_group_id in {LI_str} AND
        ymdh >= '{start_date}'  AND 
        ymdh < '{end_date}' AND 
        imp_type in (5, 7, 11) AND
        pixel_id in (0, {pixel_str}) 
    GROUP BY 
        ymdh, campaign_group_id
    ORDER BY 
        ymdh;
    """.format( LI_str= LI_str, pixel_str = pixel_str, start_date=start_date, end_date=end_date)
    print vert_q
    perf_data_pt_1 = vert_conn.select_dataframe(vert_q)
    
    sql_q = """
    SELECT
        concat(substring(last_modified,1,14),"00:00")  as ymdh,      
        campaign_group_id, 
        SUM(CASE WHEN exclusion_state_id = 2 THEN 1 ELSE 0 END) AS 'number_of_passed_ozones',
        SUM(CASE WHEN exclusion_state_id = 3 THEN 1 ELSE 0 END) AS 'number_of_excluded_ozones' 
    FROM(
        SELECT
            *
        FROM
            inventory_exclusion
        WHERE
            campaign_group_id in {LI_str} AND
	    last_modified >= '{start_date}' AND
            last_modified < '{end_date}' AND
	    exclusion_state_id != 1 AND
            active = 1
        GROUP BY
            campaign_group_id,
            country_group_id,
            tag_id,
            creative_size_bucket_id
        )L
    GROUP BY 
        ymdh, campaign_group_id
    ORDER BY
        ymdh
    """.format(LI_str = LI_str, start_date = start_date, end_date = end_date)        
    perf_data_pt_2 = opt_conn.select_dataframe(sql_q)
    perf_data_pt_1['ymdh'] = [x.strftime("%Y-%m-%d %H:%M:%S") for x in perf_data_pt_1.ymdh ]
    perf_data = pd.merge(perf_data_pt_1, perf_data_pt_2, how = 'left', on=['ymdh', 'campaign_group_id'])
    perf_data = perf_data.fillna(0)
    perf_data['group_type'] = "None"

    perf_data = perf_data.merge(LI_data, how='left', left_on='campaign_group_id', right_on='LI')
    del perf_data['LI']
    return perf_data

def conform_line_items(ctr_li, cpc_li, cpa_li):
    ctr_li = ctr_li[[ not np.isnan(x) for x in ctr_li.goal_threshold]]
    cpc_li = cpc_li[[ not np.isnan(x) for x in cpc_li.goal_threshold]]

    data_test_ctr = ctr_li[['cg_id', 'goal_type', 'goal_threshold']]
    data_test_ctr['pixel_id'] = 0
    data_test_ctr.rename(columns= {'cg_id': 'LI', 'goal_threshold': 'goal_value'}, inplace = True)
    
    data_test_cpc = cpc_li[['cg_id', 'goal_type', 'goal_threshold']]
    data_test_cpc['pixel_id'] = 0
    data_test_cpc.rename(columns= {'cg_id': 'LI', 'goal_threshold': 'goal_value'}, inplace = True)
    
    pc_cpa_li = cpa_li[[ not np.isnan(x) for x in cpa_li.pcg_threshold]]
    pv_cpa_li = cpa_li[[ not np.isnan(x) for x in cpa_li.pvg_threshold]]
    
    data_test_pc_cpa = pc_cpa_li[['cg_id', 'pcg_threshold', 'pixel_id']]
    data_test_pc_cpa['goal_type'] = 'cpa_post_click'
    data_test_pc_cpa.rename(columns= {'cg_id': 'LI', 'pcg_threshold': 'goal_value'}, inplace = True)
    
    data_test_pv_cpa = pv_cpa_li[['cg_id', 'pvg_threshold', 'pixel_id']]
    data_test_pv_cpa['goal_type' ] = 'cpa_post_view'
    data_test_pv_cpa.rename(columns= {'cg_id': 'LI', 'pvg_threshold': 'goal_value'}, inplace = True)

    return data_test_ctr, data_test_cpc, data_test_pc_cpa, data_test_pv_cpa

