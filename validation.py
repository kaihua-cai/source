import numpy as np
import pandas as pd
from collections import defaultdict 

from framework import Nexus

nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
bidder_conn = dbm.connection("mysql.bidder")
api_conn = dbm.connection("mysql.api")
opt_conn = dbm.connection("mysql.optimization")
vert_conn = dbm.connection("vertica.wutang")


# get country group 
geo_df = api_conn.select_dataframe('select id, name, code, country_group_id as cgid from bidder.country')

geo_group_dict = defaultdict(str)
for inx,row in geo_df.iterrows():
    geo_group_dict[row['code']] = row['cgid']


# given size, return size_buck
def calc_size_buck(x):
    if x > 90000:
        return 3
    if x > 74000:
        return 2

    if x == 7128:
        return 4
    if x == 33782:
        return 5
    if x == 7200:
        return 6
    return 1

# imps rows have no pixel_id=0
# convs rows have real pixel_id
# need to figure out the pixel_id for the li_goal
# vertica query to retrieve node data
# goal_type
def retrieve_vert_data4pie_node(campaign_group_id, cgid, tag_id, size_bucket, ymdh_s, ymdh_e, pixel_id=-1):
    size_dict = {1: 74000, 2: (74000, 90000), 3: 90000,4: 7128, 5: 33782, 6: 7200}

    geo_country = list(geo_df.code[geo_df.cgid == cgid])
    geo_country = "'" +  "','".join(geo_country) + "'"
    if size_bucket >=4:
        size_q = "width * height = {}".format(size_dict[size_bucket])
    elif size_bucket == 3:
        size_q = "width * height > {}".format(size_dict[size_bucket])
    elif size_bucket ==2:
        size_q = "width * height > {} and width * height <= {}".format(* size_dict[size_bucket])
    elif size_bucket ==1:
        size_q = "width * height <= {} and width * height not in (7128, 7200, 33782)".format(size_dict[size_bucket])
    elif size_bucket == 0:
        size_q = "width = 0 and  height = 0"
    else:
        raise ValueError("size bucket is not valide.")

    vert_q = """select ymdh, sum(imps) as imps, sum(clicks) as clicks, 
            sum(post_click_convs) as pcc, sum(post_view_convs) as pvc,
            sum(booked_revenue_adv_curr) as booked_rev, sum(booked_revenue_dollars) as booked_rev_dol 
            from agg_dw_advertiser_publisher_analytics_adjusted
            where campaign_group_id = {campaign_group_id}
            and geo_country in ({geo}) 
            and tag_id = {tag_id}
            and {size_q}
            and ymdh >= '{ymdh_s}'
            and ymdh <= '{ymdh_e}'
            and imp_type in (5, 7, 11)
            and pixel_id in (0, {pixel_id}) 
            group by ymdh
            """.format(campaign_group_id = campaign_group_id,
                       geo = geo_country,
                       size_q = size_q,
                       tag_id = tag_id,
                       ymdh_s=ymdh_s,
                       ymdh_e=ymdh_e,
                       pixel_id = pixel_id)
    print vert_q
    return vert_conn.select_dataframe(vert_q)


# given the line_item, figure out the campaign_group_goal, goal_type, goal threshold
# the output only keeps those li with not-NaN goal setup.
def fetch_li_goal(lis):
    camp_group_goal_q = """select cg.id as cg_id, cg.goal_type, cg.revenue_type,
                               cgv.goal_threshold,
                               cgg.pixel_id,
                               cgg.post_view_goal_threshold as pvg_threshold,
                               cgg.post_click_goal_threshold as pcg_threshold,
                               cgg.deleted AS cgg_deleted,
                               cgv.deleted AS cgv_deleted
                        from campaign_group as cg
                        left join campaign_group_valuation as cgv
                        on cg.id = cgv.campaign_group_id
                        left join campaign_group_goal as cgg
                        on cg.id = cgg.campaign_group_id
                        where cg.id in ({})
                        and cg.deleted = 0  
                        ;""".format(str(lis)[1:-1])
    ligoal = bidder_conn.select_dataframe(camp_group_goal_q)
    ctr_li = ligoal[ligoal.goal_type == 'ctr']
    ctr_li = ctr_li[[ (x is not None) for x in ctr_li.goal_threshold]]
    if ctr_li.shape[0] > 0 :
        ctr_li = ctr_li[ctr_li.cgv_deleted !=1]
        del ctr_li['cgv_deleted']
        del ctr_li['cgg_deleted']
        del ctr_li['pixel_id']
        del ctr_li['pvg_threshold']
        del ctr_li['pcg_threshold']
    
    cpc_li = ligoal[ligoal.goal_type == 'cpc']
    cpc_li = cpc_li[[ (x is not None) for x in cpc_li.goal_threshold]]

    
    cpa_li = ligoal[ligoal.goal_type == 'cpa']
    cpa_li = cpa_li[[(row['pvg_threshold'] is not None) or (row['pcg_threshold'] is not None)
                          for ind, row in cpa_li.iterrows()]]
    if cpa_li.shape[0] > 0 :   
        cpa_li = cpa_li[cpa_li.cgg_deleted !=1]
        del cpa_li['cgv_deleted']
        del cpa_li['cgg_deleted']
        del cpa_li['goal_threshold']

    return ctr_li, cpc_li, cpa_li


# fetch vertica data for a list of line items, aggregated to the most granular for nodes.
# this function works for li with goal_type=ctr or cpc
def retrieve4ctr_lis(lis, ymdh_s, ymdh_e):
    vert_q = """select ymdh, campaign_group_id, 
                        tag_id, geo_country,  
                        width * height as size,
                        sum(imps) as imps, sum(clicks) as clicks ,
                        sum(booked_revenue_adv_curr) as booked_rev, sum(booked_revenue_dollars) as booked_rev_dol
                    from agg_dw_advertiser_publisher_analytics_adjusted
                    where campaign_group_id in ({campaign_group_id})
                        and imp_type in (5, 7, 11)
                        and pixel_id = 0 
                        and ymdh >= '{ymdh_s}'
                        and ymdh <= '{ymdh_e}'
                    group by ymdh,
                        campaign_group_id,
                        pixel_id,
                        tag_id,
                        geo_country,
                        size;
                        """.format(campaign_group_id = str(lis)[1:-1],
                       ymdh_s=ymdh_s, 
                       ymdh_e=ymdh_e
                       )
    return vert_conn.select_dataframe(vert_q)




# fetch vertica data for a list of line items of goal_type = cpa, 
# aggregated to the most granular for nodes.
def retrieve4cpa_lis(lis, ymdh_s, ymdh_e):
    vert_q = """select ymdh, campaign_group_id, pixel_id, tag_id, 
                    geo_country, 
                    width * height as size,
                    sum(booked_revenue_adv_curr) as booked_rev, sum(booked_revenue_dollars) as booked_rev_dol, 
                    sum(post_view_convs) as post_view_convs,
                    sum(post_click_convs) as post_click_convs
                    from agg_dw_advertiser_publisher_analytics_adjusted
                    where campaign_group_id in ({campaign_group_id})
                    and imp_type in (5, 7, 11)
                    and ymdh >= '{ymdh_s}'
                    and ymdh <= '{ymdh_e}'
                    group by ymdh,
                    campaign_group_id,
                    pixel_id,
                    tag_id,
                    geo_country,
                    size;
                    """.format(campaign_group_id = str(lis)[1:-1],
                       ymdh_s=ymdh_s, 
                       ymdh_e=ymdh_e
                       )
    return vert_conn.select_dataframe(vert_q)


goal_type={"ctr": ('imps', 'clicks'),
           "cpc": ('booked_rev', 'clicks'),
           "cpa_post_click": ('booked_rev', 'post_click_convs'),
           "cpa_post_view": ('booked_rev', 'post_view_convs')}

def pie_testing(lis, ymdh_s, ymdh_e):
    ctr_li, cpc_li, cpa_li = fetch_li_goal(lis)
    if cpa_li.shape[0]:
	pvg_li = cpa_li[[ not np.isnan(x) for x in cpa_li.pvg_threshold ]]
        pcg_li = cpa_li[[ not np.isnan(x) for x in cpa_li.pcg_threshold ]]
     
        # handle cpa line item
        # every cpa line item has its unique pixel for goal 
        li_pixel = {}
        for inx, row in cpa_li.iterrows():
            li_pixel[row['cg_id']] = row['pixel_id']
    
        cpa_data = retrieve4cpa_lis(list(cpa_li.cg_id), ymdh_s, ymdh_e)
        cpa_data['country_group_id'] = [geo_group_dict[x] for x in cpa_data.geo_country]
        cpa_data['creative_size_bucket_id'] = [calc_size_buck(x) for x in cpa_data['size'] ]
        agg_cpa = cpa_data.groupby(['pixel_id', 'campaign_group_id', 'tag_id', 'country_group_id', 'creative_size_bucket_id', 'ymdh'], as_index=False
                     ).agg({'booked_rev':np.sum, 'booked_rev_dol': np.sum, 'post_view_convs': np.sum, 'post_click_convs':np.sum}) 
        agg_cpa['pixel_id'] = [li_pixel[row['campaign_group_id']] 
                       if row['pixel_id'] ==0 and row['campaign_group_id'] in li_pixel 
                       else row['pixel_id'] for _, row in agg_cpa.iterrows()]
        agg_cpa = agg_cpa[[ x in li_pixel.values() for x in agg_cpa.pixel_id ]]
    else:
  	agg_cpa = pd.DataFrame()

    # handle ctr and cpc line item
    if cpc_li.shape[0] + ctr_li.shape[0] == 0 :
	agg_ctr, agg_cpc =  pd.DataFrame(), pd.DataFrame()
    else:
	line_item = ( list(ctr_li.cg_id)  if ctr_li.shape[0] else []) +(list(cpc_li.cg_id)  if cpc_li.shape[0] else [])
        clk_data = retrieve4ctr_lis( line_item, ymdh_s, ymdh_e)
        clk_data['country_group_id'] = [geo_group_dict[x.upper()] for x in clk_data.geo_country]
        clk_data['creative_size_bucket_id'] = [calc_size_buck(x) for x in clk_data['size'] ]
        agg_clk = clk_data.groupby(['campaign_group_id', 'tag_id', 'country_group_id', 'creative_size_bucket_id', 'ymdh'], as_index=False
                     ).agg({'booked_rev':np.sum, 'booked_rev_dol': np.sum,  'imps': np.sum, 'clicks':np.sum}) 
	if ctr_li.shape[0] > 0 :
            agg_ctr = agg_clk[[ x in list(ctr_li.cg_id) for x in agg_clk.campaign_group_id]]   
	else:
	    agg_ctr = pd.DataFrame()
	if cpc_li.shape[0] > 0 :
            agg_cpc = agg_clk[[ x in list(cpc_li.cg_id) for x in agg_clk.campaign_group_id]]   
	else:
	    agg_cpc = pd.DataFrame()
    return agg_ctr, agg_cpc, agg_cpa
              

