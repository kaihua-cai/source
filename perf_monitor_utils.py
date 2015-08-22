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


def pull_sites() :
    """
    Returns table of all sites turned into OQT

    :params - None
    :returns - (DataFrame) sites
    """
    
    # Filter out all 958 member sites
    site_query = """
    SELECT
        s.member_id, 
        smm.site_id,
        smm.created_on,
        date(smm.last_modified) as last_modified
    FROM
        site_marketplace_map smm
    JOIN
        api.site s
    ON s.id = smm.site_id
    WHERE
        smm.deleted = 0
        AND smm.performance = 1
        AND s.deleted = 0
        AND s.member_id <> 958
    ORDER BY
        created_on DESC
    """
    sites = common_conn.select_dataframe(site_query)

    return sites

#pull sites that were flipped in by member
def pull_sites_member(flip_date, member_id):
    """
    flip_date: flip starting date
    member_id: member whose inventory was flipped into OQT
    """
    
    site_query = """
    SELECT
        s.member_id,
        smm.site_id, 
        smm.created_on, 
        date(smm.last_modified) as last_modified
    FROM
        site_marketplace_map smm
    JOIN
        api.site s
    ON s.id = smm.site_id
    WHERE
        smm.deleted = 0
        AND smm.performance = 1
        AND s.deleted = 0
        AND s.member_id = {member_id}
        AND smm.created_on>= '{flip_date}'
    """.format(member_id=member_id,flip_date=flip_date)
    
    sites = common_conn.select_dataframe(site_query)
    return sites


#mapping from v7 to OQT keys
def map_v7_to_oqt(data):
    """
    Map v7 sell side keys (site_id/geo_country/width/height) 
    to OQT sell side keys (ozone_id/country_group_id/creative_size_bucket_id).
    """
    #Map site_id to ozone_id
    site_ids='('+str(set(data['site_id']))[5:-2]+')'
    
    site_ozone_map_q = """
    SELECT id as site_id,
        managed_ozone_id as ozone_id
    FROM
        site
    WHERE
        deleted = 0
        AND state = 'active'
        AND id in {site_str}
    """.format(site_str = site_ids)
    site_ozone_map = api_conn.select_dataframe(site_ozone_map_q)
    
    #geo_country --> country_group_id
    cg_map_q = """
    SELECT 
        code as geo_country,
        country_group_id
    FROM country
    WHERE deleted = 0
    """
    new_cg_map = bidder_conn.select_dataframe(cg_map_q)
    
    #width + height --> creative_size_bucket_id
    cr_size_map_q = """
    SELECT
        id as creative_size_bucket_id,
        width,
        height
    FROM creative_size_buckets
    WHERE deleted = 0
    AND active = 1"""
    cr_size_map = opt_conn.select_dataframe(cr_size_map_q)
    
    data=pd.merge(left=data, right=site_ozone_map, how='left', on='site_id')
    data=pd.merge(left=data, right=new_cg_map, how='left', on='geo_country')
    data=pd.merge(left=data, right=cr_size_map, how='left', on=['width','height'])
    data['creative_size_bucket_id'] = data['creative_size_bucket_id'].fillna(1)    
    return data 


#get performance imps/revenue and learn imps/revenue by hour, pseudo nodes (LI:campaign:creative:tag:geo:size)
def get_perf_data(site_list, lookback_window):
    """
    Get total and learn imps, revenue for a list of sites
    
    Input:
        site_list (list)
        lookback_window (int)
    
    Output:
        perf_data (DataFrame) : data frame containing performance variables.
    
    """
    site_ids='('+str(site_list)[1:-1]+')'
    
    #all imps
    total_imps_q="""
    SELECT
        ymd,
        seller_member_id,
        site_id,     
        geo_country,
        width,
        height,
        campaign_group_id,
        campaign_id,
        creative_id,
        tag_id,
        sum(imps)::bigint as total_imps,
        sum(case when revenue_type in (3,4) then imps else 0 end)::bigint as perf_imps,
        sum(case when revenue_type in (3,4) and imp_type=5 then imps else 0 end)::bigint as perf_managed_imps,
        sum(case when revenue_type in (3,4) and imp_type=6 then imps else 0 end)::bigint as perf_xnet_imps,
        sum(booked_revenue_dollars + reseller_revenue) as total_revenue,
        sum(case when revenue_type in (3,4) then booked_revenue_dollars + reseller_revenue
          else 0 end) as perf_revenue,
        sum(case when revenue_type in (3,4) and imp_type=5 then booked_revenue_dollars + reseller_revenue
          else 0 end) as perf_managed_revenue,
        sum(case when revenue_type in (3,4) and imp_type=6 then booked_revenue_dollars + reseller_revenue
          else 0 end) as perf_xnet_revenue,    
        sum( case when imp_type in (5,6) and predict_type_rev in (0,1) and revenue_type in (3, 4) then imps
                  ELSE 0
             END ) as learn_imps,
        sum( case when imp_type in (5,6) and predict_type_rev in (0,1) and revenue_type in (3, 4) then booked_revenue_dollars
                  ELSE 0
             END ) as learn_rev
    FROM agg_dw_advertiser_publisher_analytics_adjusted 
    WHERE 
        site_id in {site_id}
        and ymd >= now() - interval '{lookback_window} days'
        and imp_type not in (7,9,10,11)
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    """.format(site_id=site_ids, lookback_window=lookback_window)
       
    return  vert_conn.select_dataframe(total_imps_q)

def get_hourly_perf_data(site_list, lookback_window):
    site_ids='('+str(site_list)[1:-1]+')'
    total_imps_q="""
    SELECT
        ymdh,
        seller_member_id,
        site_id,
        geo_country,
        width,
        height,
        campaign_group_id,
        campaign_id,
        creative_id,
        tag_id,
        sum(imps)::bigint as total_imps,
        sum(case when revenue_type in (3,4) then imps else 0 end)::bigint as perf_imps,
        sum(case when revenue_type in (3,4) and imp_type=5 then imps else 0 end)::bigint as perf_managed_imps,
        sum(case when revenue_type in (3,4) and imp_type=6 then imps else 0 end)::bigint as perf_xnet_imps,
        sum(booked_revenue_dollars + reseller_revenue) as total_revenue,
        sum(case when revenue_type in (3,4) then booked_revenue_dollars + reseller_revenue
          else 0 end) as perf_revenue,
        sum(case when revenue_type in (3,4) and imp_type=5 then booked_revenue_dollars + reseller_revenue
          else 0 end) as perf_managed_revenue,
        sum(case when revenue_type in (3,4) and imp_type=6 then booked_revenue_dollars + reseller_revenue
          else 0 end) as perf_xnet_revenue,
        sum( case when imp_type in (5,6) and predict_type_rev in (0,1) and revenue_type in (3, 4) then imps
                  ELSE 0
             END ) as learn_imps,
        sum( case when imp_type in (5,6) and predict_type_rev in (0,1) and revenue_type in (3, 4) then booked_revenue_dollars
                  ELSE 0
             END ) as learn_rev
    FROM agg_dw_advertiser_publisher_analytics_adjusted
    WHERE
        site_id in {site_id}
        and ymd >= now() - interval '{lookback_window} days'
        and imp_type not in (7,9,10,11)
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    """.format(site_id=site_ids, lookback_window=lookback_window)
    return  vert_conn.select_dataframe(total_imps_q)

#return site_id's under the publisher_ids
def get_site_of_publisher(publisher_id_list):
    """
    get a list of site under the publisher_id's provided
    
    input: publisher_id_list (list)
    output: sites (dataframe)
    
    """
    publisher_ids='('+str(publisher_id_list)[1:-1]+')'
    
    site_q="""
    select distinct id
    from site
    where publisher_id in {pub};
    """.format(pub=publisher_ids)
    
    sites=api_conn.select_dataframe(site_q)
    return list(sites.id)

def get_site_from_ozone(ozone_id):
    """ provide an ozone_id, return a list of site_id of that ozone
        the input could also be a list of ozone_id
    """
    if type(ozone_id) ==list:
        ozone_id = ','.join(map(str, ozone_id))
	
    ozone_q = '''
        SELECT site.id as site_id,
            site.managed_ozone_id as ozone_id
        FROM
            api.site site
        JOIN    
            site_marketplace_map smm
        ON 
            site.id = smm.site_id
        WHERE
            site.deleted = 0
            AND site.managed_ozone_id in ({ozone_id})
            AND smm.performance = 1
    '''.format(ozone_id = ozone_id)
    return list(common_conn.select_dataframe(ozone_q).site_id)

def get_ozone_from_site(site_list):
    site_string = ','.join(map(str,  site_list))
    
    site_ozone_map_q = """
    SELECT id as site_id,
        managed_ozone_id as ozone_id
    FROM
        site
    WHERE
        deleted = 0
        AND state = 'active'
        AND id in ({site_string})
    """.format(site_string = site_string)
    return api_conn.select_dataframe(site_ozone_map_q)

def get_cr_size_map():
    cr_size_map_q = """
    SELECT
        id as creative_size_bucket_id,
        width,
        height
    FROM creative_size_buckets
    WHERE deleted = 0
    AND active = 1"""
    return opt_conn.select_dataframe(cr_size_map_q)

def agg_perf_imp_rev(perf_imps_rev, cols):
    perf_imps_rev_day = perf_imps_rev.groupby(cols).sum()[['total_imps','total_revenue',
                                                              'learn_imps', 'learn_rev',
                                                              'perf_revenue','perf_imps',
                                                              'perf_managed_revenue','perf_managed_imps',
                                                              'perf_xnet_revenue','perf_xnet_imps']].reset_index()
    #learn %
    perf_imps_rev_day['learn_pct'] = perf_imps_rev_day['learn_imps']/perf_imps_rev_day['total_imps']
    #performance overall rpm
    perf_imps_rev_day['perf_rpm'] = perf_imps_rev_day['perf_revenue']*1000.0/perf_imps_rev_day['perf_imps']
    #performance managed rpm
    perf_imps_rev_day['perf_managed_rpm']=perf_imps_rev_day['perf_managed_revenue']*1000.0/perf_imps_rev_day['perf_managed_imps']
    #performance xnet rpm
    perf_imps_rev_day['perf_xnet_rpm']=perf_imps_rev_day['perf_xnet_revenue']*1000.0/perf_imps_rev_day['perf_xnet_imps']
    #flat CPM payment rpm
    perf_imps_rev_day['flat_cpm_rpm']=(perf_imps_rev_day['total_revenue']-perf_imps_rev_day['perf_revenue'])*1000.0/(
                                        perf_imps_rev_day['total_imps']-perf_imps_rev_day['perf_imps'])
    perf_imps_rev_day['perf_rpm']=perf_imps_rev_day['perf_revenue']*1000.0/perf_imps_rev_day['perf_imps']
    #flat CPM imps
    perf_imps_rev_day['flat_cpm_imps']=perf_imps_rev_day['total_imps']-perf_imps_rev_day['perf_imps']
    #flat CPM revenue
    perf_imps_rev_day['flat_cpm_revenue']=perf_imps_rev_day['total_revenue']-perf_imps_rev_day['perf_revenue']
    #learn rpm
    perf_imps_rev_day['learn_rpm']=perf_imps_rev_day['learn_rev']*1000.0/perf_imps_rev_day['learn_imps']
    return perf_imps_rev_day


