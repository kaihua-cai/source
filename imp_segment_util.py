import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import argparse
import subprocess

if True:
    from link import lnk
    bidder_db = lnk.dbs.prod.my_bidder
else:
    from framework import Nexus
    nexus = Nexus.get_instance()
    dbm = nexus.get_db_manager()
    bidder_db = dbm.connection("mysql.bidder")

def hive_execute(query):
    hive_command = "hive -e \"" + query.replace('\n', ' ').replace('\t', ' ') + '\"\n'
    return subprocess.call(hive_command, shell = True)

def fetch_tree_camps():
    # fetch all the campaigns using  tree model
    camp_q = '''
        select bm.bid_model_id  as model_id, bm.campaign_id  
        from campaign_bid_model bm 
        join campaign cp 
        on bm.campaign_id = cp.id 
        where cp.deleted = 0 
        and bm.deleted = 0
        and cp.state='active';
        '''
    return bidder_db.select_dataframe(camp_q)

def merge_write_imps_tree_camps(the_hour, end_hour, camp_list):
    join_q = """select
	        /*+ STREAMTABLE(imp_log) */
		imp_log.auction_id_64,
                imp_log.tag_id,
                imp_log.ip_address,
                imp_log.venue_id,
                imp_log.site_domain,
                imp_log.width,
                imp_log.height,
                imp_log.geo_country,
                imp_log.geo_region,
                imp_log.gender,
                imp_log.age,
                imp_log.seller_member_id,
                imp_log.creative_id,
                imp_log.cookie_age,
                imp_log.fold_position,
                imp_log.publisher_id,
                imp_log.site_id,
                imp_log.content_category_id,
                imp_log.user_tz_offset,
                imp_log.user_group_id,
                imp_log.media_type,
                imp_log.operating_system,
                imp_log.browser,
                imp_log.language,
                imp_log.application_id,
                imp_log.user_locale,
                imp_log.inventory_url_id,
                imp_log.audit_type,
                imp_log.truncate_ip,
                imp_log.device_id,
                imp_log.carrier_id,
                imp_log.city,
                imp_log.device_unique_id,
                imp_log.supply_type,
                imp_log.is_toolbar,
                imp_log.sdk_version,
                imp_log.inventory_session_frequency,
                imp_log.device_type,
                imp_log.dma,
                imp_log.postal,
                bid_log.targeted_segments,
                bid_log.campaign_id,
		log_pixel.pixel_id
            FROM
                dmf.log_dw_bid_pb bid_log   
            JOIN
                dmf.log_impbus_impressions_pq imp_log
            ON
                imp_log.auction_id_64 = bid_log.auction_id_64
                and imp_log.dh = '%(dh)s'
                and bid_log.dh = '%(dh)s'
                and bid_log.campaign_id in (%(camp_list)s)
	    LEFT JOIN 
		dmf.log_dw_pixel_pb  log_pixel
 	    ON 
		log_pixel.auction_id_64 = imp_log.auction_id_64
		and log_pixel.campaign_id in (%(camp_list)s)
		and log_pixel.dh >= '%(dh)s'
		and log_pixel.dh <= '%(dh_end)s'	
		;
                """
                
    insert_q = """ SET hive.exec.compress.output=false;
                   SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
                   SET mapred.output.compression.type=BLOCK;
                   SET hive.merge.mapredfiles=true;
                   SET hive.merge.mapfiles=true;
                   INSERT OVERWRITE TABLE data_science.log_imps_tree_campaigns
                   PARTITION (dy = '%(dy)s', dm = '%(dm)s', dd = '%(dd)s', dh = '%(dh)s') \n"""
    
    query_dc = {'dh': the_hour.strftime("%Y-%m-%d %H"),
            'dy': the_hour.strftime("%Y"),
            'dm': the_hour.strftime("%Y-%m"),
            'dd': the_hour.strftime("%Y-%m-%d"),
	    'dh_end': end_hour.strftime("%Y-%m-%d %H"),
            'camp_list': ",".join(map(str, camp_list))}
            
    total_q = ( insert_q + join_q ) % query_dc
    print total_q
    t1 =  datetime.now()
    hive_execute(total_q)
    print (datetime.now() - t1).total_seconds()


def get_args():
    """
    Returns the command line arguments which allows you to override the start date and end date
    """
    parser = argparse.ArgumentParser(description='using --ymdh to specify ymdh for which to load data')
    parser.add_argument('--ymdh', type=str, help='the hour to load data for, yyyy-mm-dd HH')
    parser.add_argument('--end_hour', type=str, help='the hour when to stop to merge pixel to the imp, yyyy-mm-dd HH')
    return parser.parse_args()

if __name__ == '__main__':
    _args = get_args()
    if not _args.ymdh is None :
        the_hour = datetime.strptime(_args.ymdh, "%Y-%m-%d %H")
    else:
        the_hour = datetime.now() - timedelta(hours = 25) 
    print the_hour.strftime("%Y-%m-%d %H")

    if not _args.end_hour is None :
        end_hour = datetime.strptime(_args.end_hour,  "%Y-%m-%d %H")
    else:
        end_hour = datetime.now()

    print end_hour.strftime("%Y-%m-%d %H")

    tree_camps = fetch_tree_camps() 
    merge_write_imps_tree_camps(the_hour, end_hour, tree_camps.campaign_id)
