# make hive table, stored at textfile
# with size there too

from datetime import datetime, timedelta, date
import subprocess
import random
import os,sys
from framework import Nexus
#from load_data_hadoop_util import *
from load_data_hadoop_hive_c_line import *

nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
opt_db = dbm.connection("mysql.optimization")
bidder_db = dbm.connection("mysql.bidder")

# set up hive table
"""
CREATE EXTERNAL TABLE agg_rtb_ozone_convs
( supply_type int, 
  operating_system string,
  size int,
  content_category_id int,
  site_domain string,
  geo_country string,
  campaign_id int,
  imps bigint,
  pc_convs bigint  
)
PARTITIONED BY (dd string, dh string)
STORED AS TEXTFILE
LOCATION '/user/kcai/prod/agg_convs';    
"""

members_exclude = [332, 865, 718, 948, 1754, 1251, 1886, 634,
                    1920, 1039, 326, 319, 630, 1544, 1004, 848, 
                    456, 95, 1979, 1739, 1974, 1656, 1318, 88,
                     1939, 928, 1906, 1357, 1869, 1972, 1333, 1697, 818]

agg_q = """select
                i.supply_type AS supply_type,
   		i.operating_system, 
                i.size AS size,
                i.content_category_id AS content_category_id,
                i.site_domain AS site_domain,
                i.geo_country AS geo_c,
                b.campaign_id AS campaign_id, 
                COUNT(1) AS imps, 
                SUM (c.pc_convs) AS pc_convs
           FROM
               (    SELECT auction_id_64, operating_system, width * height as size, supply_type, content_category_id, site_domain, 
                    creative_id, geo_country, seller_member_id
                    FROM log_opt_imps
                    WHERE 1=1
                    AND seller_member_id != buyer_member_id
                    And seller_member_id not in (%(members)s)
                    AND content_category_id > 0
                    And dh = '%(dh)s' 
                )  i
            JOIN
                (   SELECT auction_id_64,
		    campaign_id	
                    FROM log_opt_bids
                    WHERE payment_type in (0,3,4)
                    AND campaign_id in (%(campaigns)s) 
                    AND dh = '%(dh)s'
                )   b
            ON  b.auction_id_64 = i.auction_id_64
            LEFT  JOIN
            (   SELECT auction_id_64, 1 as pc_convs
                FROM conversions
                Where post_click_conv = 1
		    and dh >= '%(dh)s' 
		    and dh <= '%(dh_next)s'
            )   c
            ON i.auction_id_64 = c.auction_id_64
            GROUP BY 
                    i.supply_type,
		    i.operating_system,	 
                    i.size,
                    i.content_category_id,
                    i.site_domain,
                    b.campaign_id, 
                    i.geo_country;
        """ 

insert_q = """INSERT OVERWRITE TABLE agg_rtb_ozone_convs  
            PARTITION (dd = '%(dd)s', dh = '%(dh)s') \n""" 

def retrieve_pc_cpa_big_camps():
    query = "select campaign_id, count(1) from  categorizer_current where type in ( 'rtb_event_payout', 'rtb_cpm_payout')  and value_type = 'pc_cpa' group by campaign_id having count(1) = 1;"
    camp_df = opt_db.select_dataframe(query)
    query = '''
        select object_id as campaign_id, imps, clicks, post_view_convs, post_click_convs,booked_revenue 
        from quick_stats 
        where time_interval='7day' 
        and report_type='campaign'
        and object_id in (%s) 
	and imps > 3000000;
        ''' % ",".join(map(str, list(camp_df.campaign_id)))
    x = bidder_db.select_dataframe(query)
    return x[x.clicks > 2 * x.post_click_convs]

def retrieve_big_camps():
    query = '''
        select object_id as campaign_id, imps, clicks, post_view_convs, post_click_convs,booked_revenue 
        from quick_stats 
        where time_interval='7day' 
        and report_type='campaign'
        and imps > 7000000;
        '''
    return bidder_db.select_dataframe(query)


def hive_execute(query):
    hive_command = "hive -e \"" + query.replace('\n', ' ').replace('\t', ' ') + '\"\n'
    return subprocess.call(hive_command, shell = True)                        

def hive_execute2(query):
        query = query.replace('\n', ' ').replace('\t', ' ') + '\n'

        fname = str(random.random())[2:]+'.sql'
        with open(fname, 'w') as query_file:
                query_file.write(query)

        return_code = subprocess.call("hive -f "+ fname, shell = True)
        os.remove(fname)
        return(return_code)

def load_convs_data_to_hive():
    local_dump_dir = '/home/kcai/data/tmp/dump_data/'
    hours_to_get = get_hours_with_vert_conv_data()
    conv_data_exist = get_and_process_dh('conversions')
    hours_to_get = get_rid_of_hours_already_moved(hours_to_get, conv_data_exist)
    for ii in hours_to_get:
        print ii
        pull_data_from_vert(ii,local_dump_dir)
        date_strings = get_strings_for_hdfs_file_managment(ii)
        create_hdfs_folder('/user/agreenstein/conversions', date_strings)
        folder_name = "/user/agreenstein/conversions/dy={year}/dm={year_month}/dd={year_month_day}/dh={year_month_day} {hour}"
        folder_name = folder_name.format( year=date_strings['year'],
                                         year_month=date_strings['year_month'],
                                         year_month_day=date_strings['year_month_day'],
                                         hour=date_strings['hour']
                                         )
        push_data_to_hadoop(local_dump_dir +'dumpfile.txt', folder_name, 'dump.txt')
        add_data_to_hadoop_table('conversions', date_strings)

if __name__ == '__main__':
    load_convs_data_to_hive()
    yes_date = (datetime.now() - timedelta(hours=24)).date()
    print yes_date

    big_camps = retrieve_pc_cpa_big_camps ( )
    print big_camps.shape

    #yes_date = date(2014, 8, 28)
    start_hour = datetime(yes_date.year, yes_date.month, yes_date.day)
    for h in range(0,24):
        the_hour = start_hour + timedelta(hours = h) 
        next_hour = the_hour + timedelta(hours=23)
        print the_hour
        query_dc = {'dh': the_hour.strftime("%Y-%m-%d %H"),
                'dy': the_hour.strftime("%Y"),
                'dm': the_hour.strftime("%Y-%m"),
                'dd': the_hour.strftime("%Y-%m-%d"),
                'dh_next': next_hour.strftime("%Y-%m-%d %H"),
                'members': ",".join(map(str,members_exclude)),
		'campaigns': ",".join(map(str, big_camps.campaign_id))}  
                
        total_q =  ( insert_q + agg_q ) % query_dc         
        print total_q
        t1 =  datetime.now()
        hive_execute(total_q) 
        print (datetime.now() - t1).total_seconds()

