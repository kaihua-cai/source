# make hive table, stored at textfile
# with size there too

from datetime import datetime, timedelta, date
import subprocess
import random
import os,sys
from framework import Nexus

from rtb_ozone_buyer_convs import members_exclude, retrieve_big_camps, hive_execute

nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
opt_db = dbm.connection("mysql.optimization")
bidder_db = dbm.connection("mysql.bidder")

# set up hive table
"""
CREATE EXTERNAL TABLE agg_rtb_ozone_campaign
( supply_type int, 
  operating_system string,
  size int,
  content_category_id int,
  site_domain string,
  geo_country string,
  campaign_id int,
  imps bigint,
  clicks bigint  
)
PARTITIONED BY (dd string, dh string)
STORED AS TEXTFILE
LOCATION '/user/kcai/prod/agg_campaign';    
"""

agg_q = """select
                i.supply_type AS supply_type,
   		i.operating_system, 
                i.size AS size,
                i.content_category_id AS content_category_id,
                i.site_domain AS site_domain,
                i.geo_country AS geo_c,
                b.campaign_id AS campaign_id, 
                COUNT(1) AS imps, 
                SUM (c.clicks) AS clicks
           FROM
               (    SELECT auction_id_64, operating_system, width * height as size, supply_type, content_category_id, site_domain, 
                     geo_country
                    FROM log_opt_imps
                    WHERE 1=1
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
            (   SELECT auction_id_64, 1 as clicks
                FROM log_opt_clicks
                Where 
		     dh >= '%(dh)s' 
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

insert_q = """INSERT OVERWRITE TABLE agg_rtb_ozone_campaign  
            PARTITION (dd = '%(dd)s', dh = '%(dh)s') \n""" 


if __name__ == '__main__':
    yes_date = (datetime.now() - timedelta(hours=24)).date()
    print yes_date

    big_camps = retrieve_big_camps ( )
    print big_camps.shape

    #yes_date = date(2014, 8, 28)
    start_hour = datetime(yes_date.year, yes_date.month, yes_date.day)
    for h in range(0,24):
        the_hour = start_hour + timedelta(hours = h) 
        next_hour = the_hour + timedelta(hours = 1)
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

