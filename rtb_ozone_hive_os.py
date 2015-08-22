# make hive table, stored at textfile
# with size there too

from datetime import datetime, timedelta, date
import subprocess
import random

from rtb_ozone_hive  import members_exclude, hive_execute

# set up hive table
"""
CREATE EXTERNAL TABLE agg_rtb_ozone_os
( supply_type int, 
  operating_system string,
  width int,
  height int,
  content_category_id int,
  site_domain string,
  geo_country string,
  seller_member_id int,
  imps bigint,
  clicks bigint  
)
PARTITIONED BY (dd string, dh string)
STORED AS TEXTFILE
LOCATION '/user/kcai/prod/agg_0';    
"""

# modified the supply_type < 2 and device_type = 0 on Sept 15 2014
# include managed inventory on Oct 01 2014
# the data is fixed starting on Sept 30
agg_q = """select
                i.supply_type AS supply_type,
   		i.operating_system, 
                i.width AS width,
                i.height AS height,
                i.content_category_id AS content_category_id,
                i.site_domain AS site_domain,
                i.geo_country AS geo_c,
                i.seller_member_id AS seller_member_id, 
                COUNT(1) AS imps, 
                SUM (c.click) AS clicks
           FROM
               (    SELECT auction_id_64, operating_system, width, height, supply_type, content_category_id, site_domain, 
                    creative_id, geo_country, seller_member_id
                    FROM log_opt_imps
                    WHERE 1=1
		    AND supply_type < 2
		    AND device_type = 0 
                    And seller_member_id not in (%(members)s)
                    AND content_category_id > 0
                    And dh = '%(dh)s' 
                )  i
            LEFT SEMI JOIN
                (   SELECT auction_id_64
                    FROM log_opt_bids
                    WHERE  payment_type in (0,3,4)
                    AND campaign_id > 0 
                    AND dh = '%(dh)s'
                )   b
            ON  b.auction_id_64 = i.auction_id_64
            LEFT  JOIN
            (   SELECT auction_id_64, 1 as click
                FROM log_opt_clicks
                Where dh = '%(dh)s' or dh = '%(dh_next)s'
            )   c
            ON i.auction_id_64 = c.auction_id_64
            GROUP BY 
                    i.supply_type,
		    i.operating_system,	 
                    i.width,
                    i.height,
                    i.content_category_id,
                    i.site_domain,
                    i.seller_member_id, 
                    i.geo_country;
        """ 

insert_q = """INSERT OVERWRITE TABLE agg_rtb_ozone_os  
            PARTITION (dd = '%(dd)s', dh = '%(dh)s') \n""" 
    

def run_display_agg(the_hour):
    next_hour = the_hour + timedelta(hours=1)
    query_dc = {'dh': the_hour.strftime("%Y-%m-%d %H"),
                'dy': the_hour.strftime("%Y"),
                'dm': the_hour.strftime("%Y-%m"),
                'dd': the_hour.strftime("%Y-%m-%d"),
                'dh_next': next_hour.strftime("%Y-%m-%d %H"),
                'members': ",".join(map(str,members_exclude))}

    total_q =  ( insert_q + agg_q ) % query_dc
    print total_q
    hive_execute(total_q)

if __name__ == '__main__':
    yes_date = (datetime.now() - timedelta(hours=24)).date()
    start_hour = datetime(yes_date.year, yes_date.month, yes_date.day)
    for h in range(0,24):
        the_hour = start_hour + timedelta(hours = h)
        run_display_agg(the_hour) 
