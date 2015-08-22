# make hive table, stored at textfile
# with size there too

from datetime import datetime, timedelta, date
import subprocess
import random

# set up hive table
"""
CREATE EXTERNAL TABLE agg_rtb_ozone_2
( supply_type int, 
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
LOCATION '/user/kcai/prod/agg_2';    
"""

members_exclude = [332, 865, 718, 948, 1754, 1251, 1886, 634,
                    1920, 1039, 326, 319, 630, 1544, 1004, 848, 
                    456, 95, 1979, 1739, 1974, 1656, 1318, 88,
                     1939, 928, 1906, 1357, 1869, 1972, 1333, 1697, 818]


agg_q = """select
                i.supply_type AS supply_type, 
                i.width AS width,
                i.height AS height,
                i.content_category_id AS content_category_id,
                i.site_domain AS site_domain,
                i.geo_country AS geo_c,
                i.seller_member_id AS seller_member_id, 
                COUNT(1) AS imps, 
                SUM (c.click) AS clicks
           FROM
               (    SELECT auction_id_64, width, height, supply_type, content_category_id, site_domain, 
                    creative_id, geo_country, seller_member_id
                    FROM log_opt_imps
                    WHERE 1=1
                    AND seller_member_id != buyer_member_id
                    And seller_member_id not in (%(members)s)
                    AND content_category_id > 0
                    And dh = '%(dh)s' 
                )  i
            JOIN
                (   SELECT auction_id_64
                    FROM log_opt_bids
                    WHERE  payment_type = 0
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
                    i.width,
                    i.height,
                    i.content_category_id,
                    i.site_domain,
                    i.seller_member_id, 
                    i.geo_country;
        """ 

insert_q = """INSERT OVERWRITE TABLE agg_rtb_ozone_2  
            PARTITION (dd = '%(dd)s', dh = '%(dh)s') \n""" 
    


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


if __name__ == '__main__':
    yes_date = (datetime.now() - timedelta(hours=24)).date()
    start_hour = datetime(yes_date.year, yes_date.month, yes_date.day)
    for h in range(0,24):
        the_hour = start_hour + timedelta(hours = h) 
        next_hour = the_hour + timedelta(hours=1)
        print the_hour
        query_dc = {'dh': the_hour.strftime("%Y-%m-%d %H"),
                'dy': the_hour.strftime("%Y"),
                'dm': the_hour.strftime("%Y-%m"),
                'dd': the_hour.strftime("%Y-%m-%d"),
                'dh_next': next_hour.strftime("%Y-%m-%d %H"),
                'members': ",".join(map(str,members_exclude))}  
                
        total_q =  ( insert_q + agg_q ) % query_dc         
        print total_q
        t1 =  datetime.now()
        hive_execute(total_q) 
        print (datetime.now() - t1).total_seconds()

