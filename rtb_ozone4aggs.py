from datetime import datetime, timedelta, date
import subprocess
import random
from rtb_ozone_hive  import members_exclude, hive_execute

query4aggs = """
        FROM
        (
          SELECT 
          auction_id_64
          , device_id
          , operating_system
          , width
          , height
          , supply_type
          , content_category_id
          , device_type
          , site_domain
          , creative_id
          , application_id
          , geo_country
          , seller_member_id
          FROM log_opt_imps
          WHERE 1=1
          AND dh = '%(dh)s' 
          AND seller_member_id not in (%(members)s)
        ) i
        LEFT SEMI JOIN
        (
          SELECT auction_id_64
          FROM log_opt_bids
          WHERE payment_type in ( 0,3,4)
          AND campaign_id > 0 
          AND dh = '%(dh)s'
        ) b
        ON(i.auction_id_64 = b.auction_id_64)
        LEFT OUTER JOIN
        ( 
          SELECT auction_id_64, 1 as click
          FROM log_opt_clicks
          WHERE dh = '%(dh)s' or dh = '%(dh_next)s'
        ) c
        ON(i.auction_id_64 = c.auction_id_64) 
        INSERT OVERWRITE TABLE agg_rtb_ozone_fb PARTITION (dd = '%(dd)s', dh = '%(dh)s')
        SELECT
        i.operating_system
        , i.width AS width
        , i.height AS height
        , i.geo_country AS geo_c
        , COUNT(1) AS imps
        , SUM (c.click) AS clicks
        WHERE supply_type = 3 AND content_category_id > 0
        GROUP BY 
        i.operating_system
        , i.width
        , i.height
        , i.geo_country
        INSERT OVERWRITE TABLE agg_rtb_ozone_os PARTITION (dd = '%(dd)s', dh = '%(dh)s')
        SELECT
        i.supply_type AS supply_type
        , i.operating_system
        , i.width AS width
        , i.height AS height
        , i.content_category_id AS content_category_id
        , i.site_domain AS site_domain
        , i.geo_country AS geo_c
        , i.seller_member_id AS seller_member_id
        , COUNT(1) AS imps
        , SUM (c.click) AS clicks
        WHERE supply_type < 2 AND device_type = 0 AND content_category_id > 0
        GROUP BY 
        i.supply_type
        , i.operating_system
        , i.width
        , i.height
        , i.content_category_id
        , i.site_domain
        , i.seller_member_id
        , i.geo_country
        INSERT OVERWRITE TABLE agg_rtb_mob_app_ozone PARTITION (dd = '%(dd)s', dh = '%(dh)s')
        SELECT  
        i.device_id as device_id
        , i.operating_system AS operating_system
        , i.geo_country AS geo_c
        , i.application_id AS application_id
        , COUNT(1) AS imps
        , SUM (c.click) AS clicks
        WHERE supply_type = 2 
        GROUP BY
        i.device_id
        , i.operating_system
        , i.application_id
        , i.geo_country
        INSERT OVERWRITE TABLE agg_rtb_mob_web_ozone PARTITION (dd = '%(dd)s', dh = '%(dh)s')
        SELECT  
        i.device_id as device_id
        , i.operating_system AS operating_system
        , i.geo_country AS geo_c
        , i.width AS width
        , i.height AS height
        , i.site_domain AS site_domain
        , i.content_category_id AS content_category_id
        , COUNT(1) AS imps
        , SUM (c.click) AS clicks
        WHERE supply_type < 2 AND device_id != 0      
        GROUP BY
        i.device_id
        , i.operating_system
        , i.geo_country
        , i.width
        , i.height
        , i.site_domain
        , i.content_category_id;
        """ 


def run4aggs(the_hour):
    next_hour = the_hour + timedelta(hours=1)
    query_dc = {'dh': the_hour.strftime("%Y-%m-%d %H"),
                'dy': the_hour.strftime("%Y"),
                'dm': the_hour.strftime("%Y-%m"),
                'dd': the_hour.strftime("%Y-%m-%d"),
                'dh_next': next_hour.strftime("%Y-%m-%d %H"),
                'members': ",".join(map(str,members_exclude))}

    total_q =  query4aggs % query_dc
    print total_q
    hive_execute(total_q)
    
if __name__ == '__main__':
    yes_date = (datetime.now() - timedelta(hours=24)).date()
    start_hour = datetime(yes_date.year, yes_date.month, yes_date.day)
    for h in range(0,24):
        run4aggs(start_hour + timedelta(hours = h))

