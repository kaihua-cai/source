import numpy as np
import pandas as pd
import logging

import sys
sys.path.append ("/home/kcai/cgit/user/agreenstein/agreen_codes")

from load_data_hadoop_hive_c_line import *
from load_data_hadoop_util import *

from framework import Nexus

nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()
bidder_conn = dbm.connection("mysql.bidder")
opt_conn = dbm.connection("mysql.optimization")

cur_time = datetime.datetime.utcnow()

query = """
select  cg.id as campaign_group_id,
        c.id as campaign_id, 
        p.id as pixel_id, 
        p.trigger_type, 
        cgp.post_view_revenue, 
        cgp.post_click_revenue,
        cg.currency, cview.conversion_rate
from pixel p, campaign_group_pixel cgp, campaign_group cg, campaign c, currency_view cview
where   p.deleted = 0 
    and p.state = 'active'
    and cgp.deleted = 0
    and cgp.state = 'active'
    and c.deleted = 0
    and c.state = 'active'
    and cg.deleted = 0
    and cg.state = 'active'
    and p.id = cgp.pixel_id
    and cg.id = cgp.campaign_group_id
    and c.campaign_group_id = cg.id
    and cg.currency = cview.code
"""

cpg_df = bidder_conn.select_dataframe(query)

# split each row of trigger_type = hybrid into two rows
hybr_trigger = cpg_df[cpg_df.trigger_type=='hybrid'] 
hybr_trigger['trigger_type'] = 'click'

cpg_df.ix[cpg_df.trigger_type == 'hybrid', 'trigger_type'] = 'view'

split_df = pd.concat([cpg_df, hybr_trigger]) 
split_df.rename(columns = {'post_click_revenue':'revenue'}, inplace=True)
split_df.ix[split_df.trigger_type == 'view', 'revenue'] = \
split_df.ix[split_df.trigger_type == 'view', 'post_view_revenue'] 
del split_df['post_view_revenue']


# categorizer_current
cat_query = """
SELECT campaign_id, pixel_id, type, 
    CASE value_type
        WHEN 'pc_cpa' then 'click'
        WHEN 'pv_cpa' then 'view'
    END as trigger_type,
     value
FROM   categorizer_current 
WHERE  type in ('advertiser_goal', 'rtb_cpm_payout')
and value_type in ('pc_cpa', 'pv_cpa')
"""
cat_df = opt_conn.select_dataframe(cat_query)
cat_df['adv_goal'] = np.NaN
cat_df['rtb_cpm_payout'] = np.NaN
cat_df.ix[cat_df.type =='rtb_cpm_payout', 'rtb_cpm_payout'] = \
        cat_df.ix[ cat_df.type=='rtb_cpm_payout' , 'value']
cat_df.ix[cat_df.type =='advertiser_goal', 'adv_goal'] = \
        cat_df.ix[ cat_df.type=='advertiser_goal' , 'value']
del cat_df['type']
del cat_df['value']

cat_df =  cat_df.groupby(['campaign_id', 
                          'pixel_id', 
                          'trigger_type'], as_index = False).agg({'adv_goal':sum, 
                                                                  'rtb_cpm_payout': sum})
                        
# merge the campaign data with categorizer_current
merged = pd.merge(split_df, cat_df, how='left', on = ['campaign_id', 'pixel_id', 'trigger_type'])
merged['revenue'] = merged['revenue'] / merged['conversion_rate']
merged['adv_goal'] = merged['adv_goal'] / merged['conversion_rate']
merged['rtb_cpm_payout'] = merged['rtb_cpm_payout'] / merged['conversion_rate']


cg_goal_q = """select campaign_group_id,
                pixel_id,
                post_view_goal_target,
                post_click_goal_target
                from campaign_group_goal
                where state = 'active'
                """

cg_goal_df = bidder_conn.select_dataframe(cg_goal_q)

cg_goal_view = cg_goal_df.ix[:,['campaign_group_id', 'pixel_id', 'post_view_goal_target']]
cg_goal_view['trigger_type']= 'view'
cg_goal_view.rename(columns={'post_view_goal_target': 'goal_target'}, inplace = True)

cg_goal_click = cg_goal_df.ix[:,['campaign_group_id', 'pixel_id', 'post_click_goal_target']]
cg_goal_click['trigger_type']= 'click'
cg_goal_click.rename(columns={'post_click_goal_target': 'goal_target'}, inplace = True)

cg_goal_target = pd.concat( [cg_goal_view, cg_goal_click])
cg_goal_target = cg_goal_target[[not np.isnan(xx) for xx  in cg_goal_target.goal_target ]]

merged = pd.merge(merged, cg_goal_target, 
                  how='left', on = ['campaign_group_id', 'pixel_id', 'trigger_type'])

merged['goal_target'] = merged['goal_target'] / merged['conversion_rate']


merged = merged.ix[:,['campaign_group_id', 'campaign_id', 'pixel_id', 'trigger_type', 
                      'revenue', 'adv_goal', 'rtb_cpm_payout', 'goal_target']]

date_strings = get_strings_for_hdfs_file_managment(cur_time)

logging.info('writing merged campaing pixel data to local file for ' + date_strings['year_month_day_hour'])

local_f_name = "/home/kcai/data/tmp/meta_campaign_pixel.csv"
merged.to_csv(local_f_name, sep='\t', header=False, index=False)

# set up hive table
"""
CREATE EXTERNAL TABLE meta_campaign_pixel
( campaign_group_id int, 
  campaign_id int,
  pixel_id int,
  event_type string,
  revenue float,
  advertiser_goal float,
  rtb_cpm_payout float,
  goal_target float  
)
PARTITIONED BY (
    dy string, 
    dm string, 
    dd string, 
    dh string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'    
STORED AS TEXTFILE
LOCATION '/user/kcai/prod/meta_campaign_pixel';    
"""
# drop a partition file for hive table;
''' ALTER TABLE meta_campaign_pixel  DROP PARTITION(dd='2015-03-12'); '''


# write data to hadoop

logging.info('create hdfs folder for ' + date_strings['year_month_day_hour'])

create_hdfs_folder('/user/kcai/prod/meta_campaign_pixel', date_strings)  

folder_name = "/user/kcai/prod/meta_campaign_pixel/dy={year}/dm={year_month}/dd={year_month_day}/dh={year_month_day}{line_spacer} {hour}".format( 
                                                                            year=date_strings['year'],
                                                                            year_month=date_strings['year_month'],
                                                                            year_month_day=date_strings['year_month_day'],
                                                                            hour=date_strings['hour'],
                                                                            line_spacer = '\\'+'\\')
logging.info('push data to hdfs for ' + date_strings['year_month_day_hour'])

push_data_to_hadoop(local_f_name, folder_name, 'dump.txt') 

logging.info('add partition to table meta_campaign_pixel for ' + date_strings['year_month_day_hour'])
add_data_to_hadoop_table('meta_campaign_pixel', date_strings)
logging.info('finished loading meta_campaign_pixel for ' + date_strings['year_month_day_hour'])






