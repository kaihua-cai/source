#  write hive data to local files
from datetime import datetime, timedelta
#import numpy as np
from pandas import DataFrame
from fraud_util import query_reporting_db, query_api_db
import pandas as pd
import subprocess
import pickle
import os
import argparse

def hive_select_dataframe(query):
    query = 'set hive.cli.print.header=true; '+query
    query = query.replace('\n', ' ')
    query = query + '\n'
    fname = str(random.random())[2:]
    with open(fname+'.sql', 'w') as query_file:
        query_file.write(query)
    command_for_beeline = "hive -f "+ fname+'.sql > ' +fname+".out"
    subprocess.call(command_for_beeline, shell = True)
    data_frame = pd.read_csv(fname+'.out',sep = '\t', header=0) #,skipfooter=1, 
    os.remove(fname+'.sql')
    os.remove(fname+'.out')
    return(data_frame)

        
def write_query(out_path, base_query, dd):
    for hh in range(24):
        hh_str = str(hh+100)[-2:]
        query= base_query.format(dd = dd, dh = hh_str)
        with open(os.path.join(out_path, hh_str ), 'w') as query_file:
            query_file.write(query)

def save_hive_data(q_path, output_file):
    hive_command = "hive -f "+ q_path +' > ' + output_file
    subprocess.call(hive_command, shell = True)



def download_hive_data(base_path, query, dd):
    os.mkdir(base_path)
    os.mkdir(os.path.join(base_path, 'query'))
    os.mkdir(os.path.join(base_path, 'from_hive'))
    write_query( os.path.join(base_path, 'query'), query, dd)
    
    for hh in range(0,24):
        hh_str = str(hh+100)[-2:]
        q_path = os.path.join(base_path, "query", hh_str)
        output_file = os.path.join(base_path, "from_hive", hh_str)
        print q_path
        save_hive_data(q_path, output_file)
    

def get_args():
    """
    Returns the command line argumenst which allow you to override the date
    """
    parser = argparse.ArgumentParser(description='Load the agg data into local file')
    parser.add_argument('--date', type=str, help='date to run')
    return parser.parse_args()

 
if __name__ == '__main__':
    _args = get_args()
    dd = _args.date
    if not dd:
        dd = str( (datetime.now() - timedelta(hours=24)).date())
    print dd

    # mob_web 
    query= '''
    select site_domain, content_category_id, 
      device_id, width, height, geo_country, operating_system,             
       sum(imps) as imps, sum(clicks) as clicks           
           from agg_rtb_mob_web_ozone               
    where dh = '{dd} {dh}'  
    group by site_domain, content_category_id, device_id, width, height, geo_country,operating_system; 
    '''
    base_path = '/home/kcai/data/hive/{dd}_mob_web/'.format(dd = dd)
    #download_hive_data(base_path, query, dd)
    
    # running_case == 'web_clicks':    
    query= '''
    select site_domain, content_category_id, 
      supply_type, width, height, geo_country, operating_system,             
       sum(imps) as imps, sum(clicks) as clicks           
           from agg_rtb_ozone_os               
    where dh = '{dd} {dh}'  
    group by site_domain, content_category_id,supply_type, width, height, geo_country,operating_system; 
    '''
    base_path = '/home/kcai/data/hive/{dd}_web/'.format(dd = dd)
    download_hive_data(base_path, query, dd)
    
    #running_case == 'mob_app':
    query= '''
    select  
       device_id, operating_system, geo_country, application_id,                
       sum(imps) as imps, sum(clicks) as clicks           
    from agg_rtb_mob_app_ozone               
    where dh = '{dd} {dh}'  
    group by device_id, operating_system, geo_country, application_id; 
    '''
    base_path = '/home/kcai/data/hive/{dd}_mob/'.format(dd = dd)
    #download_hive_data(base_path, query, dd)

    #running_case == 'fb_clicks':    
    query= '''
    select  operating_system,  width, height, geo_country,             
       sum(imps) as imps, sum(clicks) as clicks           
           from agg_rtb_ozone_fb               
    where dh = '{dd} {dh}'  
    group by width, height, geo_country,operating_system; 
    '''
    base_path = '/home/kcai/data/hive/{dd}_fb/'.format(dd = dd)
    #download_hive_data(base_path, query, dd)
