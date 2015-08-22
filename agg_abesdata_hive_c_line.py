import pandas as pd
import numpy as np
import scipy
import random
import math
import pdb
import datetime
import logging
import os
import sys
import subprocess
from link import lnk


vertica = lnk.dbs.prod.internal_vertica
api  = lnk.dbs.prod.my_api
common  = lnk.dbs.prod.my_commom
bidder  = lnk.dbs.prod.my_bidder
opt = lnk.dbs.prod.my_optimization
#hive = lnk.dbs.prod.hive

#dt = '2014-01-21 08'


def setup_logging(fLog, name='thislog', debug = False):
	"Setup standard file logging "
	filenameLog = fLog
	isDebug = debug
	logname = name

	log = logging.getLogger(logname)
	if isDebug:
		logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s:%(levelname)s:%(message)s',
                            filename=filenameLog,
                            filemode='a')
	else:
		logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s:%(levelname)s:%(message)s',
                            filename=filenameLog,
                            filemode='a')

	return log


def hive_execute(query):
	query = query.replace('\n', ' ')
	query = query.replace('\t', ' ')
	query = query + '\n'
	fname = str(random.random())[2:]
	with open(fname+'.sql', 'w') as query_file:
		query_file.write(query)
	command_for_beeline = "hive -f "+ fname+'.sql'
	return_code = subprocess.call(command_for_beeline, shell = True)
	os.remove(fname+'.sql')
	return(return_code)


def hive_select_dataframe(query):
	query = 'set hive.cli.print.header=true; '+query
	query = query.replace('\n', ' ')
	query = query + '\n'
	fname = str(random.random())[2:]
	with open(fname+'.sql', 'w') as query_file:
		query_file.write(query)
	command_for_beeline = "hive -f "+ fname+'.sql > ' +fname+".out"
	subprocess.call(command_for_beeline, shell = True)
	data_frame = pd.read_csv(fname+'.out',sep = '\t', header=0)#,skipfooter=1, 
	os.remove(fname+'.sql')
	os.remove(fname+'.out')
	return(data_frame)


def pull_and_write_data(dt,lock_file_name):
	dy = dt[0:4]
	dm = dt[0:7]
	dd = dt[0:10]
	dh = dt
	next_hour = datetime.datetime.strptime(dt, "%Y-%m-%d %H") + datetime.timedelta(hours = 1)
	dt_plus = next_hour.strftime("%Y-%m-%d %H")
	
	main_query = """SELECT i.operating_system AS operating_system, i.supply_type AS supply_type, 
	i.content_category_id AS content_category_id,i.site_domain AS site_domain,
	i.creative_id AS creative_id, i.geo_country AS geo_c,
	i.width AS width, i.height AS height, 
	COUNT(*) AS imps, 
	SUM(CASE WHEN c.click IS NULL THEN 0 ELSE c.click END) AS clicks, 
	b.campaign_id AS campaign_id,
	i.buyer_member_id AS buyer_member_id
	FROM
	(
	SELECT dh, auction_id_64, operating_system, supply_type, content_category_id, site_domain, 
	creative_id, geo_country,width,height, buyer_member_id
	FROM log_opt_imps
	WHERE dh = '%s'
	AND seller_member_id != buyer_member_id
	AND content_category_id > 0 
	) i
	JOIN
	(
	SELECT auction_id_64,campaign_id
	FROM log_opt_bids
	WHERE dh = '%s'
	AND payment_type = 0
	AND campaign_id > 0 
	) b
	ON b.auction_id_64 = i.auction_id_64
	LEFT OUTER JOIN
	(
	SELECT auction_id_64, 1 as click
	FROM log_opt_clicks
	WHERE (dh = '%s' OR dh = '%s' )
	) c
	ON b.auction_id_64 = c.auction_id_64
	GROUP BY i.dh, i.operating_system, i.supply_type, i.content_category_id,i.site_domain,
	i.creative_id, i.geo_country,i.width, i.height , b.campaign_id, i.buyer_member_id;""" %(dt,dt,dt,dt_plus)
	
	table_update_command = """INSERT OVERWRITE TABLE abesdata\n PARTITION(dy = '%s', dm = '%s', dd = '%s', dh = '%s') \n""" %(dy, dm, dd, dh) +main_query
	#print table_update_command
	
	logging.info('started agging %s',dt)
	try:
		return_code =  hive_execute(table_update_command)
		if return_code != 0:
			logging.info('failed agging abesdata for %s',dt)
			os.remove(lock_file_name)
			sys.exit()
	except: 
		logging.info('failed agging abesdata for %s',dt)
		os.remove(lock_file_name)
		sys.exit()
	logging.info('finished agging %s',dt)
	return(dt)

def get_and_process_dh(table_name):
	#part = hive.select_dataframe("show partitions " + table_name)
	#part = bline_select_dataframe("show partitions " + table_name + ";")
	part = hive_select_dataframe("show partitions " + table_name + ";")
	part['partition'] = [part.ix[ii,'partition'][-13:] for ii in part.index]
	part = part.drop_duplicates()
	part['date_form'] = [ datetime.datetime.strptime(part.ix[ii,'partition'], "%Y-%m-%d %H") for ii in part.index]
	return(part)

def get_rid_of_parts_with_missing_data(ipart,bpart,cpart):
	ipart = pd.merge(ipart, bpart, how = 'inner', on = ['partition', 'date_form'])
	ipart = pd.merge(ipart, cpart, how = 'inner', on = ['partition', 'date_form'])
	return(ipart)

def get_rid_of_parts_already_agged(ipart,apart):
	dh_set = set(ipart.partition) - set(apart.partition)
	dh_frame = pd.DataFrame(data = list(dh_set), columns = ['partition'])
	ipart = pd.merge(ipart,dh_frame, how = 'inner', on = 'partition')
	return(ipart)

def add_follow_hour_and_get_rid_of_parts_missing_follow_click_hour(ipart,cpart):
	ipart['follow_hour'] = [ ipart.ix[ii,'date_form'] + datetime.timedelta(hours = 1) for ii in ipart.index]
	ipart = pd.merge(ipart,cpart, how = 'inner', left_on = 'follow_hour', right_on = 'date_form')
	del ipart['date_form_y']
	del ipart['partition_y']
	ipart = ipart.rename(columns = {'partition_x': 'partition' , 'date_form_x': 'date_form'})
	return(ipart)

def query_hive_to_make_sure_parts_are_populated(ipart):
	ipart['has_all_data'] = 0
	for ii in ipart.index:
		time1 = ipart.ix[ii, 'partition']
		time2 = ipart.ix[ii, 'follow_hour'].strftime("%Y-%m-%d %H")
		if hive_check_query('log_opt_imps', time1) == 0:
			ipart.ix[ii, 'has_all_data'] = 0
		elif  hive_check_query('log_opt_bids', time1) == 0:
			ipart.ix[ii, 'has_all_data'] = 0
		elif  hive_check_query('log_opt_clicks', time1) == 0:
			ipart.ix[ii, 'has_all_data'] = 0
		elif  hive_check_query('log_opt_clicks', time2) == 0:
			ipart.ix[ii, 'has_all_data'] = 0
		else:
			ipart.ix[ii, 'has_all_data'] = 1
	ipart = ipart[ipart.has_all_data == 1]
	return(ipart)

def hive_check_query(table_name, dt):
	query = "select * from %s where dh = '%s' limit 1;" %(table_name,dt)
	print query
	data_is = len(hive_select_dataframe(query))
	return(data_is)

def delete_old_data(days_lookback):
	dh  = (datetime.datetime.utcnow() - datetime.timedelta(days = days_lookback)).strftime("%Y-%m-%d %H")
	query = """ALTER TABLE abesdata DROP IF EXISTS PARTITION(dh < '%s')""" %dh
	logging.info('started delete of data older than %s',dh)
	hive.execute(query )	
	logging.info('finished delete of data older than %s',dh)
	return()

def main():
	lock_file_name = '/home/agreenstein/pythoncode/src/iqt/data_jobs/hive_agg_job.lck'
	setup_logging('/home/agreenstein/pythoncode/src/iqt/data_jobs/abes_hive_data_agg.log', name='thislog', debug = True)
	days_abesdata = 8
	if os.path.isfile(lock_file_name) == 0:
		with open(lock_file_name, "w") as fh:
			pass
		logging.info('job started and lock is on')
		imp_part = get_and_process_dh('log_opt_imps')
		imp_part = imp_part[imp_part.date_form > datetime.datetime.utcnow() - datetime.timedelta(days = days_abesdata)]
		bid_part = get_and_process_dh('log_opt_bids')
		click_part = get_and_process_dh('log_opt_clicks')
		imp_part = get_rid_of_parts_with_missing_data(imp_part, bid_part, click_part)
		abe_part = get_and_process_dh('abesdata')
		imp_part = get_rid_of_parts_already_agged(imp_part,abe_part)
		imp_part = add_follow_hour_and_get_rid_of_parts_missing_follow_click_hour(imp_part,click_part)
		imp_part = query_hive_to_make_sure_parts_are_populated(imp_part)
		for ii in imp_part.index:
			pull_and_write_data(imp_part.ix[ii,'partition'],lock_file_name)
		#delete_old_data(days_abesdata)
		os.remove(lock_file_name)
		logging.info('job is done and lock is off')
	else:
		logging.info('previous_job_still_running')

if __name__ == "__main__":
    main()

