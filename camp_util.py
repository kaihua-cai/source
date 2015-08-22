import numpy as np
import pandas as pd


from framework import Nexus

nexus = Nexus.get_instance()
dbm = nexus.get_db_manager()

bidder_conn = dbm.connection("mysql.bidder")

def filter_camps_by_target(camp_set, target_list):
    camp_prof = bidder_conn.select_dataframe("""  select camp.id as campaign_id,  
                                            camp.campaign_group_id, camp.profile_id,
                                            cg.profile_id as cg_profile_id
                                            from campaign camp 
                                            left join campaign_group cg
                                            on camp.campaign_group_id = cg.id
                                            where  camp.id in (%s) 
                                            """ % ",".join(map(str, camp_set)) )

    profile_set = set(camp_prof.profile_id).union(set(camp_prof.cg_profile_id))
    prof_id_str = ','.join(map(str, map(int, [ id for id in profile_set if id > 0])))
    camp_out = set()
    for key in target_list:
        tab_name = 'profile_target_' + key
        
        prof_q = "select distinct profile_id from {tab_name} where deleted = 0 and profile_id in ({prof_id})"
        prof_q = prof_q.format(tab_name = tab_name, prof_id = prof_id_str)
        prof_set = set(bidder_conn.select_dataframe(prof_q).profile_id)
        if len(prof_set)==0:
            continue
        
        for idx, row in camp_prof.iterrows():
            if row['profile_id'] in prof_set or row['cg_profile_id'] in prof_set:
                camp_out.add(row['campaign_id'])
                
    return camp_out        

def gauge_metrics(buck_list, conv_df, feature_priority, raw_ent):
    imp_agg = conv_df.copy()
    buck_perf = []
    for k in range(len(feature_priority)):
        features = feature_priority[k:]
        feat_camp = features + ["campaign_id"]
        imp_agg = imp_agg.groupby(feat_camp, as_index = False).agg(agg_dict)    
        cb = buck_list[k]
        imp_agg['key'] = zip(*[imp_agg[features[i]] for i, _ in enumerate(features)])
        key_set = set(zip(*[cb[features[i]] for i, _ in enumerate(features)]))
        imp_agg['this_level'] = [ kk in key_set for kk in imp_agg.key ]     
        buck_perf.append(imp_agg[imp_agg['this_level']])
        imp_agg = imp_agg[[ not tl for tl in imp_agg['this_level']]]
    buck_perf.append( imp_agg.groupby("campaign_id").agg(agg_dict))  
    
    print "total var:", sum(map(calculate_var2, buck_perf))
    condi_ent = sum(map(calculate_entropy, buck_perf))
    print "conditional entropy:", condi_ent
    print "information improvement: ", 1 - condi_ent / raw_ent
    
    return buck_perf

