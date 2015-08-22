import numpy as np
import pandas as pd

def verify_cpa_node_old(merged, cpa_li):
    # verify the PIE classifies cpa LI node fail/pass correctly
    # make a 2X2 matrix for CPA line items
    #        should_pass,     should_fail
    # pass  output_def[0,0]   output_def[0,1]
    # fail  output_def[1,0]   output_def[1,1]
    
    # make a 2X2 matrix for CPA line items
    #           should_pass,       should_fail
    # High_cpa  output_perf[0,0]   output_perf[0,1]
    # Low_cpa   output_perf[1,0]   output_perf[1,1]
 
    output_def = np.array([[0,0],[0,0]])
    output_perf = np.array([[0,0],[0,0]])
    
    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id', 'pixel_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]
        money_spent = np.sum(df.booked_rev)
        goal_achieved = 0

        camp_group_id = k[0]
        the_cg = cpa_li[cpa_li.cg_id == camp_group_id]

        if not np.isnan(list(the_cg.pvg_threshold)[0]):
            goal_achieved += np.sum(df.post_view_convs) * list(the_cg.pvg_threshold)[0]
        if not np.isnan(list(the_cg.pcg_threshold)[0]):
            goal_achieved += np.sum(df.post_click_convs) * list(the_cg.pcg_threshold)[0]

        perf_passed = goal_achieved > money_spent
        
        df = df[df.ymdh >= df.qt_start_time_adjusted]
        df = df[df.ymdh <= df.qt_finish_time]

        should_pass = False
        if not np.isnan(list(the_cg.pvg_threshold)[0]):
            if np.sum(df.post_view_convs) > 0:
                should_pass =  True
        if not np.isnan(list(the_cg.pcg_threshold)[0]):
            if np.sum(df.post_click_convs) > 0:
                should_pass =  True

        output_def[1 - int(is_passed), 1 - int(should_pass)] +=1
        output_perf[1 - int(perf_passed), 1 - int(should_pass)] +=1

    return output_def, output_perf

def verify_cpa_node(merged, cpa_li):
    # this new version has a different approach about combining pv_convs and pc_convs
    # verify the PIE classifies cpa LI node fail/pass correctly
    # make a 2X2 matrix for CPA line items
    #        should_pass,     should_fail
    # pass  output_def[0,0]   output_def[0,1]
    # fail  output_def[1,0]   output_def[1,1]

    # make a 2X2 matrix for CPA line items
    #           should_pass,       should_fail
    # High_cpa  output_perf[0,0]   output_perf[0,1]
    # Low_cpa   output_perf[1,0]   output_perf[1,1]

    output_def = np.array([[0,0],[0,0]])
    output_perf = np.array([[0,0],[0,0]])

    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id', 'pixel_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]
        money_spent = np.sum(df.booked_rev)
        

        camp_group_id = k[0]
        the_cg = cpa_li[cpa_li.cg_id == camp_group_id]
        pc_goal_achieved = 0
        pv_goal_achieved = 0
        
        if not np.isnan(list(the_cg.pvg_threshold)[0]):
            pv_goal_achieved += np.sum(df.post_view_convs) * list(the_cg.pvg_threshold)[0]
        if not np.isnan(list(the_cg.pcg_threshold)[0]):
            pc_goal_achieved += np.sum(df.post_click_convs) * list(the_cg.pcg_threshold)[0]

        perf_passed = (pv_goal_achieved > money_spent) or (pc_goal_achieved > money_spent) 

        df = df[df.ymdh >= df.qt_start_time_adjusted]
        df = df[df.ymdh <= df.qt_finish_time]

        should_pass = False
        if not np.isnan(list(the_cg.pvg_threshold)[0]):
            if np.sum(df.post_view_convs) > 0:
                should_pass =  True
        if not np.isnan(list(the_cg.pcg_threshold)[0]):
            if np.sum(df.post_click_convs) > 0:
                should_pass =  True

        output_def[1 - int(is_passed), 1 - int(should_pass)] +=1
        output_perf[1 - int(perf_passed), 1 - int(should_pass)] +=1

    return output_def, output_perf


def verify_ctr_node(merged, ctr_li):
    # verify the PIE classifies CTR LI node fail/pass correctly
    # make a 2X2 matrix for CTR line items
    #        should_pass,     should_fail
    # pass  output_def[0,0]   output_def[0,1]
    # fail  output_def[1,0]   output_def[1,1]
    
    # make a 2X2 matrix for CTR line items
    #           should_pass,       should_fail
    # High_ctr  output_perf[0,0]   output_perf[0,1]
    # Low_ctr   output_perf[1,0]   output_perf[1,1]
 
    output_def = np.array([[0,0],[0,0]])
    output_perf = np.array([[0,0],[0,0]])

    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]
        
        imps_bought = np.sum(df.imps)
        clicks = np.sum(df.clicks)

        camp_group_id = k[0]

        df = df[df.ymdh >= df.qt_start_time_adjusted]
        df = df[df.ymdh <= df.qt_finish_time]

        the_cg = ctr_li[ctr_li.cg_id == camp_group_id]
        ctr = list(the_cg.goal_threshold)[0]
        
        should_pass = np.sum(df.clicks) > 0
        perf_passed =  (clicks > ctr * imps_bought)
        
        output_def[1 - int(is_passed), 1 - int(should_pass)] +=1
        
        output_perf[1 - int(perf_passed), 1 - int(should_pass)] +=1
    return output_def, output_perf


def verify_def_cpa_node(merged, cpa_li):
    # verify the PIE classifies CPA LI node fail/pass correctly
    # return  a 2X2 matrix for cpa line items
    #        should_pass, should_fail
    # pass  output[0,0]   output[0,1]
    # fail  output[1,0]   output[1,1]  

    output = np.array([[0,0],[0,0]])  

    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id', 'pixel_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]

        camp_group_id = k[0]

        df = df[df.ymdh >= df.qt_start_time_adjusted]
        df = df[df.ymdh <= df.qt_finish_time]

        should_pass = False
        the_cg = cpa_li[cpa_li.cg_id == camp_group_id]
        if not np.isnan(list(the_cg.pvg_threshold)[0]):
            if np.sum(df.post_view_convs) > 0:
                should_pass =  True
        if not np.isnan(list(the_cg.pcg_threshold)[0]):
            if np.sum(df.post_click_convs) > 0:
                should_pass =  True

        output[1 - int(is_passed), 1 - int(should_pass)] +=1    
    return output           


def verify_def_ctr_node(merged, ctr_li):
    # verify the PIE classifies CTR LI node fail/pass correctly
    # make a 2X2 matrix for CTR line items
    #        should_pass, should_fail
    # pass  output[0,0]   output[0,1]
    # fail  output[1,0]   output[1,1]  

    output = np.array([[0,0],[0,0]])  

    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]

        camp_group_id = k[0]

        df = df[df.ymdh >= df.qt_start_time_adjusted]
        df = df[df.ymdh <= df.qt_finish_time]

        the_cg = ctr_li[ctr_li.cg_id == camp_group_id]
        should_pass = np.sum(df.clicks) > 0 

        output[1 - int(is_passed), 1 - int(should_pass)] +=1    
    return output


def calc_perf_cpa_node(merged,cpa_li):
    # calculate the cpa of node, and identify if it is pass/fail and should_pass/should_fail
    # make a 2X2 matrix for cpa line items
    #        should_pass, should_fail
    # pass  output[0,0]   output[0,1]
    # fail  output[1,0]   output[1,1]  

    output = np.array([[0,0],[0,0]])  

    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id', 'pixel_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]
	camp_group_id = k[0] 
        money_spent = np.sum(df.booked_rev)  
        goal_achieved = 0

        the_cg = cpa_li[cpa_li.cg_id == camp_group_id]
        if not np.isnan(list(the_cg.pvg_threshold)[0]):
            goal_achieved += np.sum(df.post_view_convs) * list(the_cg.pvg_threshold)[0]
        if not np.isnan(list(the_cg.pcg_threshold)[0]):    
            goal_achieved += np.sum(df.post_click_convs) * list(the_cg.pcg_threshold)[0]

        should_pass = goal_achieved > money_spent
        output[1 - int(is_passed), 1 - int(should_pass)] +=1

    return output    


def calc_perf_ctr_node(merged, ctr_li):
    # calculate the ctr of node, and identify if it is pass/fail and should_pass/should_fail
    # make a 2X2 matrix for ctr line items
    #        should_pass, should_fail
    # pass  output[0,0]   output[0,1]
    # fail  output[1,0]   output[1,1]  

    output = np.array([[0,0],[0,0]])  

    grouped = merged.groupby(['campaign_group_id', 'country_group_id', 'tag_id', 'creative_size_bucket_id'])
    for k, df in grouped:
        is_passed = list(df.is_passed)[0]
        camp_group_id = k[0]
        imps_bought = np.sum(df.imps)  
        clicks = np.sum(df.clicks)

        the_cg = ctr_li[ctr_li.cg_id == camp_group_id]
        ctr = list(the_cg.goal_threshold)[0]

        should_pass = (clicks > ctr * imps_bought)
        output[1 - int(is_passed), 1 - int(should_pass)] +=1

    return output   

 
