import pandas as pd
import os
import numpy as np
import math
import matplotlib.pyplot as plt

agg_dict = {"imps":np.sum, "clicks": np.sum}

def load_one_day_data(data_path, agg_cols,  perf_col = 'clicks'):
    if not os.path.isfile(os.path.join(data_path, 'agg.txt')) :
        "load hive data hour by hour and then aggregate them into one file "
        hour_data_path = os.path.join(data_path, "from_hive")
        agg_dict = {"imps":np.sum, perf_col: np.sum}
        day_data = pd.DataFrame()
        for filen in sorted(os.listdir(hour_data_path)):
            print filen
            day_data = day_data.append(pd.read_csv(os.path.join(hour_data_path, filen), sep = "\t") )
            day_data = day_data.groupby(agg_cols, as_index = False).agg(agg_dict)
            print day_data.shape
        
        day_data[perf_col] = [ 0 if np.isnan(x) else x for x in day_data[perf_col]]
        day_data.to_csv(os.path.join(data_path,"agg.txt"), index = False)  
	return day_data
    else:
        return  pd.read_csv(os.path.join(data_path,"agg.txt")) 



# calculate the variance of the buckets
def calculate_var (output):
    # each row of the data is one bucket
    return np.sum(output.clicks * (1 - output.clicks * 1.0 / output.imps))

def calculate_var2 (output):
    # each row of the data is one bucket
    return - np.sum(output.clicks * output.clicks * 1.0 / output.imps)

def calcContiM(df, key, key_type, torfKey, wkey= "imps"):
# calculate contigent matrix    
    catList = list(set(df[key]))
    outMat = np.zeros((2, len(catList)))
    for j in range(len(catList)):
        outMat[0,j] = sum(df.ix[ df[torfKey] & (df[key] == catList[j]), wkey])
        outMat[1,j] = sum(df.ix[ (df[torfKey] == False) & (df[key] == catList[j]), wkey])

    return outMat

# n1 imps
# y1 clicks  
# we should minimize entropy
def ent_core(n1,y1):
    if y1==0 or y1 == n1:
        return 0 
    return - ((n1-y1)* math.log((n1-y1) * 1.0 /n1) + y1 * math.log(y1 * 1.0/n1))  

def calculate_entropy(output):
    entropy = 0
    for ind, row in output.iterrows():
        entropy += ent_core(row['imps'], row['clicks'])
    return int(entropy)    


def print_info(data, agg_col, draw = True):
    output = data.groupby(agg_col).agg(agg_dict)
    output = output[output.imps>= 1]

    print agg_col, output.shape
    t_var = calculate_var(output)
    print "variance:", t_var
    var_2 = calculate_var2(output)
    print "variance_2: ", var_2
    entro = calculate_entropy(output)
    print "entropy: ", entro
    
    if draw:
        weights = [100.0/len(output)] * len(output)

        title_part = (agg_col if type(agg_col) ==str else "_".join(agg_col))
        plt.figure()
        plt.title(title_part + ": log_ctr")
        plt.hist([ math.log10(x+ 1e-6) for x in output.clicks * 1.0 / output.imps], bins =30, weights = weights)

        plt.figure()
        plt.hist([ math.log10(x) for x in  output.imps], bins =30, weights = weights)
        plt.title( title_part + ": log_imps")
        
    return (agg_col, output.shape[0], t_var, var_2, entro)    

def summarize(buck_list):
    total_var = sum(map(calculate_var2 , buck_list ))
    total_entropy = sum(map(calculate_entropy , buck_list ))
    num_buck = sum(map(len , buck_list )) 
    imps = sum(map(lambda p: sum(p.imps) , buck_list ))
    clicks = sum(map(lambda p: sum(p.clicks) , buck_list ))
    raw_ent = ent_core(imps, clicks)
    return total_var, 1 - total_entropy/raw_ent, num_buck, imps, clicks


def analyze(imp_df, feature_priority, imp_th = 1e6):
    imp_agg = imp_df.groupby(feature_priority, as_index=False).agg(agg_dict)
    buck_list = []
    for k in range(len(feature_priority) -1 ):
        buck_list.append( imp_agg[imp_agg.imps >= imp_th])
        remains = imp_agg[imp_agg.imps < imp_th]
        imp_agg = remains.groupby(feature_priority[k+1:], as_index = False).agg(agg_dict)
    else:
        buck_list.append(imp_agg[imp_agg.imps >= imp_th])
        remains = imp_agg[imp_agg.imps < imp_th]
        buck_list.append(pd.DataFrame({'imps': [np.sum(remains.imps) + 1], 'clicks': np.sum(remains.clicks)}))
    return buck_list

def calc_buck_perf(buck_list, imp_df, feature_priority):
    imp_agg = imp_df.copy()
    buck_perf = []
    for k in range(len(feature_priority)):
        print k
        features = feature_priority[k:]
        imp_agg = imp_agg.groupby(features, as_index = False).agg(agg_dict)    
        cb = buck_list[k]
        imp_agg['key'] = zip(*[ imp_agg[features[i]] for i, _ in enumerate(features)])
        cb['key'] = zip(*[cb[features[i]] for i, _ in enumerate(features)])
        key_set = set(zip(*[cb[features[i]] for i, _ in enumerate(features)]))
        imp_agg['this_level'] = [ kk in key_set for kk in imp_agg.key ]     
        buck_perf.append(imp_agg[imp_agg['this_level']])
        imp_agg = imp_agg[[ not tl for tl in imp_agg['this_level']]]
        
    return buck_perf   

def update_ozone(base_ozones, imp_df, feature_priority, delete_th = 0.5e6, grow_th = 1.5e6):
    remains = imp_df.copy()
    for k in range(len(feature_priority)):
        features = feature_priority[k:]
        remains  = remains.groupby(features, as_index=False).agg(agg_dict)
        remains['key'] = zip(*[remains[features[i]] for i, _ in enumerate(features)])
        #ozone_key_set = set(base_ozones[k].key)
        ozone_key_set = set(zip(*[base_ozones[k][features[i]] for i, _ in enumerate(features)]))
        current_level = remains[ [x in ozone_key_set for x in remains.key] ]
        current_remains = remains[[ not x in ozone_key_set for x in remains.key] ]
        print features
        if sum(current_level.imps < 0.5e6)>0:
            print "Ozones to be deleted:"
            print current_level[current_level.imps < delete_th]
        if sum(current_remains.imps > grow_th)>0:
            print "Ozones to grow:"
            print current_remains[current_remains.imps > grow_th]
        remains = current_remains[current_remains.imps <= grow_th]
