import pandas as pd
import numpy as np

def calc_impurity(comps):
    return 1 - np.sum([x*x for x in comps])

def calc_feature_impurity(conv_df, feature):
    agg1 = conv_df.groupby(feature, as_index =False).agg({"imps": np.sum})
    t_imps = np.sum(agg1.imps)*1.0
    return calc_impurity([ x / t_imps for x in agg1.imps])

def calc_weighted_impurity(conv_df, feature, feature2 = ['campaign_id']):
    if type(feature) != list:
        feature = [feature]
    if type(feature2) != list:
        feature2 = [feature2]
    
    agg1 = conv_df.groupby(feature + feature2, as_index =False).agg({"imps": np.sum})
    
    ii_list = []
    grouped = agg1.groupby(feature)
    for _ , dd in grouped:
        t_imps = np.sum(dd.imps) * 1.0
        impu =calc_impurity([ x / t_imps for x in dd.imps])
        ii_list.append(tuple([t_imps, impu]))
    
    return np.sum([ imim[0]* imim[1] for imim in ii_list ]) /  np.sum([ imim[0] for imim in ii_list ])

def calc_mim(conv_df, feature_list):
    # [u'site', u'cont_id', u'supply_type', u'size_buck', u'campaign_id', u'cgid', u'os',u'cont_gp']
    nf = len(feature_list)
    mim = np.zeros(shape=(nf+1,nf))

    for i2 in range(nf):
        for i1 in range(nf):
            if i1 != i2:
                mim[i1,i2] = round(calc_weighted_impurity(conv_df, feature_list[i1], feature_list[i2]),3)
        mim[nf,i2] = round(calc_feature_impurity(conv_df, feature_list[i2]) ,3)
    return mim
