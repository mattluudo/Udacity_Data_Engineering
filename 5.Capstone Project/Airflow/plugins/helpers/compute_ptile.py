import os
import time
import numpy as np
import pandas as pd


def weighted_percentile(data, percents, weights=None):
    """
    Calculates QCI based throughput percentiles and traffic using weighted_percentile function

    :param data: <DataFrame> Dataframe of dataset
    :param percents: <int/tuple> Percentile to be calculated
    :param weights: <int/tuple> Weighting for the dataset
    """
    if weights is None:
        return np.percentile(data, percents)
    ind = np.argsort(data)
    d = data[ind]
    w = weights[ind]
    p = 1.*w.cumsum()/w.sum()*100
    y = np.interp(percents, p, d)
    return y


def get_kpi(df, grp, ptiles, qci='QCI 6+7+8+9', vendor=False):
    """
    Calculates QCI based throughput percentiles and traffic using weighted_percentile function

    :param df: <DataFrame> Dataframe of dataset
    :param grp: <list/tuple> Groupby values
    :param ptiles: <tuple> Tuple of percentiles to calculate throughput
    :param qci: <int/str> QCI 6-9 or All(default)
    :param vendor: <boolean> Include or exclude vendor in grouping
    :returns: <list> Single list of grp with KPIs appended
    """
    if qci == 'QCI 6+7+8+9':
        qci_string = qci
        traf = df['DL_DRB_TP_NUM'].sum()
        tp = df['DL_DRB_TP_NUM'].sum() / df['DL_DRB_TP_DEN'].sum()
        tp_5p = weighted_percentile(df['DL_DRB_TP'].to_numpy(), ptiles, df['ERAB_SETUP_SUCC'].to_numpy())

    else:
        qci_string = "QCI {}".format(qci)
        qci = int(qci)
        traf = df['QCI{}_DL_TPUT_NUM'.format(qci)].sum()
        tp = df['QCI{}_DL_TPUT_NUM'.format(qci)].sum() / df['QCI{}_DL_TPUT_DEN'.format(qci)].sum()
        tp_5p = weighted_percentile(df['DL_DRB_QCI{}_TP'.format(qci)].to_numpy(), ptiles,
                                    df['QCI{}_ERAB_SETUP_SUCC'.format(qci)].to_numpy())

    if vendor:
        return [*list(grp), qci_string, traf, tp, *list(tp_5p)]
    else:
        vendor = df['VENDOR'].mode()[0]
        return [*list(grp), vendor, qci_string, traf, tp, *list(tp_5p)]


def add_row(df, ls):
    """
    Given a dataframe and a list, append the list as a new row to the dataframe.

    :param df: <DataFrame> The original dataframe
    :param ls: <list> The new row to be added
    :return: <DataFrame> The dataframe with the newly appended row
    """
    numEl = len(ls)
    newRow = pd.DataFrame(np.array(ls).reshape(1, numEl), columns=list(df.columns))
    df = df.append(newRow, ignore_index=True)
    return df


def compute_ptile(in_file, out_file):
    result_col = [
        'DATETIME',
        'MARKET',
        'SUBMARKET',
        'VENDOR',
        'QCI',
        'TRAFFIC',
        'DL Throughput Mean',
        'DL Throughput 5th Percentile',
        'DL Throughput 10th Percentile',
        'DL Throughput 15th Percentile',
        'DL Throughput 20th Percentile'
    ]

    id_col = [
        'DATETIME',
        'MARKET',
        'SUBMARKET',
        'VENDOR',
        'QCI'
    ]

    result_df = pd.DataFrame(columns=result_col)
    trunc_df = pd.read_csv(in_file)

    trunc_df['DL_DRB_TP_NUM'] = trunc_df['QCI6_DL_TPUT_NUM'] + trunc_df['QCI7_DL_TPUT_NUM'] \
                                + trunc_df['QCI8_DL_TPUT_NUM'] + trunc_df['QCI9_DL_TPUT_NUM']

    trunc_df['DL_DRB_TP_DEN'] = trunc_df['QCI6_DL_TPUT_DEN'] + trunc_df['QCI7_DL_TPUT_DEN'] \
                                + trunc_df['QCI8_DL_TPUT_DEN'] + trunc_df['QCI9_DL_TPUT_DEN']

    trunc_df['ERAB_SETUP_SUCC'] = trunc_df['QCI6_ERAB_SETUP_SUCC'] + trunc_df['QCI7_ERAB_SETUP_SUCC'] \
                                  + trunc_df['QCI8_ERAB_SETUP_SUCC'] + trunc_df['QCI9_ERAB_SETUP_SUCC']

    # Submarket
    ptiles = (5, 10, 15, 20)
    for grp, df in trunc_df.groupby(['DATETIME', 'MARKET', 'SUBMARKET']):
        # print(grp, type(grp), '\n')
        result_df = add_row(result_df, get_kpi(df, grp, ptiles))
        for qci in range(6, 10):
            result_df = add_row(result_df, get_kpi(df, grp, ptiles, qci))

    # Market
    market_df = trunc_df.copy()
    market_df['SUBMARKET'] = 'ALL'
    ptiles = (5, 10, 15, 20)
    for grp, df in market_df.groupby(['DATETIME', 'MARKET', 'SUBMARKET']):
        result_df = add_row(result_df, get_kpi(df, grp, ptiles))
        for qci in range(6, 10):
            result_df = add_row(result_df, get_kpi(df, grp, ptiles, qci))

    # Vendor
    vendor_df = trunc_df.copy()
    vendor_df['SUBMARKET'] = 'ALL'
    vendor_df['MARKET'] = 'ALL'
    ptiles = (5, 10, 15, 20)
    for grp, df in vendor_df.groupby(['DATETIME', 'MARKET', 'SUBMARKET', 'VENDOR']):
        result_df = add_row(result_df, get_kpi(df, grp, ptiles, vendor=True))
        for qci in range(6, 10):
            result_df = add_row(result_df, get_kpi(df, grp, ptiles, qci, vendor=True))

    # Nation
    nation_df = trunc_df.copy()
    nation_df['SUBMARKET'] = 'NATIONAL'
    nation_df['MARKET'] = 'NATIONAL'
    nation_df['VENDOR'] = 'NATIONAL'
    ptiles = (5, 10, 15, 20)
    for grp, df in nation_df.groupby(['DATETIME', 'MARKET', 'SUBMARKET']):
        result_df = add_row(result_df, get_kpi(df, grp, ptiles))
        for qci in range(6, 10):
            result_df = add_row(result_df, get_kpi(df, grp, ptiles, qci))

    result_df = result_df[~result_df['SUBMARKET'].str.contains('-PART')]
    result_df = result_df[~result_df['MARKET'].str.contains('Unknown')]
    melt_df = result_df.melt(id_vars=id_col, var_name='KPI', value_name='Value')
    melt_df.to_csv(out_file, float_format="%20.3f")
