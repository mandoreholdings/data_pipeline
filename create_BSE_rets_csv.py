import os
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine


def read_bse_static_map():
    #constituents of the BSE as of May 2019
    bse_map = pd.read_csv(os.path.join('..','rawdata', 'BSE_NSE_map_staticMay2019', 'bse500.csv'))

    #Check for nulls
    if bse_map.co_code.isnull().sum():
        print('missing mapped co_code for isin: {}'.format(','.join(
            bse_map[bse_map.co_code.isnull()]['isin_code_equity'].values)))
        bse_map.dropna(subset=['co_code'], inplace=True)

    #Convert co_code to int
    bse_map.co_code = bse_map.co_code.astype(int)

    return bse_map

if __name__ == '__main__':

    engine_name = 'sqlite:////'+ os.path.abspath('../') + '/rawdata/csv_database.db'
    csv_database = create_engine(engine_name)
    bse_map = read_bse_static_map()

    yrlist = [1998 + x for x in range(21)]
    ret_df_list =[]
    price_data_fields = ['bse_returns']
    id_col_list = ['co_code', 'company_name', 'co_stkdate']


    for yr in yrlist:
        q_temp = ("SELECT * FROM PriceTable WHERE"
                    " co_code in ({codelist}) AND"
                    " co_stkdate > {stdt} AND"
                    " co_stkdate < {enddt}").format(codelist = ','.join([str(x) for x in bse_map.co_code.values]),
                                                      stdt= str(yr)+'0000', enddt=str(yr+1)+'0000')

        print('Starting query: BSE data for year {} at {}'.format(yr, dt.datetime.now()))
        bse500_df = pd.read_sql_query(q_temp, csv_database)
        print('Finished query: BSE data for year {} at {}'.format(yr, dt.datetime.now()))
        print(' ')

        temp_df = pd.melt(bse500_df[id_col_list + price_data_fields], id_vars=id_col_list,
                value_vars=price_data_fields)

        ret_df_list.append(temp_df)

    all_rets_df = pd.concat(ret_df_list)
    all_rets_df.to_csv(os.path.join('..','deriveddata', price_data_fields[0]+'_staticUnivMay2019.csv'))
