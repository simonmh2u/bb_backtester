import datetime, sqlite3, time
import pandas as pd
from dateutil.parser import parse
from data.historical_expiry_dates import expiry_date_list
from data.icharts_ohlc_fetcher import fetch_icharts_ohlc
from utils.log import logger_instance

logging = logger_instance


# def get_dates():
#     start_date_config = "2019-02-11"
#     end_date_config = "2019-02-13"
#     from_date = parse(start_date_config)
#     to_date = parse(end_date_config)
#     return from_date, to_date


# def get_nearest_expiry(trade_date):
#     for i in expiry_date_list:
#         x = parse(i).date()
#         if x >= trade_date:
#             return x, i


def get_ichart_specific_expiry_string(strike, type, expiry_date):
    ichart_string = "NIFTY-" + str(strike) + type + "-" + expiry_date
    return ichart_string


def save_option_data():
    underlying_dbpath = "/Volumes/HD2/OptionData/NIFTY50.db"
    underlying_table_name = "nifty"
    db_path = "/Volumes/HD2/OptionData/nifty_2023.db"
    table_name = "nifty"
    # start_date, end_date = get_dates()
    # datelist = pd.date_range(start=start_date, end=end_date).tolist()
    con = sqlite3.connect(db_path)
    con1 = sqlite3.connect(underlying_dbpath)
    column_names = ['date', 'open', 'high', 'low', 'close', 'volume', 'oi', 'strike', 'ticker', 'type',
                    'expiry_date']
    for d in expiry_date_list:
        expiry_date_str = d
        expiry_date = parse(d).date()
        underlying_query_string = "SELECT * from {} where date(date) = date(?)".format(underlying_table_name)
        df_fut = pd.read_sql_query(underlying_query_string,
                                   con1, params=[expiry_date], parse_dates=True, index_col='date')
        ltp = df_fut.iloc[4].close
        atm_strike = round(ltp / 50) * 50
        cee_strike_list_upper = [x for x in range(atm_strike, atm_strike + 2000, 50)]
        cee_strike_list_lower = [x for x in range(atm_strike, atm_strike - 2000, -50)]
        cee_strike_list = cee_strike_list_lower + cee_strike_list_upper
        #cee_strike_list = list(set(cee_strike_list))
        for cee_strike in cee_strike_list:
            saveable_data = []
            ichart_string = get_ichart_specific_expiry_string(cee_strike, "C", expiry_date_str)
            ohlc_data = fetch_icharts_ohlc(ichart_string)
            for x in ohlc_data:
                y = x.split(",")[:7]
                y.extend([cee_strike, str(cee_strike) + "CE", "CE", str(expiry_date)])
                saveable_data.append(y)
            if len(saveable_data) <= 1:
                logging.info("Empty Data for Strike - {}-CE".format(cee_strike))
                time.sleep(0)
                continue
            df = pd.DataFrame(saveable_data, columns=column_names)
            df['date'] = pd.to_datetime(df['date'],dayfirst=True)
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(int)
            df['oi'] = df['oi'].astype(int)
            df.set_index('date', inplace=True)
            df.to_sql(table_name, con, if_exists='append')
            logging.info("Loaded data for {} for strike: {}-CE".format(d, cee_strike))
            time.sleep(0.2)

        pee_strike_list_lower = [x for x in range(atm_strike, atm_strike - 2000, -50)]
        pee_strike_list_upper = [x for x in range(atm_strike, atm_strike + 2000, +50)]
        pee_strike_list = pee_strike_list_lower + pee_strike_list_upper
        #pee_strike_list = list(set(pee_strike_list)
        for pee_strike in pee_strike_list:
            saveable_data = []
            ichart_string = get_ichart_specific_expiry_string(pee_strike, "P", expiry_date_str)
            ohlc_data = fetch_icharts_ohlc(ichart_string)
            for x in ohlc_data:
                y = x.split(",")[:7]
                y.extend([pee_strike, str(pee_strike) + "PE", "PE", str(expiry_date)])
                saveable_data.append(y)
            if len(saveable_data) <= 1:
                logging.info("Empty Data for Strike - {}-PE".format(pee_strike))
                time.sleep(0)
                continue
            df = pd.DataFrame(saveable_data, columns=column_names)
            df['date'] = pd.to_datetime(df['date'], dayfirst=True)
            df.set_index('date', inplace=True)
            df.to_sql(table_name, con, if_exists='append')
            logging.info("Loaded data for {} for strike: {}-PE".format(d, pee_strike))
            time.sleep(0.2)

if __name__ == '__main__':
    save_option_data()
