import re, os, sys, sqlite3
import pandas as pd
import pandas_ta as ta
import datetime
from dateutil.parser import parse
from utils.log import logger_instance
from dateutil.rrule import *

logging = logger_instance

ohlc = {'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'oi': 'sum'}


def splitterV2(s, index_str):
    '''
    :param s:
    handles below formats

    NIFTY2281818450CE

    :param date:
    2020

    "param index_str:
    NIFTY
    BANKNIFTY

    :return:
     ('10800', 'CE', datetime.date(2020, 2, 18))

    '''

    try:
        # import ipdb;ipdb.set_trace()
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        t = s.split(index_str)[1]
        u = t.split(".")[0]
        if u[0:2] in ["17", "18", "19", "20", "21", "22"]:
            # import ipdb;ipdb.set_trace()
            # Weekly expiry
            if u[2:5] in months:
                v, w = u[:5], u[5:]
                x, y = w[:len(w) - 2], w[-2:]
                z = v + '1'  # 1st date of the month
                z = list(rrule(MONTHLY, count=1, byweekday=TH(-1), dtstart=parse(z, yearfirst=True)))[
                    0].date()  # last thursday of month
            else:
                v, w = u[:5], u[5:]
                x, y = w[:len(w) - 2], w[-2:]
                yy, mm, dd = v[0:2], v[2:3], v[3:5]
                mm = mm.replace('O', '10').replace('N', '11').replace('D', '12')
                yr = yy + '-' + mm + '-' + dd
                z = parse(yr, yearfirst=True).date()
        return x, y, z
    except Exception as e:
        logging.exception("Exception {}, {}".format(t, e))
        return "", "", None


def splitter(s, index_str):
    '''
    :param s:
    handles below formats
    NIFTY16FEB8100CE
    NIFTY18FEB10800CE.NFO
    NIFTY08APR2115800CE
    NIFTY2281818450CE

    :param date:
    2020

    "param index_str:
    NIFTY
    BANKNIFTY

    :return:
     ('10800', 'CE', datetime.date(2020, 2, 18))

    '''

    try:
        # import ipdb;ipdb.set_trace()
        months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        t = s.split(index_str)[1]
        u = t.split(".")[0]
        if u[5:7] in ["17", "18", "19", "20", "21", "22"]:
            import ipdb;
            ipdb.set_trace()
            # Weekly expiry
            if u[7:10] in months:
                v, w = u[:7], u[7:]
                x, y = w[:len(w) - 2], w[-2:]
                z = parse(v, dayfirst=True).date()
            else:
                v, w = u[:5], u[5:]
                x, y = w[:len(w) - 2], w[-2:]
                yy, mm, dd = v[0:2], v[2:3], v[3:5]
                mm = mm.replace('O', '10').replace('N', '11').replace('D', '12')
                yr = yy + '-' + mm + '-' + dd
                z = parse(yr, yearfirst=True).date()
        else:
            import ipdb;
            ipdb.set_trace()
            # Monthly Expiry
            v, w = re.findall('\d*\D+', u)
            x, y = w[:len(w) - 2], w[-2:]
            z = v + '1'  # 1st date of the month
            z = list(rrule(MONTHLY, count=1, byweekday=TH(-1), dtstart=parse(z, yearfirst=True)))[
                0].date()  # last thursday of month
        return x, y, z
    except Exception as e:
        logging.exception("Exception {}, {}".format(t, e))
        return "", "", None


def clean_and_save(file, index_str, con, year, resample=None):
    try:
        df = pd.read_csv(file, parse_dates=[['Date', 'Time']], dayfirst=True)
        df.drop(df[df['Ticker'].str.contains('OPTIDX_')].index, inplace=True)
        df.drop(df[df['Ticker'].str.contains('FINNIFTY')].index, inplace=True)
        logging.info("Processing file {}".format(file))
        df = df.rename(
            columns={"Open Interest": "oi", "Open": "open", "Close": "close", "High": "high", "Volume": "volume",
                     "Date_Time": "date", "Ticker": "ticker", "Low": "low"})
        df = df.drop(['Date_', 'Month', 'Year'], axis=1)
        df.columns = df.columns.str.strip()
        df.index = pd.to_datetime(df.date)
        df.drop(['date'], axis=1, inplace=True)
        if resample:
            df = df.groupby('ticker').resample(resample).apply(ohlc).reset_index()
            df.index = pd.to_datetime(df.date)
            df.drop(['date'], axis=1, inplace=True)
        df[['strike', 'type', 'expiry_date']] = df.apply(lambda x: splitter(x['ticker'], index_str),
                                                         axis=1).tolist()

        if resample:
            df.to_sql('options_' + year + resample, con, if_exists='append')
        else:
            df.to_sql('options_' + year, con, if_exists='append')
    except Exception as e:
        logging.exception("Exception on processing file {}".format(file, e))


def clean_and_saveV2(file, index_str, con, year, resample=None):
    try:
        df = pd.read_csv(file)
        logging.info("Processing file {}".format(file))
        df = df.rename(
            columns={"Open Interest": "oi", "Open": "open", "Close": "close", "High": "high", "Volume": "volume",
                     "Date": "date", "Symbol": "ticker", "Low": "low"})
        df.columns = df.columns.str.strip()
        df.index = pd.to_datetime(df.date)
        # import ipdb;ipdb.set_trace()
        df.index = df.index.tz_localize(None)
        df.drop(['date'], axis=1, inplace=True)
        if resample:
            df = df.groupby('ticker').resample(resample).apply(ohlc).reset_index()
            df.index = pd.to_datetime(df.date)
            df.drop(['date'], axis=1, inplace=True)
        df[['strike', 'type', 'expiry_date']] = df.apply(lambda x: splitterV2(x['ticker'], index_str),
                                                         axis=1).tolist()

        if resample:
            df.to_sql('nifty_options_' + year + resample, con, if_exists='append')
        else:
            df.to_sql('nifty_options_' + year, con, if_exists='append')
    except Exception as e:
        logging.exception("Exception on processing file {}".format(file, e))


def clean_and_save_futures(file, con, year):
    try:
        df = pd.read_csv(file, parse_dates=[['Date', 'Time']], dayfirst=True)
        logging.info("Processing file {}".format(file))
        df = df.rename(
            columns={"Open Interest": "oi", "Open": "open", "Close": "close", "High": "high", "Volume": "volume",
                     "Date_Time": "date", "Ticker": "ticker"})

        df = df.drop(['Date_', 'Month', 'Year'], axis=1)
        df.columns = df.columns.str.strip()
        df.index = pd.to_datetime(df.date)
        df.drop(['date'], axis=1, inplace=True)
        df.to_sql('nifty_futures_' + year, con, if_exists='append')
    except Exception as e:
        logging.exception("Exception on processing file {}".format(file, e))


def get_files(path, pattern):
    file_list = []
    for dirpath, subdirs, files in os.walk(path):
        for x in files:
            if pattern in x:
                x = os.path.join(dirpath, x)
                file_list.append(x)
    return file_list


def main():
    '''
    python data/cleaner.py '/Volumes/HD2/OptionData/2016-21_raw/2016/' '_nifty_futures' 'NIFTY' 2016 'future'
    python data/cleaner.py '/Volumes/HD2/OptionData/2016-21_raw/2016/' '_nifty_options' 'NIFTY' 2016 'option' '5min'
    '''
    if len(sys.argv) < 6:
        print("Input Format Incorrect : {} {}".format(sys.argv[0],
                                                      "<path> <pattern> <index_string> <year> <future/option> [5min]"))
        exit(1)
    path = sys.argv[1]
    pattern = sys.argv[2]
    index_str = sys.argv[3]
    year = sys.argv[4]
    future_option = sys.argv[5]
    resample = None
    if len(sys.argv) == 7:
        resample = sys.argv[6]
    file_list = get_files(path, pattern)
    path = '/Volumes/HD2/OptionData/'
    db_name = path + index_str + str(year) + '.db'
    con = sqlite3.connect(db_name)
    for file in file_list:
        if future_option == "future":
            clean_and_save_futures(file, con, year)
        elif future_option == "option":
            clean_and_saveV2(file, index_str, con, year, resample)
    con.execute("CREATE INDEX IF NOT EXISTS ix_nifty_options_expiry_date ON  nifty_options_{}(expiry_date)".format(year))
    if resample:
        con.execute(
            "CREATE INDEX IF NOT EXISTS ix_nifty_options_expiry_date1 ON  nifty_options_{}5min(expiry_date)".format(year))
    con.close()


if __name__ == '__main__':
    main()
