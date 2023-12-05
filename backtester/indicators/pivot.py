import datetime
from decimal import Decimal
import pandas as pd
from utils.log import logger_instance

logging = logger_instance


def df_calculate_pivot_points(df_lastperiod):
    TWO_PLACES = Decimal("0.01")
    last_period = {}
    last_period['high'] = df_lastperiod.iloc[0].high
    last_period['low'] = df_lastperiod.iloc[0].low
    last_period['close'] = df_lastperiod.iloc[0].close

    last_period['pivot'] = (last_period['high'] +
                            last_period['low'] + last_period['close']) / 3
    last_period['R1'] = 2 * last_period['pivot'] - last_period['low']
    last_period['S1'] = 2 * last_period['pivot'] - last_period['high']
    last_period['R2'] = last_period['pivot'] + \
        (last_period['high'] - last_period['low'])
    last_period['S2'] = last_period['pivot'] - \
        (last_period['high'] - last_period['low'])
    last_period['R3'] = last_period['pivot'] + 2 * \
        (last_period['high'] - last_period['low'])
    last_period['S3'] = last_period['pivot'] - 2 * \
        (last_period['high'] - last_period['low'])
    last_period['BC'] = (last_period['high'] + last_period['low']) / 2
    last_period['TC'] = (last_period['pivot'] +
                         last_period['BC']) + last_period['pivot']

    return last_period


# takes a df with 1 minute ohlc data and returns a df with daily ohlc data with pivot points calculated
# from the previous day
def generate_pivot_data(dflowmin):
    ohlc = {'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'oi': 'sum'}

    dflowmin.index = pd.to_datetime(dflowmin.date)
    dflowmin = dflowmin.tz_localize(None)
    dflowmin.dropna(inplace=True)

    df1day = dflowmin.resample('1D').apply(ohlc)

    prev_index = None

    df1day = df1day.tz_localize(None)
    df1day['date'] = pd.to_datetime(df1day.index)
    df1day.index = pd.to_datetime(df1day.date)
    df1day.dropna(inplace=True)

    for i, row in df1day.iterrows():
        current_date = i.date()
        # the below loop is to find the previous day's data considering the fact that there might be holidays in between
        for j in range(1, 5):
            previous_date = current_date - datetime.timedelta(days=j)
            dfprevday = df1day.loc[(df1day['date'] == str(previous_date))]
            if dfprevday.empty:
                continue
            else:
                break
        if j == 4:
            continue

        pivot_dict = df_calculate_pivot_points(dfprevday)
        df1day.loc[i, 'S1'] = pivot_dict['S1']
        df1day.loc[i, 'S2'] = pivot_dict['S2']
        df1day.loc[i, 'S3'] = pivot_dict['S3']
        df1day.loc[i, 'R1'] = pivot_dict['R1']
        df1day.loc[i, 'R2'] = pivot_dict['R2']
        df1day.loc[i, 'R3'] = pivot_dict['R3']
        df1day.loc[i, 'BC'] = pivot_dict['BC']
        df1day.loc[i, 'TC'] = pivot_dict['TC']
        df1day.loc[i, 'pivot'] = pivot_dict['pivot']
        df1day.loc[i, 'PDH'] = pivot_dict['high']
        df1day.loc[i, 'PDL'] = pivot_dict['low']
        daily_date = i.date()
        logging.info(
            "Calculating Daily Pivot data  for date :{}".format(daily_date))
    return df1day
