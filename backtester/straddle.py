import sqlite3, datetime, sys, json
import backtrader as bt
import pandas as pd
from utils.log import logger_instance
from utils.misc import get_nearest_expiry
import quantstats as qs

logging = logger_instance


# Strategy Details:
# Simple straddle where we take position at 9:20 and sell at 15:15 if SL's are not hit on any leg
# If any leg has a SL hit of 25% or 30% then we exit only that leg and wait for other leg to get squared off on SL or
# at 15:15


class TestStrategy(bt.Strategy):
    params = (
        ('config', ''),
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        logging.info('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order_close = None
        self.order1_close = None
        self.order = None
        self.order1 = None
        self.pnl = 0
        self.sl = 0.3
        self.ce_retry_counter = 0
        self.pe_retry_counter = 0
        self.config = self.params.config

    def notify_trade(self, trade):
        # Closed positions PNL
        # self.log("Trade executed: {}".format(trade))
        self.pnl += trade.pnl

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED {} for {}'.format(order.executed.price, order.product_type))
            elif order.issell():
                self.log('SELL EXECUTED {} for {}'.format(order.executed.price, order.product_type))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected for {}'.format(order.product_type))

    def next(self):

        if self.config["fixed_sl_percentage"]:
            sl_pe = sl_ce = self.config["fixed_sl_percentage"]

        end_of_day_minute = self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute in [
            20, 21]
        flag = False
        # skip config days
        dayofweek = self.data0.datetime.datetime().date().strftime("%A")
        if dayofweek in self.config["blind_skip_days_list"]:
            self.log("Skipping trade as blind_skip_days_list is enabled for {}".format(dayofweek))
            return

        # Calculating open positions PNL
        pos = self.getposition(self.data0)
        # import ipdb;ipdb.set_trace()
        comminfo = self.broker.getcommissioninfo(self.data0)
        pnl = comminfo.profitandloss(pos.size, pos.price, self.data0.close[0])

        pos1 = self.getposition(self.data1)
        comminfo1 = self.broker.getcommissioninfo(self.data1)
        pnl1 = comminfo1.profitandloss(pos1.size, pos1.price, self.data1.close[0])

        total_pnl = pnl + pnl1
        #self.log('position pnl: {} trade pnl: {}, total pnl: {}'.format(total_pnl, self.pnl, total_pnl + self.pnl))

        # Check if we are in the market
        if (not self.getposition(self.data0)) and (not self.getposition(self.data1)):
            if self.data0.datetime.datetime().time().hour == self.config["position_initiate_time"]["hour"] \
                    and self.data0.datetime.datetime().time().minute == self.config["position_initiate_time"]["minute"]:
                self.log('SELL CREATE, {} for {} on {}'.format(self.data0.close[0], self.data0._dataname.ticker[0],
                                                               self.data0.datetime.date().strftime("%A")))
                self.log('SELL CREATE, {} for {} on {}'.format(self.data1.close[0], self.data1._dataname.ticker[0],
                                                               self.data0.datetime.date().strftime("%A")))
                self.order = self.sell(self.data0)
                self.order.product_type = self.data0._name
                self.order1 = self.sell(self.data1)
                self.order1.product_type = self.data1._name

        else:
            # added end of day condition here to avoid SL trigger and end of day trigger on same tick
            if not end_of_day_minute and self.order and self.data0.close[0] > (
                    self.order.executed.price * (1 + sl_pe)) and self.getposition(self.data0):
                self.log('BUY CREATE SL, {}, {}-{} - {}'.format(self.data0.close[0], self.order.executed.price,
                                                                self.order.executed.price * (1 + sl_pe),
                                                                self.order.product_type))
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                flag = True

            if not end_of_day_minute and self.order1 and self.data1.close[0] > (
                    self.order1.executed.price * (1 + sl_ce)) and self.getposition(self.data1):
                self.log('BUY CREATE SL, {}, {}-{} - {}'.format(self.data1.close[0], self.order1.executed.price,
                                                                self.order1.executed.price * (1 + sl_ce),
                                                                self.order1.product_type))
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                flag = True

            # # Below is exit condition based on portfolio loss
            # if not flag and (total_pnl + self.pnl < -3000):
            #     if self.getposition(self.data0):
            #         self.order_close = self.buy(self.data0)
            #         self.order_close.product_type = self.data0._name
            #         self.log('BUY CREATE PSL, {} {}'.format(self.data0.close[0], self.order_close.product_type))
            #
            #     if self.getposition(self.data1):
            #         self.order1_close = self.buy(self.data1)
            #         self.order1_close.product_type = self.data1._name
            #         self.log('BUY CREATE PSL, {} {}'.format(self.data1.close[0], self.order1_close.product_type))
            #     logic needed to skip all candles of today once this condition is met

        if end_of_day_minute:
            self.log("PNL: {}".format(total_pnl + self.pnl))
            if self.getposition(self.data0):
                o = self.buy(self.data0)
                o.product_type = self.data0._name
                self.log('BUY CREATE, {} {}'.format(self.data0.close[0], o.product_type))

            if self.getposition(self.data1):
                o1 = self.buy(self.data1)
                o1.product_type = self.data1._name
                self.log('BUY CREATE, {} {}'.format(self.data1.close[0], o1.product_type))
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None

        # The below because not able to set self.pnl = 0 in above block
        if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute > 20:
            self.pnl = 0


def main():
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(sys.argv[0], "<filename.json>"))
        exit(1)

    file_name = sys.argv[1]
    with open(file_name) as config_file:
        config = json.load(config_file)

    strike_dbpath = config['strike_dbpath']
    underlying_dbpath = config['underlying_dbpath']
    start_date = config["start_date"]
    end_date = config["end_date"]
    table_name = config["strike_table_name"]
    underlying_table_name = config["underlying_table_name"]
    strangle_delta = config["strangle_delta"]

    con = sqlite3.connect(strike_dbpath)
    con1 = sqlite3.connect(underlying_dbpath)
    df_final_pe = pd.DataFrame()
    df_final_ce = pd.DataFrame()
    datelist = pd.date_range(start=start_date, end=end_date).tolist()
    start_date = str(datelist[0].date())
    end_date = str(datelist[-1].date())
    for d in datelist:
        d = d.date()
        d1 = get_nearest_expiry(d)
        ohlc = {'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'sum'}
        underlying_query_string = "SELECT * from {} where date(date) = date(?)".format(underlying_table_name)
        df_fut = pd.read_sql_query(underlying_query_string,
                                   con1, params=[d], parse_dates=True, index_col='date')
        if df_fut.empty:
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        # df5_fut = df_fut.resample('5min').apply(ohlc)

        # bloody bug!! , index here should match the candle number of the exact time that we are taking a position on
        # for example if we are taking a position at 09:20 then [4] ,10:00 then [44] as these are 1 minute candles
        close = df_fut.iloc[4].close
        atm_strike = round(close / 50) * 50  # round to nearest 50
        # ce_strike = pe_strike = atm_strike
        ce_strike = atm_strike + strangle_delta
        pe_strike = atm_strike - strangle_delta
        #logging.info("Reading data for {}".format(d))
        logging.info(
            "Trade date : {} - Picking ce strike - {} , pe strike - {}, atm strike - {}  for expiry {}, underlying: {}".format(d,
                                                                                                               ce_strike,
                                                                                                               pe_strike,
                                                                                                               atm_strike,
                                                                                                               d1, close))
        query_string = "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
            table_name)
        df_opt_pe = pd.read_sql_query(query_string, con, params=[pe_strike, d, d1, 'PE'], parse_dates=True,
                                      index_col='date')
        df_opt_ce = pd.read_sql_query(query_string, con, params=[ce_strike, d, d1, 'CE'], parse_dates=True,
                                      index_col='date')

        df_opt_pe.index = pd.to_datetime(df_opt_pe.index)
        df_opt_pe = df_opt_pe.sort_index()

        df_opt_ce.index = pd.to_datetime(df_opt_ce.index)
        df_opt_ce = df_opt_ce.sort_index()
        # skip the day if first tick is not at 9:15
        if not df_opt_ce.empty and (df_opt_ce.index[0].minute != 15 or df_opt_pe.index[0].minute != 15):
            continue

        df_final_ce = df_final_ce.append(df_opt_ce)
        df_final_pe = df_final_pe.append(df_opt_pe)
    cerebro = bt.Cerebro()
    data_pe = bt.feeds.PandasData(dataname=df_final_pe)
    data_ce = bt.feeds.PandasData(dataname=df_final_ce)
    cerebro.adddata(data_pe, name= 'PE')
    cerebro.adddata(data_ce, name= 'CE')
    cerebro.addstrategy(TestStrategy, config=config)
    cerebro.addsizer(bt.sizers.SizerFix, stake=300)
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='tanal')
    cerebro.broker.setcash(config["capital"])
    print("Run start")
    logging.info("Logging Config {}".format(config))

    strats = cerebro.run()

    pyfolio = strats[0].analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
    # import ipdb;ipdb.set_trace()
    returns.index = returns.index.tz_convert(None)
    qs.extend_pandas()
    qs.reports.html(returns, output=config["output_filename"], download_filename=config["output_filename"],
                    title=config["strategy_name"])

    portvalue = cerebro.broker.getvalue()
    # Print out the final result
    print('Final Portfolio Value: ${}'.format(portvalue))
    print("Run finish")


if __name__ == '__main__':
    main()
