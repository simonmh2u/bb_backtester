import sqlite3, _pickle as pickle
import datetime, gzip
import sys
import json
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
        self.sl = 0.3
        self.ce_retry_counter = 0
        self.pe_retry_counter = 0
        self.ce_price = None
        self.pe_price = None
        self.skip_all_remaining_candles = False
        self.only_pe_position = False
        self.only_ce_position = False
        self.config = self.params.config
        self.capital = self.config["capital"]
        self.tsl_delta = self.config["trailing_sl_delta_increment"]
        self.reset_pnl_variables()

    def reset_pnl_variables(self):
        self.pnl = 0
        self.ce_pnl = 0
        self.pe_pnl = 0
        self.tsl_activated = False
        self.tsl_start = self.config["trailing_sl_percentage"]
        self.tsl_start_original = self.config["trailing_sl_percentage"]
        self.strategy_profit = self.config["capital"] * self.tsl_start

    def notify_trade(self, trade):
        # Closed positions PNL
        # self.log("Trade executed: {}".format(trade))
        # if trade data is CE then add to CE PNL else add to PE PNL
        if trade.data._name == "CE":
            self.ce_pnl += trade.pnl
        else:
            self.pe_pnl += trade.pnl
        self.pnl += trade.pnl

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED {} for {}'.format(
                    order.executed.price, order.product_type))
            elif order.issell():
                self.log('SELL EXECUTED {} for {}'.format(
                    order.executed.price, order.product_type))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(
                'Order Canceled/Margin/Rejected for {}'.format(order.product_type))

    def next(self):
        self.only_pe_position = False
        self.only_ce_position = False
        data0_ticker = "PE"
        data1_ticker = "CE"
        current_candle_datetime_underlying = self.data2.datetime.datetime()
        # self.log(
        #     "Current candle time: {} ".format(current_candle_datetime_underlying))
        try:
            data0_ticker = self.data0._dataname.loc[current_candle_datetime_underlying].ticker
            data1_ticker = self.data1._dataname.loc[current_candle_datetime_underlying].ticker
        except Exception as e:
            # import ipdb;ipdb.set_trace()
            # logging.exception(e)
            logging.info("Skipping candle as data not available for {} and {} for time {}".format(
                data0_ticker, data1_ticker, current_candle_datetime_underlying))
            return

        if self.config["fixed_sl_percentage"]:
            sl_pe = sl_ce = self.config["fixed_sl_percentage"]

        if self.config["daywise_sl_percentage"]:
            dayofweek = self.data0.datetime.datetime().date().strftime("%A")
            sl_pe = sl_ce = self.config["daywise_sl_percentage"][dayofweek]
        
        if self.getposition(self.data0) and not self.getposition(self.data1):
            self.only_pe_position = True

        if self.getposition(self.data1) and not self.getposition(self.data0):
            self.only_ce_position = True

        # To enable below condition we need good comparison of returns with and without this condition
        if self.order:
            self.pe_price = self.order.executed.price

        if self.order1:
            self.ce_price = self.order1.executed.price

        if self.data0.datetime.datetime().time().hour == self.config["position_initiate_time"]["hour"] \
                and self.data0.datetime.datetime().time().minute == self.config["position_initiate_time"]["minute"]:
            self.skip_all_remaining_candles = False
            self.tsl_start = self.config["trailing_sl_percentage"]

        if self.skip_all_remaining_candles:
            self.reset_pnl_variables()
            return

        # end_of_day_minute = self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute in [
        #     20, 21]
        end_of_day_minute = True if self.data0.datetime.datetime(
        ).time() > datetime.time(15, 21, 00) else False

        # skip config days
        dayofweek = self.data0.datetime.datetime().date().strftime("%A")
        if dayofweek in self.config["blind_skip_days_list"]:
            self.log(
                "Skipping trade as blind_skip_days_list is enabled for {}".format(dayofweek))
            return

        # Calculating open positions PNL
        pos = self.getposition(self.data0)
        # import ipdb;ipdb.set_trace()
        comminfo = self.broker.getcommissioninfo(self.data0)
        pe_pnl = comminfo.profitandloss(pos.size, pos.price, self.data0.close[0])

        pos1 = self.getposition(self.data1)
        comminfo1 = self.broker.getcommissioninfo(self.data1)
        ce_pnl = comminfo1.profitandloss(
            pos1.size, pos1.price, self.data1.close[0])

        total_pnl = pe_pnl + ce_pnl
        # self.log('position pnl: {} trade pnl: {}, total pnl: {}'.format(total_pnl, self.pnl, total_pnl + self.pnl))

        # Below is exit condition based on portfolio loss
        if not end_of_day_minute and ((total_pnl + self.pnl) < (self.config["capital"] * -self.config["mtm_sl_percentage"])):
            if self.getposition(self.data0):
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                self.log('BUY CREATE MTM SL, {} {}'.format(
                    self.data0.close[0], data0_ticker))
                self.pe_retry_counter = 0
                self.pe_price = None

            if self.getposition(self.data1):
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                self.log('BUY CREATE MTM SL, {} {}'.format(
                    self.data1.close[0], data1_ticker))
                self.ce_retry_counter = 0
                self.ce_price = None

            self.log("PNL MTM: {}".format(total_pnl + self.pnl))
            self.skip_all_remaining_candles = True
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None
            self.ce_price = None
            self.pe_price = None
            self.reset_pnl_variables()

        # Trailing Profit MTM Logic
        if self.only_ce_position:
            if self.tsl_start and (ce_pnl + self.ce_pnl) > self.strategy_profit :
                self.lower_strategy_sl = self.tsl_start - self.tsl_delta
                self.upper_strategy_sl = self.tsl_start + self.tsl_delta
                if (ce_pnl + self.ce_pnl) > (self.capital * self.upper_strategy_sl):
                    self.tsl_activated = True
                    self.log("TSL activated at {} % for profit {}".format(
                        self.upper_strategy_sl, ce_pnl+self.ce_pnl))
                    self.tsl_start = self.upper_strategy_sl
        
            if self.tsl_activated and (ce_pnl + self.ce_pnl) < (self.capital * self.lower_strategy_sl):
                self.log("Exiting CE leg only, TSL hit at {} % for profit {}".format(
                    self.lower_strategy_sl, ce_pnl+self.ce_pnl))
                self.log(
                    'BUY CREATE TSL, {} - {}'.format(self.data1.close[0], data1_ticker))
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                self.ce_retry_counter = 5
                self.reset_pnl_variables()
        
        if self.only_pe_position:
            if self.tsl_start and (pe_pnl + self.pe_pnl) > self.strategy_profit :
                self.lower_strategy_sl = self.tsl_start - self.tsl_delta
                self.upper_strategy_sl = self.tsl_start + self.tsl_delta
                if (pe_pnl + self.pe_pnl) > (self.capital * self.upper_strategy_sl):
                    self.tsl_activated = True
                    self.log("TSL activated at {} % for profit {}".format(
                        self.upper_strategy_sl, pe_pnl+self.pe_pnl))
                    self.tsl_start = self.upper_strategy_sl
        
            if self.tsl_activated and (pe_pnl + self.pe_pnl) < (self.capital * self.lower_strategy_sl):
                self.log("Exiting PE leg only, TSL hit at {} % for profit {}".format(
                    self.lower_strategy_sl, pe_pnl+self.pe_pnl))
                self.log(
                    'BUY CREATE TSL, {} - {}'.format(self.data0.close[0], data0_ticker))
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                self.pe_retry_counter = 5
                self.reset_pnl_variables()


        # Check if we are in the market
        if not self.getposition(self.data0):
            if self.pe_price and self.data0.close[0] < self.pe_price and self.pe_retry_counter <= 2:
                self.log('SELL CREATE, {} for {} on {}, retry #: {}'.format(self.data0.close[0], data0_ticker,
                                                                            self.data0.datetime.date().strftime("%A"), self.pe_retry_counter))
                self.order = self.sell(self.data0)
                self.order.product_type = self.data0._name

            if self.data0.datetime.datetime().time().hour == self.config["position_initiate_time"]["hour"] \
                    and self.data0.datetime.datetime().time().minute == self.config["position_initiate_time"]["minute"]:
                self.pe_retry_counter = 0
                self.pe_price = self.data0.close[0] - 2

        else:
            # added end of day condition here to avoid SL trigger and end of day trigger on same tick
            if not end_of_day_minute and self.order and self.data0.close[0] > (
                    self.order.executed.price * (1 + sl_pe)) and self.getposition(self.data0):
                self.log('BUY CREATE SL, {}, {}-{} - {}'.format(self.data0.close[0], self.order.executed.price,
                                                                self.order.executed.price *
                                                                (1 + sl_pe),
                                                                data0_ticker))
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name
                self.pe_retry_counter += 1

        if not self.getposition(self.data1):
            if self.ce_price and self.data1.close[0] < self.ce_price and self.ce_retry_counter <= 2:
                self.log('SELL CREATE, {} for {} on {}, retry #: {}'.format(self.data1.close[0], data1_ticker,
                                                                            self.data1.datetime.date().strftime("%A"), self.ce_retry_counter))
                self.order1 = self.sell(self.data1)
                self.order1.product_type = self.data1._name

            if self.data1.datetime.datetime().time().hour == self.config["position_initiate_time"]["hour"] \
                    and self.data1.datetime.datetime().time().minute == self.config["position_initiate_time"]["minute"]:
                self.ce_price = self.data1.close[0] - 2
                self.ce_retry_counter = 0

        else:
            if not end_of_day_minute and self.order1 and self.data1.close[0] > (
                    self.order1.executed.price * (1 + sl_ce)) and self.getposition(self.data1):
                self.log('BUY CREATE SL, {}, {}-{} - {}'.format(self.data1.close[0], self.order1.executed.price,
                                                                self.order1.executed.price *
                                                                (1 + sl_ce),
                                                                data1_ticker))
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name
                self.ce_retry_counter += 1

        if end_of_day_minute:
            self.log("PNL EOD: {}".format(total_pnl + self.pnl))
            if self.getposition(self.data0):
                o = self.buy(self.data0)
                o.product_type = self.data0._name
                self.log('BUY CREATE EOD, {} {}'.format(
                    self.data0.close[0], data0_ticker))
                self.pe_retry_counter = 0
                self.pe_price = None

            if self.getposition(self.data1):
                o1 = self.buy(self.data1)
                o1.product_type = self.data1._name
                self.log('BUY CREATE EOD, {} {}'.format(
                    self.data1.close[0], data1_ticker))
                self.ce_retry_counter = 0
                self.ce_price = None
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None
            self.ce_price = None
            self.pe_price = None
            self.skip_all_remaining_candles = True
            self.reset_pnl_variables()

        # # The below because not able to set self.pnl = 0 in above block
        # if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute > 20:
        #     self.pnl = 0


def main():
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(
            sys.argv[0], "<filename.json>"))
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
    df_final_underlying = pd.DataFrame()
    datelist = pd.date_range(start=start_date, end=end_date).tolist()
    start_date = str(datelist[0].date())
    end_date = str(datelist[-1].date())

    if config["read_from_serialized_input"]:
        datelist = []

    for d in datelist:
        d = d.date()
        d1 = get_nearest_expiry(d)
        ohlc = {'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'sum'}
        underlying_query_string = "SELECT * from {} where date(date) = date(?)".format(
            underlying_table_name)
        df_fut = pd.read_sql_query(underlying_query_string,
                                con1, params=[d], parse_dates=True, index_col='date')
        if df_fut.empty:
            logging.info("Skipping : {}".format(d))
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        # for example if we are taking a position at 09:20 then [4] ,09:30 then [15] as these are 1 minute candles
        close = df_fut.iloc[15].close
        atm_strike = round(close / 50) * 50  # round to nearest 50
        ce_strike = atm_strike + strangle_delta
        pe_strike = atm_strike - strangle_delta
        logging.info(
            "Trade date : {} - Picking ce strike - {} , pe strike - {}, atm strike - {}  for expiry {}, underlying: {}".format(d,
                                                                                                                            ce_strike,
                                                                                                                            pe_strike,
                                                                                                                            atm_strike,
                                                                                                                            d1, close))
        query_string = "SELECT distinct * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
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
        # if not df_opt_ce.empty and (df_opt_ce.index[0].minute != 15 or df_opt_pe.index[0].minute != 15):
        #     continue
        # skip the day if the number of elements in either df_opt_ce or df_opt_pe is less than 375 count.
        if not df_opt_ce.empty and (len(df_opt_ce) < 370 or len(df_opt_pe) < 370):
            logging.info("Skipping : {} as data is incomplete , count : {}".format(d,len(df_opt_ce)))
            continue

        df_final_ce = df_final_ce.append(df_opt_ce)
        df_final_pe = df_final_pe.append(df_opt_pe)
        df_final_underlying = df_final_underlying.append(df_fut)
    cerebro = bt.Cerebro()
    data_pe = bt.feeds.PandasData(dataname=df_final_pe)
    data_ce = bt.feeds.PandasData(dataname=df_final_ce)
    data_underlying = bt.feeds.PandasData(dataname=df_final_underlying)
    cerebro.adddata(data_pe, name='PE')
    cerebro.adddata(data_ce, name='CE')
    cerebro.adddata(data_underlying, name='underlying')

    if not config["read_from_serialized_input"]:
        with open(config["serialize_input_filename"], 'wb') as f:
            logging.info("Serializing input to file")
            pickle.dump(cerebro, f)
    
    else :
        with open(config["serialize_input_filename"], 'rb') as f:
            logging.info("Reading serialized input from file")
            cerebro = pickle.load(f)

    print("Run start")
    logging.info("Logging Config {}".format(config))
    cerebro.addstrategy(TestStrategy, config=config)
    cerebro.addsizer(bt.sizers.SizerFix, stake=config["quantity"])
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='tanal')
    cerebro.broker = bt.brokers.BackBroker(slip_perc=0.005)
    cerebro.broker.setcash(config["capital"])

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
