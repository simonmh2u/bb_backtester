from data.http_ohlc_fetcher import fetch_http_ohlc
import pandas as pd
import datetime, sqlite3, time, sys, json
from utils.log import logger_instance

logging = logger_instance


# TODO incremental download which picks last date from DB and starts download from thatd ate until yestrday
def main():
    """
  -Downloads data from start to end range in chunks of  1 month and save in sqlite DB
  - end data shoule atleast 30 days greater than start date
  - local_conf should have the broker cookie set
  -config format as slt json
      "instrument": 264969,
      "tradingsymbol": "INDIA VIX",
      "start_date": "2016-01-01",
      "end_date": "2022-02-28",
      "table_name": "vix",
      "dbfile_path": "/Volumes/HD2/OptionData/vix.db",
      "period": "minute| 5minute| 15minute | etc"



    :return:
    """
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(sys.argv[0], "<filename.json>"))
        exit(1)

    file_name = sys.argv[1]
    with open(file_name) as config_file:
        data = json.load(config_file)

    instrument = data['instrument']
    tradingsymbol = data['tradingsymbol']
    start_date = data["start_date"]
    end_date = data["end_date"]
    db_path = data["dbfile_path"]
    table_name = data["table_name"]
    period = data["period"]
    con = sqlite3.connect(db_path)
    stock = {"instrumenttoken": instrument, "tradingsymbol": tradingsymbol}
    date_range = pd.date_range(start_date, end_date, freq="M")
    for datex in date_range:
        d_ref = datex.date()
        s_date = datetime.date(d_ref.year, d_ref.month, 1)
        e_date = datex.date()
        current = fetch_http_ohlc(stock=stock, from_date=s_date, to_date=e_date,
                                  period=period)
        df = pd.DataFrame(current,
                          columns=["date", "open", "high", "low", "close", "volume", "oi"])
        df['date'] = pd.to_datetime(df['date']) \
            .dt.tz_localize(None)
        df.to_sql(table_name, con, if_exists='append')
        logging.info("Saving OHLC for {} from {} to {}".format(stock, s_date, e_date))
        # adding sleep just in case to bypass rate limits of broker if any
        time.sleep(1)


if __name__ == '__main__':
    main()
