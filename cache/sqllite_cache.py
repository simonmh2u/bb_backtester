import sqlite3, datetime, json
from utils.log import logger_instance

logging = logger_instance


class Sqllite(object):

    def init_ltp_db(self):
        dbpath = 'live_price.db'
        self.con = sqlite3.connect(dbpath)
        self.cursor = self.con.cursor()
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS price (name text PRIMARY KEY ,ltp integer,time_stamp DATE DEFAULT (datetime('now','localtime')))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS price_index on price (name)")
        self.con.commit()

    def init_option_chain_db(self):
        dbpath = 'option_chain.db'
        self.con = sqlite3.connect(dbpath)
        self.cursor = self.con.cursor()
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS option_chain (chain_date text PRIMARY KEY ,data text,time_stamp DATE DEFAULT (datetime('now','localtime')))")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS date_index on option_chain (chain_date)")
        self.con.commit()

    def get_ltp(self, name, time_window=1000):
        d = datetime.datetime.now() - datetime.timedelta(seconds=time_window)
        result = self.cursor.execute("select ltp from price where  name = ? and time_stamp > ?", (name, d))
        result = result.fetchone()
        return result[0] if result else None

    def set_ltp(self, key, price):
        self.cursor.execute(
            "INSERT INTO price (name,ltp) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET ltp= ?, time_stamp=?",
            (key, price, price, datetime.datetime.now()))
        self.con.commit()

    def get_chain(self, chain_date, time_window=1000):
        try:
            d = datetime.datetime.now() - datetime.timedelta(seconds=time_window)
            chain_date = str(chain_date)
            result = self.cursor.execute("select data from option_chain where  chain_date = ? and time_stamp > ?", (chain_date, d))
            result = result.fetchone()
            logging.info("Getting Chain from cache - {}".format(result))
            return json.loads(result[0]) if result else None
        except Exception as e:
            logging.exception("Error during getting chain from cache")

    def set_chain(self, chain_date, data):
        chain_date = str(chain_date)
        data = json.dumps(data)
        self.cursor.execute(
            "INSERT INTO option_chain (chain_date,data) VALUES(?,?) ON CONFLICT(chain_date) DO UPDATE SET data= ?, "
            "time_stamp=?",
            (chain_date, data, data, datetime.datetime.now()))
        logging.info("Setting Chain in cache - {}".format(data))
        self.con.commit()