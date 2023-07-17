from utils.log import logger_instance
from conf.local_conf import username
logger = logger_instance



def fetch_http_ohlc(stock, from_date=None, to_date=None, period="15minute"):
    import requests
    import datetime
    from conf.local_conf import auth_token
    current_date = datetime.datetime.now().date()
    if not from_date:
        from_date = current_date
    if not to_date:
        from_date = current_date
    instrument = stock['instrumenttoken']
    url = "https://kite.zerodha.com/oms/instruments/historical/" + str(instrument) + "/" + period + "?user_id=" + username +"&oi=1&from=" + str(from_date) + "&to=" + str(to_date)

    payload = {}
    headers = {
        'Accept': '*/*',
        'Referer': 'https://kite.zerodha.com/static/build/chart.html?v=2.9.1',
        'Accept-Language': 'en-us',
        'Host': 'kite.zerodha.com',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15',
        'Authorization': auth_token,
    }
    logger.info("Fetching {} period OHLC data for {}".format(period, stock['tradingsymbol']))
    response = requests.request("GET", url, headers=headers, data=payload)
    json_data = {}
    if response.status_code == 200:
        json_data = response.json()
        return json_data['data']['candles']
