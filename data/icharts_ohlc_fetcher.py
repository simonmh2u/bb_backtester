import requests


def fetch_icharts_ohlc(symbol, timeframe="1min"):
    from conf.local_conf import ichart_session_key, ichart_username
    cookies = {
        'PHPSESSID': ichart_session_key,
    }

    headers = {
        'authority': 'www.icharts.in',
        'accept': '*/*',
        'accept-language': 'en-GB,en;q=0.5',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'referer': 'https://www.icharts.in/opt/OptionsChart.php',
        'sec-ch-ua': '"Not/A)Brand";v="99", "Brave";v="115", "Chromium";v="115"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/115.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    params = {
        'mode': 'INTRA',
        'symbol': symbol,
        'timeframe': timeframe,
        'u': ichart_username,
        'sid': ichart_session_key,
    }

    response = requests.get(
        'https://www.icharts.in/opt/hcharts/stx8req/php/getdataForOptions_curr_atp.php',
        params=params,
        cookies=cookies,
        headers=headers,
    )
    json_data = []
    if response.status_code == 200:
        s_data = response.content.decode("utf-8")
        json_data = s_data.split("\n")
        return json_data
