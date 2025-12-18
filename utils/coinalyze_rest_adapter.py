import requests
import requests.packages
import time
import math
import pathlib
import os
from typing import List, Dict
from .logging import logger
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
from tqdm import tqdm


class CoinalyzeRestAdapter:
    def __init__(self, ssl_verify: bool = True) -> None:
        self.url = 'https://api.coinalyze.net/v1/'
        self._ssl_verify = ssl_verify
        if not ssl_verify:
            requests.packages.urllib3.disable_warnings()

        load_dotenv()
        self._api_key = os.getenv('COINALYZE_API_KEY')
        if self._api_key is None:
            raise ValueError('value of COINALYZE_API_KEY is None!')

        # param
        self.coinalyze_max_number_of_dp = 2000
        self.default_interval = '5min'
        self.convert_to_usd = 'true'
        
        # logger
        pwd = pathlib.Path(__file__).parent.resolve()
        curr_script_name = os.path.basename(__file__).split('.')[0]
        self.logger =  logger
            
    def _get(self, endpoint: str, params: Dict = None) -> List[Dict]:
        full_url = self.url + endpoint
        headers = {'api_key': self._api_key}

        # check params
        if params:
            if 'interval' in params:
                assert params['interval'] in ['1min', '5min', '15min', '30min', '1hour', '2hour', '4hour', '6hour', '12hour',
                                    'daily'], "<interval> shld be in [1min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 12hour, daily]"

            if 'to' not in params:
                params['to'] = math.floor(time.time())

            if 'from' not in params:
                # max out coinalyze's max number of lookback
                if params['interval'] == "1min":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 1
                elif params['interval'] == "5min":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 5
                elif params['interval'] == "15min":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 15
                elif params['interval'] == "30min":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 30
                elif params['interval'] == "1hour":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 60 * 1
                elif params['interval'] == "2hour":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 60 * 2
                elif params['interval'] == "4hour":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 60 * 4
                elif params['interval'] == "6hour":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 60 * 6
                elif params['interval'] == "12hour":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 60 * 12
                elif params['interval'] == "daily":
                    params['from'] = params['to'] - self.coinalyze_max_number_of_dp * 60 * 60 * 24 * 1

        response = requests.get(url=full_url, verify=self._ssl_verify, headers=headers, params=params)
        data_out = response.json()

        if 200 <= response.status_code <= 299:     # OK
            return data_out
        elif response.status_code == 429: # exceeded rate limit
            retry_after = float(response.headers['Retry-After']) # in seconds
            print(f'reached Coinalyze rate-limit, sleeping {retry_after} seconds before retrying...')
            time.sleep(retry_after)
            return self._get(endpoint, params)
        elif response.status_code == 500:
            retry_after = 5 # in seconds
            print(f'server error, sleeping {retry_after} seconds before retrying...')
            time.sleep(retry_after)
            return self._get(endpoint, params)
        raise Exception(data_out["message"])    # Todo: raise custom exception later

    def get_supported_exchanges(self) -> List[Dict]:
        return self._get('exchanges')
    
    def get_supported_future_markets(self) -> List[Dict]:
        return self._get('future-markets')
    
    def get_supported_spot_markets(self) -> List[Dict]:
        return self._get('spot-markets')
    
    def get_curr_open_interest(self, symbols: List[str]) -> List[Dict]:
        return self._get('open-interest', {"symbols": ','.join(symbols)})
        
    def get_curr_funding_rate(self, symbols: List[str]) -> List[Dict]:
        return self._get('funding-rate', {"symbols": ','.join(symbols)})

    def get_open_interest_history(self, symbols: List[str], interval: str, **kwargs) -> List[Dict]:
        ret = []

        for i in tqdm(range(0, len(symbols), 20)):
            params = {
                "symbols": ','.join(symbols),
                "interval": interval,
                "convert_to_usd": "true"
            }
            params.update(kwargs)

            part_ret = self._get('open-interest-history', params)
            ret.extend(part_ret)

        return ret
    
    def get_funding_rate_history(self, symbols: List[str], interval: str, **kwargs) -> List[Dict]:
        ret = []

        for i in tqdm(range(0, len(symbols), 20)):
            params = {
                "symbols": ','.join(symbols[i:i + 20]),
                "interval": interval,
            }
            params.update(kwargs)

            part_ret = self._get('funding-rate-history', params)
            ret.extend(part_ret)

        return ret
    
    def get_predicted_funding_rate_history(self, symbols: List[str], interval: str, **kwargs) -> List[Dict]:
        ret = []

        for i in tqdm(range(0, len(symbols), 20)):
            params = {
                "symbols": ','.join(symbols[i:i + 20]),
                "interval": interval,
            }
            params.update(kwargs)

            part_ret = self._get('predicted-funding-rate-history', params)
            ret.extend(part_ret)

        return ret
    
    def get_liquidation_history(self, symbols: List[str], interval: str, **kwargs) -> List[Dict]:
        ret = []

        for i in tqdm(range(0, len(symbols), 20)):
            params = {
                "symbols": ','.join(symbols[i:i + 20]),
                "interval": interval,
                "convert_to_usd": "true"
            }
            params.update(kwargs)

            part_ret = self._get('liquidation-history', params)
            ret.extend(part_ret)

        return ret

    def get_long_short_ratio_history(self, symbols: List[str], interval: str, **kwargs) -> List[Dict]:
        ret = []

        for i in tqdm(range(0, len(symbols), 20)):
            params = {
                "symbols": ','.join(symbols[i:i + 20]),
                "interval": interval,
            }
            params.update(kwargs)

            part_ret = self._get('long-short-ratio-history', params)
            ret.extend(part_ret)

        return ret

    def get_ohlcv_history(self, symbols: List[str], interval: str, **kwargs) -> List[Dict]:
        ret = []

        for i in tqdm(range(0, len(symbols), 20)):
            params = {
                "symbols": ','.join(symbols[i:i + 20]),
                "interval": interval,
            }
            params.update(kwargs)

            part_ret = self._get('ohlcv-history', params)
            ret.extend(part_ret)

        return ret

if __name__ == '__main__':
    cyz = CoinalyzeRestAdapter()
    
    ohlc = cyz.get_ohlcv_history(symbols=['ETHUSD.A'], interval='daily')
    
    import pandas as pd
    df = pd.DataFrame(ohlc[0]['history'], columns=['t', 'o', 'h', 'l', 'c', 'v'])
    print(df.shape)
    
    # def calc_ahr999_index(coinalyze: CoinalyzeRestAdapter) -> pd.DataFrame:
    #     ret = {}
        
    #     birth_date = datetime(year=2009, month=1, day=3)
    #     today = datetime.now()
    #     age_days = (today - birth_date).days
        
    #     target_price = 10 ** (5.84 * log10(age_days) - 17.01)
        
    #     param_to = math.floor(time.time())
    #     lookback_days = 201
    #     param_start = param_to - lookback_days * 24 * 60 * 60 # 22 days ago
    #     btc_prices = coinalyze.get_ohlcv_history(symbols=['BTCUSD.A'], **{'interval': 'daily', 'to': param_to, 'start': param_start})[0]['history']
        
    #     if len(btc_prices) < lookback_days:
    #         ret['success'] = False
    #         ret['data'] = f'coinalyze returned {len(btc_prices)} data points, but need 200 days'
    #         return ret
        
    #     price_sum = 0
    #     for price in btc_prices[:-1]:
    #         price_sum += btc_prices['c']
    #     average_200_days_price = price_sum / 200
        
    #     curr_price = btc_prices[-1]['c']
        
    #     p1 = curr_price / target_price
    #     p2 = curr_price / average_200_days_price
    #     p = p1 * p2
        

    
    
    # calc_ahr999_index(obj)
