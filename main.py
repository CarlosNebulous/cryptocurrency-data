import requests
from datetime import datetime
import pandas as pd
import cryptocompare
from tqdm import tqdm
import numpy as np


def get_filename(from_symbol, to_symbol, exchange, datetime_interval, download_date):
    return '%s_%s_%s_%s_%s.csv' % (from_symbol, to_symbol, exchange, datetime_interval, download_date)


def download_data(from_symbol, to_symbol, exchange, datetime_interval):
    supported_intervals = {'minute', 'hour', 'day'}
    assert datetime_interval in supported_intervals,\
        'datetime_interval should be one of %s' % supported_intervals
    print('Downloading %s trading data for %s %s from %s' %
          (datetime_interval, from_symbol, to_symbol, exchange))
    base_url = 'https://min-api.cryptocompare.com/data/histo'
    url = '%s%s' % (base_url, datetime_interval)
    params = {'fsym': from_symbol, 'tsym': to_symbol,
              'limit': 400, 'aggregate': 1}  # this will change the exchange parameter: 'e': exchange}
    request = requests.get(url, params=params)
    data = request.json()
    return data


def convert_to_dataframe(data, symbol, image_url, coin_name):
    df = pd.json_normalize(data, ['Data'])
    df['datetime'] = pd.to_datetime(df.time, unit='s')
    df['cryptocurrency'] = symbol
    df['image_url'] = image_url
    df['coin_name'] = coin_name
    df = df[['datetime', 'low', 'high', 'open', 'close', 'volumefrom', 'volumeto', 'cryptocurrency', 'image_url',
             'coin_name']]
    return df


def filter_empty_datapoints(df):
    indices = df[df.sum(axis=1) == 0].index
    print('Filtering %d empty datapoints' % indices.shape[0])
    df = df.drop(indices)
    return df


def get_coins_higher_price(number_coins: int = 200):
    """
    Get the coins with the currently higher price
    :param number_coins: the number of coins you want to include
    :return: a dict with all the info about the coins with higher price
    """
    coins_dict = cryptocompare.get_coin_list(format=False)
    coins_names_list = list(coins_dict.keys())
    chunk_coins = np.array_split(coins_names_list, 200)  # due to limits I have to split the coins in 200 parts
    prices_in_USD = {}
    print('Getting cripto currencies prices:')
    for chunk in tqdm(chunk_coins):
        try:
            chunk_prices = cryptocompare.get_price(coin=list(chunk), curr='USD')
            chunk_dict_one_level = {}
            for cripto in chunk_prices:
                chunk_dict_one_level[cripto] = chunk_prices[cripto]['USD']
            prices_in_USD.update(chunk_dict_one_level)
        except Exception as e:
            print(e)
    sorted_criptos = dict(sorted(prices_in_USD.items(), key=lambda item: item[1]))
    final_criptos = list(sorted_criptos.keys())[-number_coins:]
    final_dict = {}
    for money in final_criptos:
        if money in coins_dict:
            final_dict[money] = coins_dict[money]
    return final_dict


if __name__ == '__main__':
    to_symbol = 'USD'
    datetime_interval = 'day'
    data_frame = pd.DataFrame(columns=['datetime', 'low', 'high', 'open', 'close', 'volumefrom', 'volumeto',
                                       'cryptocurrency', 'image_url', 'coin_name'])
    crypt_skipped = []
    # Unused function, if better to use the one already built in
    # coins_dict = get_coins_higher_price()
    params = {'limit': '100', 'tsym': 'USD'}
    coins_dict = requests.get('https://min-api.cryptocompare.com/data/top/totalvolfull', params=params).json()['Data']
    print('Getting data about currencies:')

    for coin in tqdm(coins_dict):
        try:
            from_symbol = coin['CoinInfo']['Internal']
            name = coin['CoinInfo']['FullName']
            image_url = 'https://www.cryptocompare.com' + coin['CoinInfo']['ImageUrl']
            data = download_data(from_symbol, to_symbol, 'default', datetime_interval)
            df = convert_to_dataframe(data, from_symbol, image_url, name)
            # df = filter_empty_datapoints(df)
            data_frame = pd.concat([data_frame, df])
        except Exception as e:
            print(e)
            crypt_skipped.append(name)

    current_datetime = datetime.now().date().isoformat()
    filename = get_filename('Cryptocurrencies_to', to_symbol, 'default', datetime_interval, current_datetime)
    print('Saving data to %s' % filename)
    data_frame.to_csv(filename, index=False)
    print(len(crypt_skipped))
