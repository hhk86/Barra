# -*- coding: utf-8 -*-
'''
Created on 2019.1.25

@author: DoubleZ
'''
import pandas as pd
from dateutil.parser import parse
from jinja2 import Template
import numpy as np

import sys
try:
    sys.path.append("C:\\Programs\\Tinysoft\\Analyse.NET")
    import TSLPy3 as tsl3
except ImportError:
    sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
    import TSLPy3 as tsl3


__all__ = ['TsPy']

# TinySoft Server Infomation
TinySoft_Server = "tsl.tinysoft.com.cn", 443
User_Pwd = "fzzqjyb", "123456"


# _TsPy metaclass, indeed same as type until now
class TsPyMeta(type):
    def __new__(cls, *args, **kwargs):
        return super(TsPyMeta, cls).__new__(cls, *args, **kwargs)

    def __init__(cls, *args, **kwargs):
        super(TsPyMeta, cls).__init__(*args, **kwargs)


class TsPy(object):
    """
    TsInterface provide an uniform interface to Tinysoft server.
    The tinysoft server return values
    with format "[{...},{...},...]" and "gbk" coding.
    In this class, the list return values are converted
    into pandas DataFrame, with unicode coding.
    """
    __metaclass__ = TsPyMeta
    _instance = None

    def __new__(cls):
        '''
        This class is singleton mode.
        '''
        if cls._instance is None:
            obj = super(TsPy, cls).__new__(cls)
            cls._instance = obj
        return cls._instance

    def __init__(self):
        '''
        connect tinysoft server by calling self.start()
        '''
        super(TsPy, self).__init__()
        self.isconnected = False
        self.ts = tsl3

    def __enter__(self):
        self.start()
        self.isconnected = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.isconnected = False

    def __del__(self):
        if self.isconnected:
            self.stop()

    def start(self):
        '''
        connect tinysoft server.
        '''
        if not self.isconnected:
            fail, _, _ = self.ts.RemoteExecute('return 1;', {})
            if fail:
                self.ts.ConnectServer(*TinySoft_Server)
                dl = self.ts.LoginServer(*User_Pwd)
                if dl[0] != 0:
                    raise Exception("TS server Reloginning Refused!")
                self.isconnected = self.ts.Logined()
            print("connect to Ts server")

    def stop(self):
        '''
        Disconnect tinysoft server.
        '''
        self.ts.Disconnect()
        print('disconnect with Ts server.')

    def calltsl(self, tsl, columns=None, index_names=None):
        raw_data = self._fetch(tsl)
        if raw_data:
            return _ts_rawdata_to_dataframe(raw_data, columns, index_names)
        else:
            return None

    def _fetch(self, tsl):
        "Get data through tinysoft interface."
        fail, data, _ = self.ts.RemoteExecute(tsl, {})
        if not fail:
            return data
        else:
            raise Exception("Error when execute tsl")


def _ts_rawdata_to_dataframe(raw_data, columns, index_names):
    """
    This function convert raw data returned by Tinysoft
    into pd.DataFrame. The string of 'gbk' format is also
    converted into unicode.
    """
    array_data = np.asanyarray(raw_data)
    if isinstance(array_data[0], dict):
        array_data = _dict_to_series_with_decode('gbk', array_data)
        df_data = pd.concat(array_data, axis=1).transpose()
    elif isinstance(array_data[0], (str, bytes)):
        df_data = pd.DataFrame(array_data)
    elif isinstance(array_data[0], np.ndarray):
        df_data = pd.DataFrame.from_records(array_data)
    else:
        raise NotImplementedError()
    # reindex columns
    if columns:
        df_data.columns = columns
    # set index
    if index_names:
        df_data.set_index(index_names, inplace=True)
    # return result
    return df_data.sort_index()


@np.vectorize
def _dict_to_series_with_decode(coding, dict_):
    """
    This function convert dict of raw value into pd.Series.
    Any key/value with 'gbk' format will be converted into unicode.
    """
    new_dict = {}
    for key, val in dict_.items():
        # decode any key with string format
        if isinstance(key, (str, bytes)):
            key = key.decode(coding)
        # decode any value with string format
        if isinstance(val, (str, bytes)):
            val = val.decode(coding)
        # construct new dict
        new_dict[key] = val
    # convert new dict into series and return
    return pd.Series(new_dict)


# universe
class Universe(object):
    _instance = None
    _init_flag = True

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Universe, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, *args, **kwargs):
        if self.__class__._init_flag:
            self.__class__._init_flag = False

    def getbk(self, ranges, tradeday=None):
        '''
        return stock range.

        Arguments
        ---------
        ranges: list
        start_date: str like '%Y-%m-%d'
        end_date: str like '%Y-%m-%d'
        tradeday: str like '%Y-%m-%d'
            either start_date/end_date or tradeday must be specified.

        Returns
        -------
        stk list
        '''
        # ranges(list) into str
        tradeday = parse(tradeday).strftime('%Y-%m-%d')

        # getbk
        tsql_temp = Template('''
                datearray:= MarketTradeDayQk(strtodate('{{tradeday}}'),
                                             strtodate('{{tradeday}}')+0.99);
                results := array();
                for i:=0 to length(datearray)-1 do
                begin
                    tradeday := datearray[i];
                    results := results | select thisrow as datetostr(tradeday)
                                         from getabkbydate('{{range}}', tradeday) end;
                end
                return results;
                ''')

        tsql = tsql_temp.render(tradeday=tradeday, range=ranges)
        print(tsql)
        with TsPy() as ts:
            results = ts.calltsl(tsql).squeeze()

        if isinstance(results, pd.Series):
            results = list(map(lambda s: '.'.join([s[-6:], s[:2]]), results.tolist()))

        return results


if __name__ == '__main__':
    a = Universe().getbk('A股', '20190115')
    print(a)