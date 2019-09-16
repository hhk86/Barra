from Barra.statements.income import IncomeDataPort
from Barra.statements.balance import BalanceDataPort
from Barra.statements.cashflow import CashflowDataPort
from jinja2 import Template
import pandas as pd
import numpy as np
from dateutil.parser import parse

import sys
try:
    sys.path.append("C:\\Programs\\Tinysoft\\Analyse.NET")
    import TSLPy3 as tsl3
except ImportError:
    sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
    import TSLPy3 as tsl3

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

    def _fetch(self, tsl):
        "Get data through tinysoft interface."
        fail, data, _ = self.ts.RemoteExecute(tsl, {})
        if not fail:
            return data
        else:
            raise Exception("Error when execute tsl")

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


class IncomeTs(object):
    def __init__(self):
        self.ts = TsPy()

    def ttm(self, entry, codes, start_date=None, end_date=None, tradeday=None):
        if all([start_date, end_date]) and tradeday is None:
            pass
        elif (tradeday is not None) and all([start_date is None, end_date is None]):
            start_date, end_date = tradeday, tradeday
        else:
            raise Exception('Income.ttm: 日期格式混乱')

        codes = list(map(lambda s: s[-2:] + s[:6], codes))
        start_date = parse(start_date).strftime('%Y-%m-%d')
        end_date = parse(end_date).strftime('%Y-%m-%d')

        entries = {'revenue': 46002, 'net_income': 46033,
                   'net_income2major': 46078, 'cost': 46005,
                   'total_asset': 44059, 'operating_cashflow': 48018}

        tsl_temp = Template('''
            stockarray:=array('{{codes}}');
            begt:=strtodate('{{startdate}}');
            endt:=strtodate('{{enddate}}') + 0.99;
            total:=array();
            dates:=MarketTradeDayQk(begt,endt);
            for i:=0 to length(stockarray)-1 do
            begin
              for j:=0 to length(dates)-1 do
              begin
                  setsysparam(pn_stock(),stockarray[i]);
                  setsysparam(pn_date(), dates[j]);
                  RDate:=NewReportDateOfEndT2(dates[j]);
                  v:=Last12MData(RDate,{{entrynum}});
                  total union= ``array('ticker':stockarray[i],
                                       'date':datetostr(dates[j]),
                                       '{{entry}}':v);
              end
            end
            return total;        
        ''')
        tsl = tsl_temp.render(codes="','".join(codes),
                             startdate=start_date, enddate=end_date,
                             entry=entry, entrynum=str(entries[entry]))
        with self.ts as ts:
            df = ts.calltsl(tsl, None, ['date', 'ticker']).squeeze()
        df = pd.DataFrame(df).reset_index()
        df['date'] = pd.to_datetime(df['date'].astype('str')).values
        df['ticker'] = list(map(lambda s: s[2:] + '.' + s[:2], df['ticker']))
        df = df.astype({entry: 'float64'})
        series = df.set_index(['date', 'ticker']).squeeze()
        series.name = entry
        return series


if __name__ == '__main__':
    income_statement = IncomeDataPort()
    codes = income_statement.universe('20140430')
    date_range = income_statement.calendar('20100101', '20170707')

    codes = codes[:20]
    codes =["002279.SZ", ]
    # date_range = date_range[:20]

    income_statement = IncomeDataPort()
    # factor_values, _, _ = income_statement.raw(codes, date_range, 'net_income', factor_method='ttm')
    factor_values,  _, _ = income_statement.raw(codes, date_range, 'total_revenue', factor_method='ttm')
    values = IncomeTs().ttm('net_income2major', codes[:20], min(date_range), max(date_range))
    print(factor_values)

    # balance_sheet = BalanceDataPort()
    # # factor_values, _, _ = balance_sheet.raw(codes, date_range, 'total_asset', factor_method='latest')
    # factor_values, _, _ = balance_sheet.raw(codes, date_range, 'total_debt', factor_method='latest')
    # # values = IncomeTs().ttm('total_asset', codes[:20], min(date_range), max(date_range))
    # pd.set_option("display.max_columns", None)
    # print(factor_values)

    # cashflow_statement = CashflowDataPort()
    # factor_values, _, _ = cashflow_statement.raw(codes, date_range, 'operating_cashflow', factor_method='ttm')
    # factor_values, _, _ = cashflow_statement.raw(codes, date_range, 'cashflow', factor_method='ttm')
    # values = IncomeTs().ttm('operating_cashflow', codes[:20], min(date_range), max(date_range))
    # print(factor_values)


