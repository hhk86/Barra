from raw import *
import cx_Oracle
import numpy as np
import pandas as pd
from multiprocessing import Process, Queue, Value


class OracleSql(object):
    '''
    Oracle数据库数据访问

    '''

    def __init__(self):
        '''
        初始化数据库连接
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'

    def __enter__(self):
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
        except Exception:
            print('不能连接oracle数据库')
            connection = None
        return connection

    def query(self, sql):
        '''
        查询并返回数据

        '''
        return pd.read_sql(sql, self.conn)

    def execute(self, sql):
        '''
        对数据库执行插入、修改等数据上行操作

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()


def getTradeCalendar(start_date: str, end_date: str) -> list:
    '''
    获取某一日期区间内的所有交易日（包括起始日期和终止日期）。
    :param start_date: str, 起始日期, "YYYMMDD"
    :param end_date:str, 终止日期, "YYYMMDD"
    :return: list, 交易日列表
    '''
    sql = \
        '''
        SELECT
            TRADE_DAYS 
        FROM
            asharecalendar
        WHERE
            S_INFO_EXCHMARKET = 'SSE' 
            AND trade_days BETWEEN {} AND {}
    '''.format(start_date, end_date)
    with OracleSql() as oracle:
        tradingDays = oracle.query(sql)
    return sorted(tradingDays.TRADE_DAYS.tolist())


def get_all_available_stocks(start_date:str, end_date:str) -> pd.DataFrame:
    '''
    获取某一日起区间内所出现过的所有股票的代码、上市日和退市日
    :param start_date: str, 初始日期, “YYYYMMDD”
    :end_date: str, 结束日期，“YYYYMMDD”
    :return: pd.DataFrame, columns = [S_INFO_WINDCODE -]
    '''
    sql = \
        '''
        SELECT
            S_INFO_WINDCODE, S_INFO_LISTDATE, S_INFO_DELISTDATE
        FROM
            ASHAREDESCRIPTION
        WHERE
            S_INFO_LISTDATE <= {1} AND S_INFO_LISTDATE <= '20190731'
            AND (S_INFO_DELISTDATE >= {0} OR S_INFO_DELISTDATE IS NULL)
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        all_available_stocks = oracle.query(sql)
        all_available_stocks = all_available_stocks[all_available_stocks["S_INFO_WINDCODE"].apply(
            lambda s: s[0] in "036T")]

    return all_available_stocks


def subMakeFactor(stock_slice: pd.DataFrame, dataPort, factor: str,  factor_method: str, q: Queue, i: Value):
    factor_df = pd.DataFrame(columns=[factor, "ticker"])
    for _, record in stock_slice.iterrows():
        try:
            if record["S_INFO_WINDCODE"] == "000498.SZ":
                continue
            codes = [record["S_INFO_WINDCODE"], ]
            date_range = dataPort.calendar(record["start_dt"], record["end_dt"])
            factor_values, snapshots, _, _ = dataPort.raw(codes, date_range, factor, factor_method=factor_method)
            df = pd.DataFrame(index=factor_values.index)
            df[factor] = factor_values.iloc[:, 0]
            df[factor + "_snapshots"] = snapshots.iloc[:, 0]
            df["ticker"] = codes[0]
            factor_df = factor_df.append(df)
            i.value += 1
            print('\rMaking financial factor ' + str(i.value), end= " ")
        except Exception as e:
            print(codes)
            print(date_range)
            raise (e)
    q.put(factor_df)



def make_financial_factor(start_date, end_date, factor, test_mode=False):
    pd.set_option("display.max_columns", None)
    all_available_stocks = get_all_available_stocks(start_date, end_date)
    all_available_stocks["end_dt"] = np.where(all_available_stocks["S_INFO_DELISTDATE"].isna(), end_date,
                                              all_available_stocks["S_INFO_DELISTDATE"])
    all_available_stocks["start_dt"] = all_available_stocks["S_INFO_LISTDATE"].apply(
        lambda date: start_date if date < start_date else date)
    all_available_stocks = all_available_stocks[all_available_stocks["S_INFO_WINDCODE"] != "600087.SH"]
    if test_mode is True:
        all_available_stocks = all_available_stocks[:50]
    income_statement = IncomeDataPort()
    balance_sheet = BalanceDataPort()
    cashflow_statement = CashflowDataPort()
    i = 0
    factor_df = pd.DataFrame(columns=[factor, "ticker"])
    if factor in ["net_income", "total_revenue",
                "revenue", "total_opcost", "operating_cost", "sale_expense",
                "management_expense", "research_expense", "financial_expense", "operating_profit"]:
        dataPort = income_statement
        factor_method = "ttm"
    elif factor in ['total_assets', 'total_equities_exc_min', 'total_equities_inc_min',
                    'noncur_liabilities', 'total_liabilities',
                    'longterm_loan', 'bonds_payable', 'longterm_payable', 'preferred_stock',
                    "cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
                    "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                    "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
        dataPort = balance_sheet
        factor_method = "latest"
    elif factor in ["operating_cashflow",
                    "operating_cashinflow", "operating_cashoutflow", "investment_cashinflow", "investment_cashoutflow",
                    "investment_cashflow", "finance_cashinflow", "finance_cashoutflow", "finance_cashflow"]:
        dataPort = cashflow_statement
        factor_method = "ttm"


    for _, record in all_available_stocks.iterrows():
        try:
            if record["S_INFO_WINDCODE"] == "000498.SZ":
                continue
            codes = [record["S_INFO_WINDCODE"], ]
            date_range = dataPort.calendar(record["start_dt"], record["end_dt"])
            factor_values, snapshots, _, _ = dataPort.raw(codes, date_range, factor, factor_method=factor_method)
            df = pd.DataFrame(index=factor_values.index)
            df[factor] = factor_values.iloc[:, 0]
            df[factor + "_snapshots"] = snapshots.iloc[:, 0]
            df["ticker"] = codes[0]
            factor_df = factor_df.append(df)
            i += 1
            print('\rMaking financial factor ' + str(i), end= " ")
        except Exception as e:
            print(codes)
            print(date_range)
            raise (e)

    # Multiprocess part
    #
    # n = 2
    # q = Queue()
    # i = Value("d", 0)
    # jobs = list()
    # N = len(all_available_stocks)
    # for j in range(0, n):
    #     stock_slice = all_available_stocks.iloc[N * j // n : N * (j + 1) // n, :]
    #     p = Process(target=subMakeFactor, args=(stock_slice, dataPort, factor, factor_method, q, i))
    #     jobs.append(p)
    #     p.start()
    #     print("Start Process", i)
    # for i in range(0, n):
    #     factor_df = factor_df.append(q.get())
    # for job in jobs:
    #     job.join()
    # print(factor_df)




    factor_df["tradeday"] = factor_df.index
    factor_df.index = range(factor_df.shape[0])
    factor_df = factor_df[["tradeday", "ticker", factor, factor + "_snapshots"]]
    if factor in ["longterm_loan", "bonds_payable", "longterm_payable", "preferred_stock",
                  "cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
                  "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                  "development_expenditure", "goodwill", "notes_payable", "accounts_payable",
                  "revenue", "total_opcost", "operating_cost", "sale_expense",
                  "management_expense", "research_expense", "financial_expense", "operating_profit",
                  "operating_cashinflow", "operating_cashoutflow", "investment_cashinflow", "investment_cashoutflow",
                  "investment_cashflow", "finance_cashinflow", "finance_cashoutflow", "finance_cashflow"]:
        # In this circumstance, None or NaN mean not missing but 0 in the financial statements
        print("fill out NaN and None")
        factor_df[factor] = factor_df[factor].apply(lambda x: 0 if pd.isna(x) else x)
        factor_df[factor + "_snapshots"] = factor_df[factor + "_snapshots"].apply(lambda s: s.replace("None", '0'))
    factor_df.loc[factor_df[factor].isnull(), factor] = None

    factor_df.to_csv(factor + ".csv", encoding="gbk")
    return factor_df

def fill_out_na(snapshots):
    for key, value in snapshots.items():
        if pd.isna(value):
            snapshots[key] = 0
    return snapshots


if __name__ == "__main__":

    # all_available_stocks["first_letter"] = all_available_stocks["S_INFO_WINDCODE"].apply(lambda s: s[0])
    print(make_financial_factor("20090101", "20090228", "notes_receiveable", test_mode=True))
