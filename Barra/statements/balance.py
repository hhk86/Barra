# -*- coding: utf-8 -*-
from ..db.oracle import OracleSql
from datetime import datetime as dt
import pandas as pd
from ipdb import set_trace
from .factorsport import FactorsPort
from dateutil.relativedelta import relativedelta as td
from calendar import monthrange


# from time import time
# from decimal import Decimal as D
# from calendar import monthrange
# from ipdb import set_trace
# from bisect import bisect
#
# from MSSQL import *
#
# idx = pd.IndexSlice

# 数据库连接
class BalanceDataPort(object):
    def __init__(self):
        self.oracle = OracleSql()

    def raw(self, code_range, date_range, factor, factor_method='avg'):
        code_str = self._coderange2str(code_range)
        min_date, max_date = self._date_range_limits(date_range)
        factor_str = self._factor2str(factor)

        report_period_start = self._report_range_min(min_date)

        sql = "SELECT S_INFO_WINDCODE,ANN_DT,ACTUAL_ANN_DT,REPORT_PERIOD,STATEMENT_TYPE," + factor_str \
              + """ FROM ASHAREBALANCESHEET
                  WHERE STATEMENT_TYPE IN ('408001000','408004000','408005000') 
                        AND S_INFO_WINDCODE IN (%s) AND REPORT_PERIOD >= '%s'
                        AND ACTUAL_ANN_DT < '%s'
                  ORDER BY S_INFO_WINDCODE,REPORT_PERIOD,ACTUAL_ANN_DT""" % (
              code_str, report_period_start, max_date)
        columns = ['Code', 'AnnDate', 'ActualAnnDate', 'ReportPeriod', 'StatementType', factor_str]

        with self.oracle as orc:
            data = orc.query(sql)

        data.columns = columns
        data = data.set_index(['Code', 'ReportPeriod', 'ActualAnnDate'])
        data = data.sort_index(level=['ActualAnnDate', 'Code', 'ReportPeriod'])

        factor_port = FactorsPort(code_range)

        for item in data[data.index.get_level_values('ActualAnnDate') < date_range[0]].iterrows():
            # item[0]: code, reportperiod, actualanndate
            factor_port.push(item[0][0], item[0][1], item[0][2], item[1][factor_str])

        # latest_released_report为在指定交易日已知的最新财务报表报告期
        latest_released_report = pd.DataFrame([], columns=code_range, index=date_range)
        # latest_factor_value为最新财务报表的数据
        factor_values = pd.DataFrame([], columns=code_range, index=date_range)
        factor_values[factor_values.isnull()] = None

        snapshots = pd.DataFrame([], columns=code_range, index=date_range)

        season_table = pd.DataFrame([], columns=['prevQuarter', 'prevAnnual', 'lastyearQuarter'],\
                                    index=self._generate_season_table(report_period_start, max_date))

        season_table['prevQuarter'] = season_table.index.map(lambda s: self._prev_quarter(s))
        season_table['prevAnnual'] = season_table.index.map(lambda s: self._prev_annual(s))
        season_table['lastyearQuarter'] = season_table.index.map(lambda s: self._lastyear_quarter(s))

        date_beginning = date_range[0]

        data = data.reset_index().set_index(['ActualAnnDate', 'Code'])
        data = data.sort_index(level=['ActualAnnDate', 'Code'])

        # 产生初始值的公告日
        ann_date_container = data.index.get_level_values('ActualAnnDate').drop_duplicates().values
        ann_date_container = ann_date_container[ann_date_container >= date_beginning]

        func = self._find_eff_date(date_range)
        func.send(None)

        # # 以下计算初始值
        for code in latest_released_report.columns:
            if not factor_port.get_latest_report(code):
                continue

            latest_released_report.loc[latest_released_report.index[0], code] = factor_port.get_latest_report(code)
            if factor_method in ('avg'):
                try:
                    lastyearQuater = season_table.loc[latest_released_report.loc[latest_released_report.index[0], code], 'lastyearQuarter']
                    factor_values.loc[factor_values.index[0], code] = 0.5 * factor_port.top(code).factor \
                        + 0.5 * factor_port.get(code, lastyearQuater).factor
                except KeyError:
                    factor_values.loc[factor_values.index[0], code] = None
            elif factor_method == 'latest':
                factor_values.loc[factor_values.index[0], code] = factor_port.top(code).factor

            snapshots.loc[factor_values.index[0], code] = factor_port.snap(code, factor_port.get_latest_report(code))

        # print('Initialization Finished')

        for ann_date in ann_date_container:
            ipos, eff_date = func.send(ann_date)
            new_info = data.loc[ann_date, ['ReportPeriod', factor_str]]
            for new_factor in new_info.iterrows():
                # 需要更新处理的新信息：code, report_period, ann_date, factor
                code, report_period, factor = new_factor[0], new_factor[1]['ReportPeriod'], new_factor[1][factor_str]
                # 若数据过于陈旧，则本次更新取消，这里是非常陈旧，例如update 90年代的年报
                if report_period < report_period_start:
                    continue

                # 新数据推送入库之前的最新报告期
                old_latest_report = factor_port.get_latest_report(code)
                # 新数据推送入库
                factor_port.push(code, report_period, ann_date, factor)
                # 新数据推送入库后的最新报告期
                new_latest_report = factor_port.get_latest_report(code)

                snapshots.loc[eff_date, code] = factor_port.snap(code, factor_port.get_latest_report(code))
                # 至此，存在3个报告期，old_latest_report, report_period, new_latest_report
                # 如果：
                # 情形一：report_period < old_latest_report, 则 report_period < old_latest_report = new_latest_report
                # 情形二：report_period = old_latest_report，则 report_period = old_latest_report = new_latest_report
                # 情形三：report_period > old_latest_report，则 old_latest_report < report_period = new_latest_report
                # 更新factor_values值
                if (old_latest_report) and (report_period <= old_latest_report):
                    # 本次推送的数据是对之前报告数据的更新
                    assert old_latest_report == new_latest_report, "Error in old_latest_report == new_latest_report"
                    if factor_method in ('avg'):
                        try:
                            lastyearQuater_value = factor_port.get(code, season_table.loc[old_latest_report, 'lastyearQuarter']).factor
                            factor_values.loc[eff_date, code] = 0.5 * factor_port.get(code, old_latest_report).factor \
                                + 0.5 * lastyearQuater_value
                        except KeyError:
                            factor_values.loc[eff_date, code] = None
                    elif factor_method == 'latest':
                        factor_values.loc[eff_date, code] = factor_port.get(code, old_latest_report).factor
                else:
                    assert report_period == new_latest_report, "Error in report_period == new_latest_report"
                    # 新信息为新发布的财务报告
                    latest_released_report.loc[eff_date, code] = new_latest_report
                    if factor_method in ('avg'):
                        try:
                            lastyearQuater_value = factor_port.get(code, season_table.loc[new_latest_report, 'lastyearQuarter']).factor
                            factor_values.loc[eff_date, code] = 0.5 * factor_port.get(code, new_latest_report).factor \
                                + 0.5 * lastyearQuater_value
                        except KeyError:
                            factor_values.loc[eff_date, code] = None
                    elif factor_method == 'latest':
                        factor_values.loc[eff_date, code] = factor_port.get(code, new_latest_report).factor

        latest_released_report = latest_released_report.fillna(method='ffill')
        factor_values = factor_values.fillna(method='ffill')
        factor_values[factor_values.isnull()] = None
        snapshots = snapshots.fillna(method='ffill')

        return factor_values, snapshots, latest_released_report, data

    def _report_range_min(self, min_date):
        start_date = dt.strptime(min_date, '%Y%m%d')
        return dt(start_date.year - 7, 9, 30).strftime('%Y%m%d')

    def _date_range_limits(self, date_range):
        if isinstance(date_range, (bytes, str)):
            date_range = [date_range]
        date = list(map(lambda s: dt.strptime(s, '%Y%m%d'), date_range))
        return min(date).strftime('%Y%m%d'), max(date).strftime('%Y%m%d')

    def _coderange2str(self, code_range):
        if isinstance(code_range, (bytes, str)):
            code_range = [code_range]
        return '\'' + '\',\''.join(code_range) + '\''

    def _factor2str(self, factor):
        if factor == 'total_assets': # 总资产
            return 'TOT_ASSETS'
        elif factor == "total_equities_exc_min": # 股东权益-不含少数股东权益
            return "TOT_SHRHLDR_EQY_EXCL_MIN_INT"
        elif factor == 'total_equities_inc_min': # 股东权益-含少数股东权益
            return 'TOT_SHRHLDR_EQY_INCL_MIN_INT'
        elif factor == "noncur_liabilities": # 非流动性负债
            return "TOT_NON_CUR_LIAB"
        elif factor == "total_liabilities": # 总负债-即负债科目
            return "TOT_LIAB"
        elif factor == 'longterm_loan': # 长期负债
            return 'LT_BORROW'
        elif factor == 'bonds_payable': # 应付贷款
            return 'BONDS_PAYABLE'
        elif factor == 'longterm_payable': # 长期应付款
            return 'LT_PAYABLE'
        elif factor == 'preferred_stock': # 优先股
            return 'OTHER_EQUITY_TOOLS_P_SHR'
        elif factor == "cash":  #货币资金
            return "MONETARY_CAP"
        elif factor == "tradable_financialasset": # 交易性金融资产
            return "TRADABLE_FIN_ASSETS"
        elif factor == "notes_receiveable": # 应收票据
            return "NOTES_RCV"
        elif factor == "accounts_receivable": # 应收账款
            return "ACCT_RCV"
        elif factor == "inventory": # 存货
            return "INVENTORIES"
        elif factor == "fixed_asset": # 固定资产
            return "FIX_ASSETS"
        elif factor == "construction_inprogress": # 在建工程
            return "CONST_IN_PROG"
        elif factor == "intangible_asset": # 无形资产
            return "INTANG_ASSETS"
        elif factor == "development_expenditure": # 开发支出
            return "R_AND_D_COSTS"
        elif factor == "goodwill": # 商誉
            return "GOODWILL"
        elif factor == "notes_payable": #应付票据
            return "NOTES_PAYABLE"
        elif factor == "accounts_payable": #应付账款
            return "ACCT_PAYABLE"

    def _generate_season_table(self, min_date, max_date):
        d0 = self._nearby_season_month(min_date, -1)
        d1 = self._nearby_season_month(max_date, 0)
        datelist = [d0]
        while datelist[-1] < d1:
            datelist.append(self._nearby_season_month(datelist[-1], 1))
        return datelist

    def _nearby_season_month(self, datestr, offset):
        date = dt.strptime(datestr, '%Y%m%d')
        date = date.replace(day=1)
        season_month = date.replace(month=3 * (date.month // 3 + int(date.month % 3 > 0))) + td(months=3 * offset)
        return season_month.replace(day=monthrange(season_month.year, season_month.month)[1]).strftime('%Y%m%d')

    def _prev_quarter(self, datestr):
        return self._nearby_season_month(datestr, -1)

    def _prev_annual(self, datestr):
        date = dt.strptime(datestr, '%Y%m%d')
        return date.replace(year=date.year - 1, month=12, day=31).strftime('%Y%m%d')

    def _lastyear_quarter(self, datestr):
        return self._nearby_season_month(datestr, -4)

    def _find_eff_date(self, date_range):
        ipos = 0
        while ipos < len(date_range):
            basedate = yield ipos, date_range[ipos]
            while date_range[ipos] <= basedate:
                ipos = ipos + 1

    def calendar(self, date0, date1):
        sql = '''SELECT trade_days FROM ASHARECALENDAR 
            WHERE TRADE_DAYS>='%s' AND TRADE_DAYS<='%s' AND S_INFO_EXCHMARKET='SSE'
            ORDER BY TRADE_DAYS''' % (date0, date1)
        with self.oracle as orc:
            data = orc.query(sql).squeeze().tolist()
        return data

    def universe(self, date):
        sql = '''SELECT S_INFO_WINDCODE FROM ASHAREDESCRIPTION WHERE S_INFO_LISTDATE IS NOT NULL 
                 AND S_INFO_LISTDATE <'%s' AND S_INFO_DELISTDATE IS NULL ORDER BY S_INFO_WINDCODE''' % (date)
        with self.oracle as orc:
            data = orc.query(sql).squeeze().tolist()
        return data


