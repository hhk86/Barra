# -*- coding: utf-8 -*-
from ipdb import set_trace
import datetime as dt
import pandas as pd

class FactorNode(object):
    def __init__(self, ann_date, factor):
        self.ann_date = ann_date
        if pd.isna(factor):
            factor = None # float('nan')
        self.factor = factor

    def show(self):
        print(self.ann_date, self.factor)


class FactorStack(object):
    def __init__(self, ana_date=None, factor=None):
        self.values = []

        if ana_date:
            if not pd.isna(factor):
                self.push(ana_date, factor)
            else:
                self.push(ana_date, None)
        else:
            raise Exception('ann_date missing')

    def push(self, ana_date, factor):
        self.values.append(FactorNode(ana_date, factor))

    def top(self):
        try:
            return self.values[-1]
        except Exception as e:
            raise Exception('FactorStack.top')

    def show(self):
        for factor_node in self.values:
            factor_node.show()


class StkNode(object):
    def __init__(self, ticker, report_period, ana_date, factor):
        self.ticker = ticker
        self.latest_report = report_period
        self.values = {report_period: FactorStack(ana_date,factor)}

    def push(self, report_period, ana_date, factor):
        if report_period > self.latest_report:
            self.values[report_period] = FactorStack(ana_date, factor)
            self.latest_report = report_period
        else:
            try:
                self.values[report_period].push(ana_date, factor)
            except KeyError:
                self.values[report_period] = FactorStack(ana_date, factor)
                self.values[report_period].push(ana_date, factor)
                print(self.ticker + '的' + report_period + '报告晚于通常情况')
            except Exception as e:
                print(e)
                raise Exception('StkNode.push')

    def get(self, report_period):
        return self.values[report_period].top()

    def get_latest_report(self):
        return self.latest_report

    def top(self):
        return self.values[self.latest_report].top()

    def show(self):
        for report_period, factorstack in self.values.items():
            print('Report Period: '+ report_period)
            factorstack.show()


class FactorsPort(object):
    def __init__(self, tickers):
        self._data = dict.fromkeys(tickers, None)

    def push(self, ticker, report_period, ana_date, factor):
        if not self._data[ticker]:
            self._data[ticker] = StkNode(ticker, report_period, ana_date, factor)
        else:
            self._data[ticker].push(report_period, ana_date, factor)
        pass

    def get(self, ticker, report_period):
        return self._data[ticker].get(report_period)

    def get_latest_report(self, ticker):
        try:
            return self._data[ticker].get_latest_report()
        except AttributeError:
            # self._data全部为None值
            return
        except Exception:
            raise Exception('FactorsPort.get_latest_report')

    def top(self, ticker):
        return self._data[ticker].top()

    def show(self):
        for k, v in self._data.items():
            print("Code is " + k)
            if v is not None:
                v.show()

    def snap(self, ticker, report_date):
        all_report_dates = self._data[ticker].values.keys()
        report_date = dt.datetime.strptime(report_date, '%Y%m%d')
        start_report_date = report_date.replace(year=report_date.year - 5).strftime('%Y%m%d')

        results = {}
        for rdate in all_report_dates:
            if rdate >= start_report_date:
                try:
                    results[rdate] = self._data[ticker].values[rdate].top().factor
                except:
                    print('haha')

        if results:
            return str(results)
        else:
            return ''

