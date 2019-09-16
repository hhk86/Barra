from makeDailyUniverse import *
from makeDailyUniverse2 import makeDailyUnverse2
from makeTradeCalendar import getTradeCalendar

if __name__ == "__main__":
    trade_date_list = getTradeCalendar("20170801", "20170820")
    for date in trade_date_list:
        stocks_from_description =  makeDailyUnverse(date)
        stocks_from_EODPrices = makeDailyUnverse2(date)
        if len(stocks_from_description) != len(stocks_from_EODPrices):
            print('~' * 100)
            print("Not equal", date)
        for ticker in stocks_from_description.ticker:
            if ticker not in stocks_from_EODPrices.ticker.tolist():
                print("Lack ticker", ticker, date)
        print("Complete", date)

