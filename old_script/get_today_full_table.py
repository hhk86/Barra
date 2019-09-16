from makeDailyUniverse import *
from makeStkName import *
from makeStkPrice import *
from makeStkCiticIndustry import *
from makeStkConstituent import *

if __name__ == "__main__":
    start_date = "20190701"
    end_date = "20190823"
    tradedays = getTradeCalendar(start_date, end_date)
    daily_universe = pd.DataFrame([])
    for tradeday in tradedays:
        daily_universe = daily_universe.append(makeDailyUniverse(tradeday))
    name = makeStkName(start_date, end_date, daily_universe)
    industry = makeStkCiticsIndustry(start_date, end_date, daily_universe)
    constituent = makeStkConstituent(start_date, end_date, daily_universe)
    price = makeStkPrice(start_date, end_date, daily_universe)
    df = pd.merge(name, industry, on=["tradeday", "ticker"], how="left")
    df = pd.merge(df, constituent, on=["tradeday", "ticker"], how="left")
    df = pd.merge(df, price, on=["tradeday", "ticker"], how="left")
    print(df)
    df.to_csv("full_table.csv", encoding="gbk")