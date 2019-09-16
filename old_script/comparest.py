from makeStkST import *
from makeStkName import *

def parse(s):
    if s.startswith("ST"):
        return "ST"
    elif s.startswith("*ST"):
        return "*ST"
    elif s.startswith("SST"):
        return "SST"
    elif s.startswith("S*ST"):
        return "S*ST"
    elif s.startswith("PT"):
        return "PT"
    elif s.startswith('*'):
        return 'S'
    elif s.startswith('*'):
        return '*'
    else:
        return s

if __name__ == "__main__":
    df1 = makeStkST("20190525", "20190601")
    df2 = makeStkName("20190525", "20190601")
    df = pd.merge(df1, df2, on=["tradeday", "ticker"])
    df["从名字解析"] = df["name"].apply(parse)
    df.to_csv("debug.csv", encoding="gbk")
    print(df)