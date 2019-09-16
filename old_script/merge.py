import pandas as pd
import sys

if __name__ == "__main__":
    sys.exit()
    position = pd.read_excel("C:\\Users\\huang\\Desktop\\analysis\\305持仓.xlsx", header=4, dtype={'证券代码': str})
    full_table = pd.read_csv("C:\\Users\\huang\\Desktop\\analysis\\full_table.csv", encoding="gbk", dtype={'code': str, "tradeday": str})
    one_day_table = full_table[full_table["tradeday"] == "20190328"]
    df = pd.merge(position, full_table, left_on="证券代码", right_on="code", how="left")
    df.drop(["交易市场", "报盘股东", "股东代码", "股东姓名", "股份余额", "股份可用数", "卖出冻结", "买入冻结", "非交易冻结", "限售股份", "昨日余股", "席位代码",
             "权益数量", "权益冻结数量", "资金帐号", "客户代码", "营业部", "质押券余额", "质押券可用数", "标准券余额/元", "标准券可用/元", "待交收数量", "可售冻结余额",
             "不可售冻结余额", "净资本类别", "已融券数量", "当日借券余额", "借券卖出冻结", "综合平台当日回转", "综合平台买入冻结", "流通股市值", "限售股市值", "非流通股市值",
             "股份状态", "账户属性", "可卖数量", "结算币种实时市值", "参考汇率", "结算币种最新价", "结算币种昨日买入成本", "结算币种实时成本", "今日卖出金额", "今日买入费用",
             "今日卖出费用", "今日净买入数量", "今日净买入金额", "今日买入浮动盈亏", "今日卖出浮动盈亏", "当前累计卖出盈亏", "结算币种今日买入金额", "结算币种今日卖出金额",
             "结算币种今日买入费用", "结算币种今日卖出费用", "结算币种今日净买入金额", "结算币种今日买入浮动盈亏", "结算币种今日卖出浮动盈亏", "结算币种当前累计卖出盈亏"],
            axis=1, inplace=True)

    yinhuarili = df.iloc[1,:]
    jianhuitianyi = df.iloc[0, :]
    df = df[df["tradeday"] == "20190823"]
    yinhuarili = yinhuarili[: df.shape[1]]
    jianhuitianyi = jianhuitianyi[: df.shape[1]]
    df = df.append(yinhuarili)
    df = df.append(jianhuitianyi)
    print(yinhuarili)
    print(jianhuitianyi)
    print(df)
    df.to_excel("C:\\Users\\huang\\Desktop\\analysis\\X305_analysis.xlsx")
