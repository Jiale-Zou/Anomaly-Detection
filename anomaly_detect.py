import numpy as np
import sys
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.sql.execution.arrow.enabled", "true") \
.config('spark.sql.execution.arrow.pyspark.enabled',"true").enableHiveSupport().getOrCreate()


def get_day_data(today, length, usersource):
    '''
    获取today日期前length长度的日度数据
    '''

    sql = f'''
    select
    label_period
    ,sum(case when paycategory='高费率信用卡' then self_amt end) highRate_amt
    ,sum(self_amt) self_amt
    ,sum(amt) amt
    from report_paydb.adm_kpi_selfpay_period
    where period = '日'
    and usersource = '{usersource}'
    and label_period between date_sub('{today}', {length}) and '{today}'
    group by 1
    order by 1 asc
    '''

    return sql


def zscore_bins(data, threshold=2):
    '''
    使用Z-score得到正常区间
    threshold: 取2, 2.5或3（对应95%, 99%, 99.7%置信区间）
    '''
    mean = np.mean(data)
    std = np.std(data)

    return round(mean - threshold * std, 4), round(mean + threshold * std, 4)


def get_bins(today, usersource, length=295):
    res = {
        'today': today,
        'usersource': usersource,
    }

    # 1.获取当年数据
    sql1 = get_day_data(today, length, usersource)
    day_data = spark.sql(sql1).toPandas()
    day_data['self_rt'] = day_data['self_amt'] / day_data['amt']
    day_data['highRate_rt'] = day_data['highRate_amt'] / day_data['amt']
    # 2.获取去年同期数据
    sql2 = get_day_data(str(int(today[:4]) - 1) + today[-6:], length, usersource)
    day_data_before = spark.sql(sql2).toPandas()
    day_data_before['self_rt'] = day_data_before['self_amt'] / day_data_before['amt']
    day_data_before['highRate_rt'] = day_data_before['highRate_amt'] / day_data_before['amt']
    # 3.自有支付日
    window = 7
    data_rolling = day_data['self_rt'].rolling(window=window)
    rolling_mean = data_rolling.mean().dropna()
    t = (data_rolling.apply(lambda x: x[-1]).dropna() / rolling_mean).reset_index(drop=True)
    data_rolling_before = day_data_before['self_rt'].rolling(window=window)
    rolling_mean_before = data_rolling_before.mean().dropna()
    t_before = (data_rolling_before.apply(lambda x: x[-1]).dropna() / rolling_mean_before).reset_index(drop=True)
    processed_data = (t / t_before).dropna()
    bottom, top = zscore_bins(processed_data)
    res['self_rt_day_top'] = top
    res['self_rt_day_bottom'] = bottom
    # 4.自有支付周
    week_data = pd.DataFrame(
        data={'this_year_self': day_data['self_amt'].values,  # 直接传递Series，Pandas会尝试按照 Series 的索引对齐数据
              'this_year_amt': day_data['amt'].values,
              'last_year_self': day_data_before['self_amt'].values,
              'last_year_amt': day_data_before['amt'].values
              },
        index=day_data['label_period'].values
    )
    week_rolling = week_data.rolling(window=7).sum().dropna()
    week_rolling['this_year_self_rt'] = week_rolling['this_year_self'] / week_rolling['this_year_amt']
    week_rolling['last_year_self_rt'] = week_rolling['last_year_self'] / week_rolling['last_year_amt']
    week_rolling['this_year_qoq'] = week_rolling['this_year_self_rt'] / week_rolling['this_year_self_rt'].shift(7)
    week_rolling['last_year_qoq'] = week_rolling['last_year_self_rt'] / week_rolling['last_year_self_rt'].shift(7)
    processed_data = (week_rolling['this_year_qoq'] / week_rolling['last_year_qoq']).dropna()
    bottom, top = zscore_bins(processed_data)
    res['self_rt_week_top'] = top
    res['self_rt_week_bottom'] = bottom
    # 5.自有支付月
    month_rolling = week_data.rolling(window=30).sum().dropna()
    month_rolling['this_year_self_rt'] = month_rolling['this_year_self'] / month_rolling['this_year_amt']
    processed_data = (month_rolling['this_year_self_rt'] / month_rolling['this_year_self_rt'].shift(30)).dropna()
    bottom, top = zscore_bins(processed_data)
    res['self_rt_month_top'] = top
    res['self_rt_month_bottom'] = bottom
    # 6.高费率卡日
    window = 7
    data_rolling = day_data['highRate_rt'].rolling(window=window)
    rolling_mean = data_rolling.mean().dropna()
    t = (data_rolling.apply(lambda x: x[-1]).dropna() / rolling_mean).reset_index(drop=True)
    data_rolling_before = day_data_before['highRate_rt'].rolling(window=window)
    rolling_mean_before = data_rolling_before.mean().dropna()
    t_before = (data_rolling_before.apply(lambda x: x[-1]).dropna() / rolling_mean_before).reset_index(drop=True)
    processed_data = (t / t_before).dropna()
    bottom, top = zscore_bins(processed_data)
    res['highRate_rt_day_top'] = top
    res['highRate_rt_day_bottom'] = bottom
    # 7.高费率卡周
    week_data = pd.DataFrame(
        data={'this_year_highRate': day_data['highRate_amt'].values,  # 直接传递Series，Pandas会尝试按照 Series 的索引对齐数据
              'this_year_amt': day_data['amt'].values,
              'last_year_highRate': day_data_before['highRate_amt'].values,
              'last_year_amt': day_data_before['amt'].values
              },
        index=day_data['label_period'].values
    )
    week_rolling = week_data.rolling(window=7).sum().dropna()
    week_rolling['this_year_highRate_rt'] = week_rolling['this_year_highRate'] / week_rolling['this_year_amt']
    week_rolling['last_year_highRate_rt'] = week_rolling['last_year_highRate'] / week_rolling['last_year_amt']
    week_rolling['this_year_qoq'] = week_rolling['this_year_highRate_rt'] / week_rolling['this_year_highRate_rt'].shift(
        7)
    week_rolling['last_year_qoq'] = week_rolling['last_year_highRate_rt'] / week_rolling['last_year_highRate_rt'].shift(
        7)
    processed_data = (week_rolling['this_year_qoq'] / week_rolling['last_year_qoq']).dropna()
    bottom, top = zscore_bins(processed_data)
    res['highRate_rt_week_top'] = top
    res['highRate_rt_week_bottom'] = bottom
    # 8.高费率卡月
    month_rolling = week_data.rolling(window=30).sum().dropna()
    month_rolling['this_year_highRate_rt'] = month_rolling['this_year_highRate'] / month_rolling['this_year_amt']
    processed_data = (
                month_rolling['this_year_highRate_rt'] / month_rolling['this_year_highRate_rt'].shift(30)).dropna()
    bottom, top = zscore_bins(processed_data)
    res['highRate_rt_month_top'] = top
    res['highRate_rt_month_bottom'] = bottom

    return res


if __name__ == '__main__':
    today = sys.argv[1]

    df = pd.DataFrame(columns = ['Date', 'Usersource', 'self_rt_day_bottom', 'self_rt_day_top', 'self_rt_week_bottom',
                                 'self_rt_week_top', 'self_rt_month_bottom', 'self_rt_month_top', 'highRate_rt_day_bottom',
                                 'highRate_rt_day_top', 'highRate_rt_week_bottom', 'highRate_rt_week_top', 'highRate_rt_month_bottom',
                                 'highRate_rt_month_top', 'd'])
    for us in ['CTRIP', 'QUNAR']:
        res = get_bins(today, us)
        df.loc[len(df)] = [res['today'], res['usersource'], res['self_rt_day_bottom'], res['self_rt_day_top'], res['self_rt_week_bottom'],
                            res['self_rt_week_top'], res['self_rt_month_bottom'], res['self_rt_month_top'], res['highRate_rt_day_bottom'],
                            res['highRate_rt_day_top'], res['highRate_rt_week_bottom'], res['highRate_rt_week_top'], res['highRate_rt_month_bottom'],
                            res['highRate_rt_month_top'], res['today']]

    # Transform DataFrame to Spark DataFrame
    Spark_df = spark.createDataFrame(df)

    # Overwrite table
    Spark_df.write \
        .mode("overwrite") \
        .partitionBy("d") \
        .saveAsTable("report_paydb.adm_anomaly_bounds")