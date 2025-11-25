import pandas as pd
import numpy as np
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 1. Configuration
spark = SparkSession.builder.config("spark.sql.execution.arrow.enabled", "true") \
.config('spark.sql.execution.arrow.pyspark.enabled',"true").enableHiveSupport().getOrCreate()

IdcDict = {
    'self_rt': '自有支付占比'
}
new_line = '\n'
new_tab = '\t'

# 2. Final Data Tank
HiveData = pd.DataFrame(columns=['Date', 'Period', 'Target', 'Usersource','Layer', 'Indicator', 'Element',
                                 'Actual', 'Pred', 'CTA', 'CTS', 'CTB', 'CTBRatio', 'Conclusion'])

# 3. Utils
# SQL
class SQL:

    def __init__(self, usersource):
        self.usersource = usersource

    def indicator(self, day, length):
        sql = f'''
        select
        label_period
        , sum(amt) amt
        , sum(self_amt) self_amt
        , sum(case when paycategory='高费率信用卡' then self_amt end) highRate_amt
        from report_paydb.adm_kpi_selfpay_period
        where period='日'
        and usersource='{self.usersource}'
        and d between date_sub('{day}', {length}) and '{day}'
        group by 1
        order by 1 asc
        '''

        return sql

    def special(self, day, length):
        sql = f'''
        select
        label_period
        , paycategory
        , platform
        , sum(amt) amt
        , sum(self_amt) self_amt
        , sum(self_amt_payonly) self_amt_payonly
        , sum(self_amt_refund) self_amt_refund
        from report_paydb.adm_kpi_selfpay_period
        where period='日'
        and usersource='{self.usersource}'
        and d between date_sub('{day}', {length}) and '{day}'
        group by 1,2,3
        order by 1 asc,2,3
        '''

        return sql

    def bu(self, day, length):
        sql = f'''
        select
        bu
        , label_period
        , sum(amt) amt
        , sum(self_amt) self_amt
        , sum(case when paycategory='高费率信用卡' then self_amt end) highRate_amt
        from report_paydb.adm_kpi_selfpay_period
        where period='日'
        and usersource='{self.usersource}'
        and d between date_sub('{day}', {length}) and '{day}'
        group by 1,2
        order by 1,2 asc
        '''

        return sql

    def structure(self, day, length, bu):
        sql = f'''
        select
        platform
        , paycategory
        , new_post_flag
        , label_period
        , sum(amt) amt
        , sum(self_amt) self_amt
        , sum(case when paycategory='高费率信用卡' then self_amt end) highRate_amt
        from report_paydb.adm_kpi_selfpay_period
        where period='日'
        and usersource='{self.usersource}'
        and d between date_sub('{day}', {length}) and '{day}'
        and bu = '{bu}'
        group by 1,2,3,4
        order by 1,2,3,4 asc
        '''

        return sql

    def fir_permeability(self, day, length, bu):
        sql = f'''
        select
        label_period
        , SUM(IF(new_post_flag = '预付', amt, 0)) pre_amt
        , SUM(IF(new_post_flag = '预付', self_amt, 0)) pre_self_amt
        , SUM(IF(new_post_flag = '极速', amt, 0)) quick_amt
        , SUM(IF(new_post_flag = '极速', self_amt, 0)) quick_self_amt
        , SUM(IF(new_post_flag = '后付', amt, 0)) post_amt
        , SUM(IF(new_post_flag = '后付', self_amt, 0)) post_self_amt
        from report_paydb.adm_kpi_selfpay_period
        where period='日'
        and usersource='{self.usersource}'
        and d between date_sub('{day}', {length}) and '{day}'
        and bu = '{bu}'
        group by 1
        order by 1 asc
        '''

        return sql

    def sec_permeability(self, day, length, bu):
        sql = f'''
        select
        label_period
        , SUM(IF(new_post_flag = '预付', amt, 0)) pre_amt
        , SUM(IF(new_post_flag = '预付', self_amt_payonly, 0))-SUM(IF(new_post_flag = '预付', self_amt, 0)) pre_refund
        , SUM(IF(new_post_flag = '预付', cnt_payonly, 0)) pre_cnt
        , SUM(IF(paytype = '卡' and new_post_flag = '预付', cnt_payonly, 0)) pre_card_cnt
        , SUM(IF(paytype = '拿去花' and new_post_flag = '预付', cnt_payonly, 0)) pre_nqh_cnt
        , SUM(IF(paytype = '其他自有' and new_post_flag = '预付', cnt_payonly, 0)) pre_otherself_cnt
        , SUM(IF(paytype = '卡' and new_post_flag = '预付', amt_payonly, 0)) AS pre_card_amt
        , SUM(IF(paytype = '拿去花' and new_post_flag = '预付', amt_payonly, 0)) AS pre_nqh_amt
        , SUM(IF(paytype = '其他自有' and new_post_flag = '预付', amt_payonly, 0)) AS pre_otherself_amt
        , SUM(IF(new_post_flag = '极速', amt, 0)) quick_amt
        , SUM(IF(new_post_flag = '极速', self_amt_payonly, 0))-SUM(IF(new_post_flag = '极速', self_amt, 0)) quick_refund
        , SUM(IF(new_post_flag = '极速', cnt_payonly, 0)) quick_cnt
        , SUM(IF(new_post_flag = '极速' and label_a = '标准极速新开通', cnt_payonly, 0)) quick_new_cnt
        , SUM(IF(new_post_flag = '极速' and label_a = '标准极速新开通', self_cnt_payonly, 0)) quick_new_self_cnt
        , SUM(IF(new_post_flag = '极速' and label_a = '标准极速新开通', self_amt_payonly, 0)) quick_new_self_amt
        , SUM(IF(new_post_flag = '极速' and label_a = '标准极速已开通', cnt_payonly, 0)) quick_old_cnt
        , SUM(IF(new_post_flag = '极速' and label_a = '标准极速已开通', self_cnt_payonly, 0)) quick_old_self_cnt
        , SUM(IF(new_post_flag = '极速' and label_a = '标准极速已开通', self_amt_payonly, 0)) quick_old_self_amt
        , SUM(IF(new_post_flag = '后付', amt, 0)) post_amt
        , SUM(IF(new_post_flag = '后付', self_amt_payonly, 0))-SUM(IF(new_post_flag = '后付', self_amt, 0)) post_refund
        , SUM(IF(new_post_flag = '后付', cnt_payonly, 0)) post_cnt
        , SUM(IF(new_post_flag = '后付' and label_a = '后付新开通', cnt_payonly, 0)) post_new_cnt
        , SUM(IF(new_post_flag = '后付' and label_a = '后付新开通', self_cnt_payonly, 0)) post_new_self_cnt
        , SUM(IF(new_post_flag = '后付' and label_a = '后付新开通', self_amt_payonly, 0)) post_new_self_amt
        , SUM(IF(new_post_flag = '后付' and label_a = '后付已开通', cnt_payonly, 0)) post_old_cnt
        , SUM(IF(new_post_flag = '后付' and label_a = '后付已开通', self_cnt_payonly, 0)) post_old_self_cnt
        , SUM(IF(new_post_flag = '后付' and label_a = '后付已开通', self_amt_payonly, 0)) post_old_self_amt
        from report_paydb.adm_kpi_selfpay_period
        where period='日'
        and usersource='{self.usersource}'
        and d between date_sub('{day}', {length}) and '{day}'
        and bu = '{bu}'
        group by 1
        order by 1 asc
        '''

        return sql

    def thd_permeability(self, sec_indicator, day, length, bu):
        if sec_indicator == 'pre_card_rt':
            sql = f'''
            select
            label_period
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('无卡新绑卡', '无卡未绑卡'), card_cnt, 0)) pre_nocard_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡新绑卡', '有卡默勾卡', '有卡换勾卡', '有卡其他'), card_cnt, 0)) pre_havecard_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('无卡新绑卡'), card_cnt, 0)) pre_nocard_bindcard_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('无卡新绑卡'), cnt_payonly, 0)) pre_nocard_bindcard_sub_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡默勾卡'), card_cnt, 0)) pre_havecard_default_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡默勾卡'), cnt_payonly, 0)) pre_havecard_default_sub_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡换勾卡'), card_cnt, 0)) pre_havecard_change_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡换勾卡'), cnt_payonly, 0)) pre_havecard_change_sub_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡新绑卡'), card_cnt, 0)) pre_havecard_bindcard_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '卡' and label_a IN ('有卡新绑卡'), cnt_payonly, 0)) pre_havecard_bindcard_sub_cnt
            from report_paydb.adm_kpi_selfpay_period
            where period='日'
            and usersource='{self.usersource}'
            and d between date_sub('{day}', {length}) and '{day}'
            and bu = '{bu}'
            group by 1
            order by 1 asc
            '''
        elif sec_indicator == 'pre_nqh_rt':
            sql = f'''
            select
            label_period
            , SUM(IF(new_post_flag = '预付' and paytype = '拿去花' and label_a IN ('未激活新激活', '未激活非拿去花支付'), nqh_cnt, 0)) pre_noactivate_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '拿去花' and label_a IN ('未激活新激活'), cnt_payonly, 0)) pre_noactivate_sub_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '拿去花' and label_a IN ('已激活'), nqh_cnt, 0)) pre_activate_cnt
            , SUM(IF(new_post_flag = '预付' and paytype = '拿去花' and label_a IN ('已激活'), cnt_payonly, 0)) pre_activate_sub_cnt
            from report_paydb.adm_kpi_selfpay_period
            where period='日'
            and usersource='{self.usersource}'
            and d between date_sub('{day}', {length}) and '{day}'
            and bu = '{bu}'
            group by 1
            order by 1 asc
            '''
        elif sec_indicator == 'quick_new_self_rt':
            sql = f'''
            select
            label_period
            , SUM(IF(new_post_flag = '极速' and label_a IN ('标准极速新开通'), cnt_payonly, 0)) quicknew_cnt_payonly
            , SUM(IF(new_post_flag = '极速' and label_a IN ('标准极速新开通'), quickpay_haveself_cnt_payonly, 0)) quicknew_haveself_cnt_payonly
            , SUM(IF(new_post_flag = '极速' and label_a IN ('标准极速新开通'), self_cnt_payonly, 0)) quicknew_self_cnt_payonly
            from report_paydb.adm_kpi_selfpay_period
            where period='日'
            and usersource='{self.usersource}'
            and d between date_sub('{day}', {length}) and '{day}'
            and bu = '{bu}'
            group by 1
            order by 1 asc
            '''
        else:
            sql = ''

        return sql


# 计算实际值和预测值
def indicator_calculate(today, period):
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    cols = ['amt', 'self_amt', 'highRate_amt']

    # 1.获取当期数据
    sql1 = sql.indicator(today, dic[period])
    day_data = spark.sql(sql1).toPandas()[cols]
    # 2.获取往期数据
    sql2 = sql.indicator(str(int(today[:4]) - 1) + today[-6:], dic[period])
    day_data_before = spark.sql(sql2).toPandas()[cols]

    # 3.计算实际值和预测值
    if period == '日':
        actual = day_data.iloc[-1]
        pred = day_data_before.iloc[-1] * day_data.iloc[:-1].sum() / day_data_before.iloc[:-1].sum()
    elif period == '周':
        actual = day_data.iloc[-7:].mean()
        pred = day_data_before.iloc[-7:].mean() * day_data.iloc[:7].sum() / day_data_before.iloc[:7].sum()
    else:
        actual = day_data.iloc[-30:].mean()
        pred = day_data_before.iloc[-30:].mean() * day_data.iloc[:30].sum() / day_data_before.iloc[:30].sum()

    return {
        'self_rt': {'actual': actual['self_amt'] / actual['amt'], 'pred': pred['self_amt'] / pred['amt']},
        # 'highRate_rt': {'actual': actual['highRate_amt']/actual['amt'], 'pred': pred['highRate_amt']/pred['amt']},
    }


# 特殊场景分析
def specail_analyse(today, period, pre_pred):
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    cols = ['amt', 'self_amt', 'self_amt_payonly', 'self_amt_refund']

    # 1.获取当期数据
    sql1 = sql.special(today, dic[period])
    day_data = spark.sql(sql1).toPandas()
    # 2.获取往期数据
    sql2 = sql.special(str(int(today[:4]) - 1) + today[-6:], dic[period])
    day_data_before = spark.sql(sql2).toPandas()

    def fill_value(series, *keys):
        for key in keys:
            if key not in series:
                series[key] = 0
        pass

    def sep_refund(this_year, last_year, pre_pred):
        nonlocal cols
        day_data = this_year.groupby('label_period')[cols].sum()
        day_data_before = last_year.groupby('label_period')[cols].sum()
        # 计算实际值和预测值
        if period == '日':
            actual = day_data.iloc[-1]
            pred = day_data_before.iloc[-1] * day_data.iloc[:-1].sum() / day_data_before.iloc[:-1].sum()
        elif period == '周':
            actual = day_data.iloc[-7:].mean()
            pred = day_data_before.iloc[-7:].mean() * day_data.iloc[:7].sum() / day_data_before.iloc[:7].sum()
        else:
            actual = day_data.iloc[-30:].mean()
            pred = day_data_before.iloc[-30:].mean() * day_data.iloc[:30].sum() / day_data_before.iloc[:30].sum()
        # 计算仅付款和退款
        payonly_actual = actual['self_amt_payonly'] / actual['amt']
        refund_actual = actual['self_amt_refund'] / actual['amt']
        payonly_pred = pred['self_amt_payonly'] / pred['amt']
        refund_pred = pred['self_amt_refund'] / pred['amt']
        zoom_rt = pre_pred / (payonly_pred + refund_pred)
        actual = payonly_actual + refund_actual

        return 'Refund', {'all': [actual, pre_pred, actual - pre_pred],
                          '退款': [refund_actual, refund_pred * zoom_rt, refund_actual - refund_pred * zoom_rt],
                          }

    def sep_paycategory(this_year, last_year, pre_pred):
        nonlocal cols
        day_data_groupby = \
        day_data.groupby(['paycategory', 'label_period']).sum(numeric_only=True).groupby('paycategory')[cols]
        day_data_before_groupby = \
        day_data_before.groupby(['paycategory', 'label_period']).sum(numeric_only=True).groupby('paycategory')[cols]
        # 计算实际值和预测值
        if period == '日':
            temp = day_data_groupby.apply(lambda x: x[cols].iloc[:-1].sum().astype(float))
            actual = day_data_groupby.apply(lambda x: x[cols].iloc[-1].astype(float))
            pred = day_data_before_groupby.apply(
                lambda x: x[cols].iloc[-1].astype(float) / x[cols].iloc[:-1].sum()) * temp
        elif period == '周':
            temp = day_data_groupby.apply(lambda x: x[cols].iloc[:7].sum().astype(float))
            actual = day_data_groupby.apply(lambda x: x[cols].iloc[-7:].mean().astype(float))
            pred = day_data_before_groupby.apply(
                lambda x: x[cols].iloc[-7:].mean().astype(float) / x[cols].iloc[:7].sum()) * temp
        else:
            temp = day_data_groupby.apply(lambda x: x[cols].iloc[:30].sum().astype(float))
            actual = day_data_groupby.apply(lambda x: x[cols].iloc[-30:].mean().astype(float))
            pred = day_data_before_groupby.apply(
                lambda x: x[cols].iloc[-30:].mean().astype(float) / x[cols].iloc[:30].sum()) * temp

        self_actual = actual['self_amt'] / np.sum(actual['amt'])
        self_pred = pred['self_amt'] / np.sum(pred['amt'])
        zoom_rt = pre_pred / np.sum(self_pred)

        fill_value(self_actual, *['汇付', '易宝', '票券'])
        fill_value(self_pred, *['汇付', '易宝', '票券'])

        return 'PayChannel', {'all': [np.sum(self_actual), pre_pred, np.sum(self_actual) - pre_pred],
                              '汇付': [self_actual['汇付'], self_pred['汇付'] * zoom_rt,
                                       self_actual['汇付'] - self_pred['汇付'] * zoom_rt],
                              '易宝': [self_actual['易宝'], self_pred['易宝'] * zoom_rt,
                                       self_actual['易宝'] - self_pred['易宝'] * zoom_rt],
                              '票券': [self_actual['票券'], self_pred['票券'] * zoom_rt,
                                       self_actual['票券'] - self_pred['票券'] * zoom_rt],
                              }

    def sep_platform(this_year, last_year, pre_pred):
        nonlocal cols
        day_data_groupby = day_data.groupby(['platform', 'label_period']).sum(numeric_only=True).groupby('platform')[
            cols]
        day_data_before_groupby = \
        day_data_before.groupby(['platform', 'label_period']).sum(numeric_only=True).groupby('platform')[cols]
        # 计算实际值和预测值
        if period == '日':
            temp = day_data_groupby.apply(lambda x: x[cols].iloc[:-1].sum().astype(float))
            actual = day_data_groupby.apply(lambda x: x[cols].iloc[-1].astype(float))
            pred = day_data_before_groupby.apply(
                lambda x: x[cols].iloc[-1].astype(float) / x[cols].iloc[:-1].sum()) * temp
        elif period == '周':
            temp = day_data_groupby.apply(lambda x: x[cols].iloc[:7].sum().astype(float))
            actual = day_data_groupby.apply(lambda x: x[cols].iloc[-7:].mean().astype(float))
            pred = day_data_before_groupby.apply(
                lambda x: x[cols].iloc[-7:].mean().astype(float) / x[cols].iloc[:7].sum()) * temp
        else:
            temp = day_data_groupby.apply(lambda x: x[cols].iloc[:30].sum().astype(float))
            actual = day_data_groupby.apply(lambda x: x[cols].iloc[-30:].mean().astype(float))
            pred = day_data_before_groupby.apply(
                lambda x: x[cols].iloc[-30:].mean().astype(float) / x[cols].iloc[:30].sum()) * temp

        self_actual = actual['self_amt'] / np.sum(actual['amt'])
        self_pred = pred['self_amt'] / np.sum(pred['amt'])
        zoom_rt = pre_pred / np.sum(self_pred)

        fill_value(self_actual, *['微信公众号', '微信小程序', '支付宝小程序', '百度小程序'])
        fill_value(self_pred, *['微信公众号', '微信小程序', '支付宝小程序', '百度小程序'])

        return 'PayPlatform', {'all': [np.sum(self_actual), pre_pred, np.sum(self_actual) - pre_pred],
                               '微信公众号': [self_actual['微信公众号'], self_pred['微信公众号'] * zoom_rt,
                                              self_actual['微信公众号'] - self_pred['微信公众号'] * zoom_rt],
                               '微信小程序': [self_actual['微信小程序'], self_pred['微信小程序'] * zoom_rt,
                                              self_actual['微信小程序'] - self_pred['微信小程序'] * zoom_rt],
                               '支付宝小程序': [self_actual['支付宝小程序'], self_pred['支付宝小程序'] * zoom_rt,
                                                self_actual['支付宝小程序'] - self_pred['支付宝小程序'] * zoom_rt],
                               '百度小程序': [self_actual['百度小程序'], self_pred['百度小程序'] * zoom_rt,
                                              self_actual['百度小程序'] - self_pred['百度小程序'] * zoom_rt],
                               }

    return sep_refund(day_data, day_data_before, pre_pred), sep_paycategory(day_data, day_data_before,
                                                                            pre_pred), sep_platform(day_data,
                                                                                                    day_data_before,
                                                                                                    pre_pred)


# bu拆分
def bu_split(today, period, pre_pred):
    ''' 两因素拆解法拆解bu '''
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    cols = ['amt', 'self_amt', 'highRate_amt']

    # 1.获取当期数据
    sql1 = sql.bu(today, dic[period])
    day_data = spark.sql(sql1).toPandas()
    # 2.获取往期数据
    sql2 = sql.bu(str(int(today[:4]) - 1) + today[-6:], dic[period])
    day_data_before = spark.sql(sql2).toPandas()
    # 3.转化为分组列表
    day_data_groupby = day_data.groupby('bu')
    day_data_before_groupby = day_data_before.groupby('bu')
    if period == '日':
        temp = day_data_groupby.apply(lambda x: x[cols].iloc[:-1].sum().astype(float))
        actual = day_data_groupby.apply(lambda x: x[cols].iloc[-1].astype(float))
        pred = day_data_before_groupby.apply(lambda x: x[cols].iloc[-1].astype(float) / x[cols].iloc[:-1].sum()) * temp
    elif period == '周':
        temp = day_data_groupby.apply(lambda x: x[cols].iloc[:7].sum().astype(float))
        actual = day_data_groupby.apply(lambda x: x[cols].iloc[-7:].mean().astype(float))
        pred = day_data_before_groupby.apply(
            lambda x: x[cols].iloc[-7:].mean().astype(float) / x[cols].iloc[:7].sum()) * temp
    else:
        temp = day_data_groupby.apply(lambda x: x[cols].iloc[:30].sum().astype(float))
        actual = day_data_groupby.apply(lambda x: x[cols].iloc[-30:].mean().astype(float))
        pred = day_data_before_groupby.apply(
            lambda x: x[cols].iloc[-30:].mean().astype(float) / x[cols].iloc[:30].sum()) * temp
    '''自有支付占比'''
    self_rt_actual = np.dot(actual['amt'] / np.sum(actual['amt']), actual['self_amt'] / actual['amt'])
    self_rt_pred = np.dot(pred['amt'] / np.sum(pred['amt']), pred['self_amt'] / pred['amt'])
    zoom_ratio = pre_pred / self_rt_pred  # 加性放缩比(每个子项是两数乘积)
    '''结构'''
    structure_actual = actual['amt'] / np.sum(actual['amt'])
    structure_pred = pred['amt'] / np.sum(pred['amt'])
    '''渗透率'''
    perm_actual = actual['self_amt'] / actual['amt']
    perm_pred = pred['self_amt'] * zoom_ratio / pred['amt']
    # 4.计算结构变化和渗透率变化的贡献
    return {
        'self_rt': {
            '结构占比': [structure_actual, structure_pred, (structure_actual - structure_pred) * perm_pred],
            '渗透率': [perm_actual, perm_pred, (perm_actual - perm_pred) * structure_pred],
            'AmtSumPred': np.sum(pred['amt']) / zoom_ratio,
        },
    }


# Adtributor
class Adtributor:

    @classmethod
    def surprise(self, df):
        '''
        计算惊奇度
        传入的dataframe，索引为分组，第一列为真实值，第二列为预测值
        '''
        pq = df / np.sum(df, axis=0)
        res = pq.iloc[:, 0] * np.log(pq.iloc[:, 0]) + pq.iloc[:, 1] * np.log(pq.iloc[:, 1]) - np.sum(pq,
                                                                                                     axis=1) * np.log(
            0.5 * np.sum(pq, axis=1))
        return res

    @classmethod
    def explanatory(self, df, actual, pred, ratio):
        '''
        计算解释力
        传入的为率值指标，df第一列为真实值，第二列为预测值; actual为总体真实值，pred为整体预测值
        '''
        if not ratio:
            return (df.iloc[:, 0] - df.iloc[:, 1]) / (actual - pred)
        else:
            return ((df.iloc[:, 0] - df.iloc[:, 1]) * pred - (actual - pred) * df.iloc[:, 1]) / (pred * actual)

    @classmethod
    def adtributor(self, data, exp_threshold=0.1, top_k=3, ratio=False):
        '''
        计算根因
        率值指标：最后四列依次为真实分子、真实分母、预测分子、预测分母，前面的为一个个维度
        真实指标：最后两列分别为真实值、预测值，前面的为一个个维度
        '''

        ret = {
        }

        if ratio:
            dimensions = data.columns[:-4][-1:]  # [-1]只取最后一个收银台类型
            candidates = []  # 候选异动维度
            actual = np.sum(data.iloc[:, -4]) / np.sum(data.iloc[:, -3])  # 总体真实值
            pred = np.sum(data.iloc[:, -2]) / np.sum(data.iloc[:, -1])  # 总体预测值
            for dim in dimensions:
                df = data.groupby(dim).sum().iloc[:, -4:]
                df['_actual'] = df.iloc[:, -4] / df.iloc[:, -3]
                df['_pred'] = df.iloc[:, -3] / df.iloc[:, -2]
                ret[dim] = df[['_actual', '_pred']]
                exp = self.explanatory(df.iloc[:, -2:], actual, pred, ratio)
                surp = self.surprise(df.iloc[:, [0, 2]]) + self.surprise(df.iloc[:, [1, 3]])
                temp_candidates1 = surp
                temp_candidates2 = exp
                candidates.extend(
                    list(zip([dim] * len(temp_candidates2), temp_candidates2.index, temp_candidates2.values)))
        else:
            dimensions = data.columns[:-2]
            candidates = []  # 候选异动维度
            actual = np.sum(data.iloc[:, -2])  # 总体真实值
            pred = np.sum(data.iloc[:, -1])  # 总体预测值
            for dim in dimensions:
                df = data.groupby(dim).sum().iloc[:, -2:]
                ret[dim] = df
                exp = self.explanatory(df.iloc[:, -2:], actual, pred, ratio)
                surp = self.surprise(df.iloc[:, -2:])
                temp_candidates1 = surp[exp >= exp_threshold]
                temp_candidates2 = exp[exp >= exp_threshold]
                candidates.extend(
                    list(zip([dim] * len(temp_candidates2), temp_candidates2.index, temp_candidates2.values)))

        candidates.sort(reverse=True, key=lambda x: x[2])
        ret['adtributor'] = candidates[:top_k]

        return ret


# 结构拆分
def structure_split(today, period, bu, ratio=False):
    ''' Adtributor分析结构根因 '''
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    cols = ['amt', 'self_amt', 'highRate_amt']
    groups = ['platform', 'paycategory', 'new_post_flag']

    # 1.获取当期数据
    sql1 = sql.structure(today, dic[period], bu)
    day_data = spark.sql(sql1).toPandas()
    # 2.获取往期数据
    sql2 = sql.structure(str(int(today[:4]) - 1) + today[-6:], dic[period], bu)
    day_data_before = spark.sql(sql2).toPandas()
    # 3.转化为分组列表
    day_data_groupby = day_data.groupby(groups)
    day_data_before_groupby = day_data_before.groupby(groups)
    if period == '日':
        temp = day_data_groupby.apply(lambda x: x[cols].iloc[:-1].sum().astype(float))
        actual = day_data_groupby.apply(lambda x: x[cols].iloc[-1].astype(float))
        pred = day_data_before_groupby.apply(lambda x: x[cols].iloc[-1].astype(float) / x[cols].iloc[:-1].sum()) * temp
    elif period == '周':
        temp = day_data_groupby.apply(lambda x: x[cols].iloc[:7].sum().astype(float))
        actual = day_data_groupby.apply(lambda x: x[cols].iloc[-7:].mean().astype(float))
        pred = day_data_before_groupby.apply(
            lambda x: x[cols].iloc[-7:].mean().astype(float) / x[cols].iloc[:7].sum()) * temp
    else:
        temp = day_data_groupby.apply(lambda x: x[cols].iloc[:30].sum().astype(float))
        actual = day_data_groupby.apply(lambda x: x[cols].iloc[-30:].mean().astype(float))
        pred = day_data_before_groupby.apply(
            lambda x: x[cols].iloc[-30:].mean().astype(float) / x[cols].iloc[:30].sum()) * temp

    actual.reset_index(drop=False, inplace=True)
    pred.reset_index(drop=False, inplace=True)
    processed_data = pd.merge(left=actual, right=pred, how='inner', on=groups, suffixes=['_actual', '_pred'])

    if ratio:
        return {
            'self_rt': Adtributor.adtributor(
                processed_data[[*groups, 'self_amt_actual', 'amt_actual', 'self_amt_pred', 'amt_pred']], top_k=3,
                ratio=ratio),
            # 'highRate': Adtributor.adtributor(processed_data[[*groups,'amt_actual','amt_pred']], top_k=3)
        }
    else:
        return {
            'self_rt': Adtributor.adtributor(processed_data[[*groups, 'amt_actual', 'amt_pred']], top_k=3, ratio=ratio),
            # 'highRate': Adtributor.adtributor(processed_data[[*groups,'amt_actual','amt_pred']], top_k=3)
        }


# 渗透率一级指标
def counter_split(today, period, bu, pre_pred):
    ''' lmdi拆解收银台的转化率 '''
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    cols = ['pre_amt', 'pre_self_amt', 'quick_amt', 'quick_self_amt', 'post_amt', 'post_self_amt']

    # 1.获取当期数据
    sql1 = sql.fir_permeability(today, dic[period], bu)
    day_data = spark.sql(sql1).toPandas()[cols]
    # 2.获取往期数据
    sql2 = sql.fir_permeability(str(int(today[:4]) - 1) + today[-6:], dic[period], bu)
    day_data_before = spark.sql(sql2).toPandas()[cols]
    # 3.计算实际值和预测值
    if period == '日':
        actual = day_data.iloc[-1]
        pred = day_data_before.iloc[-1] * day_data.iloc[:-1].sum() / day_data_before.iloc[:-1].sum()
    elif period == '周':
        actual = day_data.iloc[-7:].mean()
        pred = day_data_before.iloc[-7:].mean() * day_data.iloc[:7].sum() / day_data_before.iloc[:7].sum()
    else:
        actual = day_data.iloc[-30:].mean()
        pred = day_data_before.iloc[-30:].mean() * day_data.iloc[:30].sum() / day_data_before.iloc[:30].sum()

    # 4.得到实际和预测的指标值
    def cal_indicators(data, label=''):
        res = pd.Series(dtype='float64', index=pd.MultiIndex.from_product([
            ('结构占比', '渗透率'), ('后付', '极速', '预付')
        ]))
        amt_sum = (data['pre_amt'] if not pd.isna(data['pre_amt']) else 0) + \
                  (data['quick_amt'] if not pd.isna(data['quick_amt']) else 0) + \
                  (data['post_amt'] if not pd.isna(data['post_amt']) else 0)

        self_amt_sum = (data['pre_self_amt'] if not pd.isna(data['pre_self_amt']) else 0) + \
                       (data['quick_self_amt'] if not pd.isna(data['quick_self_amt']) else 0) + \
                       (data['post_self_amt'] if not pd.isna(data['post_self_amt']) else 0)

        rt = self_amt_sum / amt_sum
        zoom_ratio = pre_pred / rt if label == '预测' else 1

        res[('结构占比', '预付')] = data['pre_amt'] / amt_sum
        res[('渗透率', '预付')] = data['pre_self_amt'] / data['pre_amt'] * zoom_ratio

        res[('结构占比', '极速')] = data['quick_amt'] / amt_sum
        res[('渗透率', '极速')] = data['quick_self_amt'] / data['quick_amt'] * zoom_ratio

        res[('结构占比', '后付')] = data['post_amt'] / amt_sum
        res[('渗透率', '后付')] = data['post_self_amt'] / data['post_amt'] * zoom_ratio

        res.fillna(0, inplace=True)

        return res

    actual = cal_indicators(actual, '实际')
    pred = cal_indicators(pred, '预测')

    # 5.lmdi法分解指标贡献
    def lmdi(actual, pred):
        '''
        LMDI分解法
        actual为真实值，pred为预测值；每部分都指标分解值
        '''
        actual_prod = np.prod(actual)
        pred_prod = np.prod(pred)
        if actual_prod == 0 or pred_prod == 0:
            res = np.full(len(actual), actual_prod - pred_prod)
            res[0] = 0
            return res  # 如果存在0的数，直接用全量变化为其变化值(但不包括第一个公共基数)
        temp2 = (actual_prod - pred_prod) / np.log(actual_prod / pred_prod)
        temp1 = np.log(actual / pred)
        res = temp1 * temp2
        return res

    res = pd.Series(index=actual.index, data=np.zeros(len(actual)))
    idx = pd.IndexSlice
    for cols in ('后付',
                 '极速',
                 '预付',
                 ):
        res.loc[idx[:, cols]] = (res.loc[idx[:, cols]] + lmdi(actual.loc[idx[:, cols]], pred.loc[idx[:, cols]])).values

    return actual, pred, res


# 渗透率二级指标
def permeability_split(today, period, bu, counter, pre_pred):
    ''' lmdi拆解收银台的转化率 '''
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    pre_cols = ['pre_refund', 'pre_amt', 'pre_cnt', 'pre_card_cnt', 'pre_nqh_cnt', 'pre_otherself_cnt', 'pre_card_amt',
                'pre_nqh_amt', 'pre_otherself_amt']
    quick_cols = ['quick_refund', 'quick_amt', 'quick_cnt', 'quick_new_cnt', 'quick_new_self_cnt', 'quick_new_self_amt',
                  'quick_old_cnt', 'quick_old_self_cnt', 'quick_old_self_amt']
    post_cols = ['post_refund', 'post_amt', 'post_cnt', 'post_new_cnt', 'post_new_self_cnt', 'post_new_self_amt',
                 'post_old_cnt', 'post_old_self_cnt', 'post_old_self_amt']
    cols = pre_cols + quick_cols + post_cols

    # 1.获取当期数据
    sql1 = sql.sec_permeability(today, dic[period], bu)
    day_data = spark.sql(sql1).toPandas()[cols]
    # 2.获取往期数据
    sql2 = sql.sec_permeability(str(int(today[:4]) - 1) + today[-6:], dic[period], bu)
    day_data_before = spark.sql(sql2).toPandas()[cols]
    # 3.计算实际值和预测值
    if period == '日':
        actual = day_data.iloc[-1]
        pred = day_data_before.iloc[-1] * day_data.iloc[:-1].sum() / day_data_before.iloc[:-1].sum()
    elif period == '周':
        actual = day_data.iloc[-7:].mean()
        pred = day_data_before.iloc[-7:].mean() * day_data.iloc[:7].sum() / day_data_before.iloc[:7].sum()
    else:
        actual = day_data.iloc[-30:].mean()
        pred = day_data_before.iloc[-30:].mean() * day_data.iloc[:30].sum() / day_data_before.iloc[:30].sum()

    # 4.得到实际和预测的指标值
    def cal_indicators(data, label=''):
        res = pd.Series(dtype='float64')

        if counter == '预付':
            res['pre_card_rt'] = data['pre_card_cnt'] / data['pre_cnt']
            res['pre_card_p'] = data['pre_card_amt'] / data['pre_card_cnt']
            res['pre_nqh_rt'] = data['pre_nqh_cnt'] / data['pre_cnt']
            res['pre_nqh_p'] = data['pre_nqh_amt'] / data['pre_nqh_cnt']
            res['pre_otherself_rt'] = data['pre_otherself_cnt'] / data['pre_cnt']
            res['pre_otherself_p'] = data['pre_otherself_amt'] / data['pre_otherself_cnt']
            res['pre_cnt'] = data['pre_cnt']
            res['pre_amt'] = 1 / data['pre_amt']
            res['pre_refund'] = -data['pre_refund']
            res.fillna(0, inplace=True)
            if label == '预测':
                self_rt = res['pre_cnt'] * res['pre_amt'] * (res['pre_card_rt'] * res['pre_card_p'] +
                                                             res['pre_nqh_rt'] * res['pre_nqh_p'] +
                                                             res['pre_otherself_rt'] * res['pre_otherself_p']) + res[
                              'pre_refund'] * res['pre_amt']
                zoom_ratio = pre_pred / self_rt
                for x in res.index:
                    if x == 'pre_refund':
                        res[x] = res[x] * pow(zoom_ratio, 3 / 4)
                    else:
                        res[x] = res[x] * pow(zoom_ratio, 1 / 4)
        elif counter == '极速':
            res['quick_new_rt'] = data['quick_new_cnt'] / data['quick_cnt']
            res['quick_new_self_rt'] = data['quick_new_self_cnt'] / data['quick_new_cnt']
            res['quick_new_self_p'] = data['quick_new_self_amt'] / data['quick_new_self_cnt']
            res['quick_old_rt'] = data['quick_old_cnt'] / data['quick_cnt']
            res['quick_old_self_rt'] = data['quick_old_self_cnt'] / data['quick_old_cnt']
            res['quick_old_self_p'] = data['quick_old_self_amt'] / data['quick_old_self_cnt']
            res['quick_cnt'] = data['quick_cnt']
            res['quick_amt'] = 1 / data['quick_amt']
            res['quick_refund'] = -data['quick_refund']
            res.fillna(0, inplace=True)
            if label == '预测':
                self_rt = res['quick_cnt'] * res['quick_amt'] * (
                        res['quick_new_rt'] * res['quick_new_self_rt'] * res['quick_new_self_p'] +
                        res['quick_old_rt'] * res['quick_old_self_rt'] * res['quick_old_self_p']
                ) + res['quick_refund'] * res['quick_amt']
                zoom_ratio = pre_pred / self_rt
                for x in res.index:
                    if x == 'quick_refund':
                        res[x] = res[x] * pow(zoom_ratio, 4 / 5)
                    else:
                        res[x] = res[x] * pow(zoom_ratio, 1 / 5)
        else:
            res['post_new_rt'] = data['post_new_cnt'] / data['post_cnt']
            res['post_new_self_rt'] = data['post_new_self_cnt'] / data['post_new_cnt']
            res['post_new_self_p'] = data['post_new_self_amt'] / data['post_new_self_cnt']
            res['post_old_rt'] = data['post_old_cnt'] / data['post_cnt']
            res['post_old_self_rt'] = data['post_old_self_cnt'] / data['post_old_cnt']
            res['post_old_self_p'] = data['post_old_self_amt'] / data['post_old_self_cnt']
            res['post_cnt'] = data['post_cnt']
            res['post_amt'] = 1 / data['post_amt']
            res['post_refund'] = -data['post_refund']
            res.fillna(0, inplace=True)
            if label == '预测':
                self_rt = res['post_cnt'] * res['post_amt'] * (
                        res['post_new_rt'] * res['post_new_self_rt'] * res['post_new_self_p'] +
                        res['post_old_rt'] * res['post_old_self_rt'] * res['post_old_self_p']
                ) + res['post_refund'] * res['post_amt']
                zoom_ratio = pre_pred / self_rt
                for x in res.index:
                    if x == 'post_refund':
                        res[x] = res[x] * pow(zoom_ratio, 4 / 5)
                    else:
                        res[x] = res[x] * pow(zoom_ratio, 1 / 5)

        res.fillna(0, inplace=True)

        return res

    actual = cal_indicators(actual, '实际')
    pred = cal_indicators(pred, '预测')

    # 5.lmdi法分解指标贡献
    def lmdi(actual, pred):
        '''
        LMDI分解法
        actual为真实值，pred为预测值；每部分都指标分解值
        '''
        actual_prod = np.prod(actual)
        pred_prod = np.prod(pred)
        if actual_prod == 0 or pred_prod == 0:
            res = np.full(len(actual), actual_prod - pred_prod)
            res[0] = 0
            return res  # 如果存在0的数，直接用全量变化为其变化值(但不包括第一个公共基数)
        temp2 = (actual_prod - pred_prod) / np.log(actual_prod / pred_prod)
        temp1 = np.log(actual / pred)
        res = temp1 * temp2
        return res

    res = pd.Series(index=actual.index, data=np.zeros(len(actual)))
    if counter == '预付':
        cols_batch = (
            ['pre_cnt', 'pre_amt', 'pre_card_rt', 'pre_card_p'],
            ['pre_cnt', 'pre_amt', 'pre_nqh_rt', 'pre_nqh_p'],
            ['pre_cnt', 'pre_amt', 'pre_otherself_rt', 'pre_otherself_p'],
            ['pre_amt', 'pre_refund'],
        )
    elif counter == '极速':
        cols_batch = (
            ['quick_cnt', 'quick_amt', 'quick_new_rt', 'quick_new_self_rt', 'quick_new_self_p'],
            ['quick_cnt', 'quick_amt', 'quick_old_rt', 'quick_old_self_rt', 'quick_old_self_p'],
            ['quick_amt', 'quick_refund'],
        )
    else:
        cols_batch = (
            ['post_cnt', 'post_amt', 'post_new_rt', 'post_new_self_rt', 'post_new_self_p'],
            ['post_cnt', 'post_amt', 'post_old_rt', 'post_old_self_rt', 'post_old_self_p'],
            ['post_amt', 'post_refund'],
        )
    for cols in cols_batch:
        res[cols] += lmdi(actual[cols], pred[cols])

    return actual, pred, res


# 渗透率三级指标
def detail_split(today, period, bu, sec_indicator, pre_pred):
    ''' lmdi拆解三级指标 '''
    global sql

    dic = {
        '日': 6,
        '周': 13,
        '月': 59
    }
    # 1.获取当期数据
    sql1 = sql.thd_permeability(sec_indicator, today, dic[period], bu)
    day_data = spark.sql(sql1).toPandas().iloc[:, 1:]
    # 2.获取往期数据
    sql2 = sql.thd_permeability(sec_indicator, str(int(today[:4]) - 1) + today[-6:], dic[period], bu)
    day_data_before = spark.sql(sql2).toPandas().iloc[:, 1:]
    # 3.计算实际值和预测值
    if period == '日':
        actual = day_data.iloc[-1]
        pred = day_data_before.iloc[-1] * day_data.iloc[:-1].sum() / day_data_before.iloc[:-1].sum()
    elif period == '周':
        actual = day_data.iloc[-7:].mean()
        pred = day_data_before.iloc[-7:].mean() * day_data.iloc[:7].sum() / day_data_before.iloc[:7].sum()
    else:
        actual = day_data.iloc[-30:].mean()
        pred = day_data_before.iloc[-30:].mean() * day_data.iloc[:30].sum() / day_data_before.iloc[:30].sum()

    # 4.得到实际和预测的指标值
    def cal_indicators(data, label=''):
        res = pd.Series(dtype='float64')

        if sec_indicator == 'pre_card_rt':
            res['nocard_rt'] = data['pre_nocard_cnt'] / (data['pre_nocard_cnt'] + data['pre_havecard_cnt'])
            res['havecard_rt'] = data['pre_havecard_cnt'] / (data['pre_nocard_cnt'] + data['pre_havecard_cnt'])
            res['nocard_bindcard_rt'] = data['pre_nocard_bindcard_cnt'] / data['pre_nocard_cnt']
            res['nocard_bindcard_sub_rt'] = data['pre_nocard_bindcard_sub_cnt'] / data['pre_nocard_bindcard_cnt']
            res['havecard_default_rt'] = data['pre_havecard_default_cnt'] / data['pre_havecard_cnt']
            res['havecard_default_sub_rt'] = data['pre_havecard_default_sub_cnt'] / data['pre_havecard_default_cnt']
            res['havecard_change_rt'] = data['pre_havecard_change_cnt'] / data['pre_havecard_cnt']
            res['havecard_change_sub_rt'] = data['pre_havecard_change_sub_cnt'] / data['pre_havecard_change_cnt']
            res['havecard_bindcard_rt'] = data['pre_havecard_bindcard_cnt'] / data['pre_havecard_cnt']
            res['havecard_bindcard_sub_rt'] = data['pre_havecard_bindcard_sub_cnt'] / data['pre_havecard_bindcard_cnt']
            res.fillna(0, inplace=True)
            if label == '预测':
                self_rt = (data['pre_nocard_bindcard_sub_cnt'] + data['pre_havecard_default_sub_cnt'] +
                           data['pre_havecard_change_sub_cnt'] + data['pre_havecard_bindcard_sub_cnt']) / (
                                      data['pre_nocard_cnt'] + data['pre_havecard_cnt'])
                zoom_ratio = pre_pred / self_rt
                for x in res.index:
                    res[x] = min(res[x] * pow(zoom_ratio, 1 / 3), 1)
        elif sec_indicator == 'pre_nqh_rt':
            res['noactivate_rt'] = data['pre_noactivate_cnt'] / (data['pre_noactivate_cnt'] + data['pre_activate_cnt'])
            res['activate_rt'] = data['pre_activate_cnt'] / (data['pre_noactivate_cnt'] + data['pre_activate_cnt'])
            res['noactivate_sub_rt'] = data['pre_noactivate_sub_cnt'] / data['pre_noactivate_cnt']
            res['activate_sub_rt'] = data['pre_activate_sub_cnt'] / data['pre_activate_cnt']
            res.fillna(0, inplace=True)
            if label == '预测':
                self_rt = (data['pre_noactivate_sub_cnt'] + data['pre_activate_sub_cnt']) / (
                            data['pre_noactivate_cnt'] + data['pre_activate_cnt'])
                zoom_ratio = pre_pred / self_rt
                for x in res.index:
                    res[x] = min(res[x] * pow(zoom_ratio, 1 / 2), 1)
        elif sec_indicator == 'quick_new_self_rt':
            res['quicknew_haveself_rt'] = data['quicknew_haveself_cnt_payonly'] / data['quicknew_cnt_payonly']
            res['quicknew_self_sub_rt'] = data['quicknew_self_cnt_payonly'] / data['quicknew_haveself_cnt_payonly']
            res.fillna(0, inplace=True)
            if label == '预测':
                self_rt = data['quicknew_self_cnt_payonly'] / data['quicknew_cnt_payonly']
                zoom_ratio = pre_pred / self_rt
                for x in res.index:
                    res[x] = min(res[x] * pow(zoom_ratio, 1 / 2), 1)

        res.fillna(0, inplace=True)

        return res

    actual = cal_indicators(actual, '实际')
    pred = cal_indicators(pred, '预测')

    # 5.lmdi法分解指标贡献
    def lmdi(actual, pred):
        '''
        LMDI分解法
        actual为真实值，pred为预测值；每部分都指标分解值
        '''
        actual_prod = np.prod(actual)
        pred_prod = np.prod(pred)
        if actual_prod == 0 or pred_prod == 0:
            return np.full(len(actual), actual_prod - pred_prod)  # 如果存在0的数，直接用全量变化为其变化值
        temp2 = (actual_prod - pred_prod) / np.log(actual_prod / pred_prod)
        temp1 = np.log(actual / pred)
        res = temp1 * temp2
        return res

    res = pd.Series(index=actual.index, data=np.zeros(len(actual)))
    if sec_indicator == 'pre_card_rt':
        for cols in (['nocard_rt', 'nocard_bindcard_rt', 'nocard_bindcard_sub_rt'],
                     ['havecard_rt', 'havecard_default_rt', 'havecard_default_sub_rt'],
                     ['havecard_rt', 'havecard_change_rt', 'havecard_change_sub_rt'],
                     ['havecard_rt', 'havecard_bindcard_rt', 'havecard_bindcard_sub_rt']
                     ):
            res[cols] += lmdi(actual[cols], pred[cols])
    elif sec_indicator == 'pre_nqh_rt':
        for cols in (['activate_rt', 'activate_sub_rt'],
                     ['noactivate_rt', 'noactivate_sub_rt']
                     ):
            res[cols] += lmdi(actual[cols], pred[cols])
    elif sec_indicator == 'quick_new_self_rt':
        for cols in (['quicknew_haveself_rt', 'quicknew_self_sub_rt']
        ):
            res[cols] += lmdi(actual[cols], pred[cols])

    return actual, pred, res


def main(DATE, PERIOD, TARGET):
    global spark, IdcDict, new_tab, new_line, HiveData, sql

    SummaryConclusion = [f'{DATE} {PERIOD} {TARGET}\n异动总结: ']
    
    # 4. Broader Market
    Layer = '大盘' # layer column

    # Detail record
    Broader_Analyse = indicator_calculate(DATE, PERIOD)
    TOTALANOMALY = Broader_Analyse[TARGET]['actual']-Broader_Analyse[TARGET]['pred'] # Broader Market Bias
    Broader_Df = [DATE, PERIOD, TARGET, sql.usersource, Layer, '大盘', '大盘', Broader_Analyse[TARGET]['actual'],
                          Broader_Analyse[TARGET]['pred'], TOTALANOMALY, TOTALANOMALY, TOTALANOMALY, 1, np.nan]
    HiveData.loc[len(HiveData)] = Broader_Df

    # Conclusion
    Contri_Conclusion_Append = f"{IdcDict[TARGET]}的预测值为{Broader_Analyse[TARGET]['pred']:.02%}, 实际值为{Broader_Analyse[TARGET]['actual']:.02%}, 差值为{Broader_Analyse[TARGET]['actual']-Broader_Analyse[TARGET]['pred']:.02%}"
    Broader_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, '总结', '总结', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, Contri_Conclusion_Append]
    HiveData.loc[len(HiveData)] = Broader_Conclusion

    SummaryConclusion.append(Contri_Conclusion_Append)

    # 5. Specail Scene
    Layer = '特殊场景' # layer column
    Contri_Sorted = [] # sorted contribution to Anomaly, which conclude a summary

    # Detail record
    Specail_Analyse = specail_analyse(DATE, PERIOD, Broader_Analyse[TARGET]['pred'])
    Special_Df = []
    for idc, ret in Specail_Analyse:
        for ele, value in list(ret.items())[1:]:
            Special_Df.append(
                [DATE, PERIOD, TARGET, sql.usersource, Layer, idc, ele, *value, value[2], value[2], value[2]/ret['all'][2], np.nan]
            )
            Contri_Sorted.append((idc, ele, value[2]/ret['all'][2]))
    HiveData = HiveData.append(pd.DataFrame(data=Special_Df, columns=HiveData.columns), ignore_index=True)

    # Conclusion
    Contri_Sorted.sort(key=lambda x : x[2], reverse=True)
    cumsum_contri = 0 # positive cumsum contribution to anomaly
    idc_ele_contri = [] # indicator who contribute positively
    for idc, ele, contri in Contri_Sorted:
        if contri > 0:
            cumsum_contri += contri
            idc_ele_contri.append(ele)
            continue
        break
    Contri_Conclusion_Append = '但还达不到50%的水平，显然解释力不足，需要继续分析' if cumsum_contri< 0.5\
                                else f"特殊场景{'、'.join(idc_ele_contri)}对异动的贡献达到{cumsum_contri:.02%}，具有较强的解释力"
    Contri_Conclusion_Append = f"特殊场景中，{Contri_Sorted[0][1]}对异动的贡献最大，为{Contri_Sorted[0][2]:.02%}。{Contri_Conclusion_Append}。"
    Special_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, '总结', '总结', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, Contri_Conclusion_Append]
    HiveData.loc[len(HiveData)] = Special_Conclusion

    SummaryConclusion.append(f'\t·特殊场景: {Contri_Conclusion_Append}')

    # 6.BU Split
    Layer = 'BU拆分' # layer column
    Contri_Sorted = [] # sorted contribution to Anomaly, which conclude a summary

    # Detail record
    BU_Analyse = bu_split(DATE, PERIOD, Broader_Analyse[TARGET]['pred'])[TARGET]
    BU_Df = []
    for idc, value in list(BU_Analyse.items())[:-1]:
        for ele in value[0].index:
            BU_Df.append(
                [DATE, PERIOD, TARGET, sql.usersource, Layer, idc, ele, value[0][ele], value[1][ele], value[0][ele]-value[1][ele], value[2][ele], value[2][ele], value[2][ele]/TOTALANOMALY, np.nan]
            )
            Contri_Sorted.append((idc, ele, value[1][ele], value[0][ele]-value[1][ele], value[2][ele], value[2][ele]/TOTALANOMALY))
    HiveData = HiveData.append(pd.DataFrame(data=BU_Df, columns=HiveData.columns), ignore_index=True)

    # Conclusion
    Contri_Sorted.sort(key=lambda x : x[-1], reverse=True)
    Contri_Conclusion_Append = f"从结果看，贡献最大的Top3分别是: {Contri_Sorted[0][1]}的{Contri_Sorted[0][0]}({Contri_Sorted[0][2]:.02%})、{Contri_Sorted[1][1]}的{Contri_Sorted[1][0]}({Contri_Sorted[1][2]:.02%})、{Contri_Sorted[2][1]}的{Contri_Sorted[2][0]}({Contri_Sorted[2][2]:.02%})。"
    BU_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, '总结', '总结', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, Contri_Conclusion_Append]
    HiveData.loc[len(HiveData)] = BU_Conclusion

    SummaryConclusion.append(f'\t·业务线拆分: {Contri_Conclusion_Append}')

    # Determine the structure factors and permeability factors in top3
    struc_top3 = [] # structure factor
    permea_top3 = [] # permeability factor
    for idc, ele, pred, gap, contri, contri_ratio in Contri_Sorted[:3]:
        '''
        idc: Indicator,
        ele: Element,
        pred: Pred,
        gap: CTA
        contri: CTB,
        contri_ratio: CTBRatio
        '''
        if contri_ratio > 0:
            if idc == '结构占比':
                struc_top3.append((ele, pred, gap, contri))
            else:
                permea_top3.append((ele, pred, gap, contri))


    # 7. Structure Adtributor
    Layer = '结构分析' # layer column
    Contri_Top3 = [] # contains top3 factors of structures selected above and then generates conclusions

    # Detail record
    Struc_Df = []
    for pre_ele, _, pre_gap, pre_contri in struc_top3:
        Struc_Analyse = structure_split(DATE, PERIOD, pre_ele)[TARGET]
        for idc, value in list(Struc_Analyse.items())[:-1]:
            total_gap = np.sum(value['amt_actual']-value['amt_pred'])
            actual = value['amt_actual']
            pred = value['amt_pred']
            for ele in value.index:
                Struc_Df.append(
                    [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre_ele}-{idc}', ele, actual.loc[ele], pred.loc[ele], actual.loc[ele]-pred.loc[ele],
                    (actual.loc[ele]-pred.loc[ele])/total_gap*pre_gap, (actual.loc[ele]-pred.loc[ele])/total_gap*pre_contri,
                    (actual.loc[ele]-pred.loc[ele])/total_gap*pre_contri/TOTALANOMALY, np.nan]
                )
        Contri_Top3.append((pre_ele, pre_gap, Struc_Analyse['adtributor']))
    HiveData = HiveData.append(pd.DataFrame(data=Struc_Df, columns=HiveData.columns), ignore_index=True)

    # Conclusion
    dims_dict = {
        'platform': '支付平台',
        'new_post_flag': '收银台',
        'paycategory': '支付方式',
    }
    Contri_Conclusion_Append = '；'.join([
        f"造成{pre_ele}业务占比{'下降' if pre_gap<0 else '上升'}的主要维度的元素为: {'、'.join(['{0} 的 {1}({2:.02%})'.format(x[1],dims_dict[x[0]],x[2]) for x in ad])}"\
        for pre_ele, pre_gap, ad in Contri_Top3
    ])
    Struc_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, '总结', '总结', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, Contri_Conclusion_Append]
    HiveData.loc[len(HiveData)] = Struc_Conclusion

    SummaryConclusion.append(f'\t·结构变化归因: {Contri_Conclusion_Append}')

    # 8. First Permeability
    Layer = '渗透率一级指标'  # layer column
    Contri_Top3 = {}  # contains top3 factors of structures selected above and then generates conclusions
    First_Permea_Top3 = {}  # contains top3 factors from the result of first permeability splitting if it's about permeability

    # Detail record
    First_Df = []
    for pre_ele, pre_pred, pre_gap, pre_contri in permea_top3:
        Contri_Top3[pre_ele] = []
        First_Analyse = counter_split(DATE, PERIOD, pre_ele, pre_pred)
        actual = First_Analyse[0]
        pred = First_Analyse[1]
        gap = First_Analyse[2]
        for idc, ele in First_Analyse[0].index:
            First_Df.append(
                [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre_ele}-{idc}', ele, actual.loc[(idc, ele)], pred.loc[(idc, ele)],
                 actual.loc[(idc, ele)] - pred.loc[(idc, ele)],
                 gap.loc[(idc, ele)], gap.loc[(idc, ele)] / pre_gap * pre_contri,
                 gap.loc[(idc, ele)] / pre_gap * pre_contri / TOTALANOMALY, np.nan]
            )
            ct3 = (f'{pre_ele}-{idc}-{ele}', pred.loc[(idc, ele)], actual.loc[(idc, ele)] - pred.loc[(idc, ele)],
                   gap.loc[(idc, ele)] / pre_gap * pre_contri, gap.loc[(idc, ele)] / pre_gap * pre_contri / TOTALANOMALY)
            Contri_Top3[pre_ele].append(ct3)
    HiveData = HiveData.append(pd.DataFrame(data=First_Df, columns=HiveData.columns), ignore_index=True)

    # Conclusion
    XXX = []
    for pre_ele in Contri_Top3:
        Contri_Top3[pre_ele] = sorted(Contri_Top3[pre_ele], key=lambda x: x[-1], reverse=True)[:3]
        First_Permea_Top3[pre_ele] = []
        Conclusion_Append_End = []
        for ct in Contri_Top3[pre_ele]:
            ct_l = ct[0].split('-')
            if ct_l[1] == '渗透率':
                First_Permea_Top3[pre_ele].append(ct)
                Conclusion_Append_End.append(f'{ct_l[0]}业务{ct_l[2]}的{ct_l[1]}')
        Contri_Conclusion_Append = f"从{pre_ele}业务线的渗透率归因来看，对大盘贡献的Top3为{'、'.join(['{1}({0:.02%})'.format(Contri_Top3[pre_ele][i][-1], '的'.join(Contri_Top3[pre_ele][i][0].split('-')[2:0:-1])) for i in range(3)])}。接下来要分析{'、'.join(Conclusion_Append_End)}。"
        XXX.append(Contri_Conclusion_Append)
        First_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre_ele}-总结', '总结', np.nan, np.nan, np.nan, np.nan, np.nan,
                            np.nan, Contri_Conclusion_Append]
        HiveData.loc[len(HiveData)] = First_Conclusion

    SummaryConclusion.append(f"\t·一级渗透率归因: \n{new_line.join(['{0}{0}{1}'.format(new_tab, x) for x in XXX])}")

    # 9. Second Permeability
    Layer = '渗透率二级指标'  # layer column
    Contri_Top3 = {}  # contains top3 factors of structures selected above and then generates conclusions
    Sec_Permea_Top3 = {}  # contains top3 factors from the result of second permeability splitting if it's about permeability

    # Detail record
    sec_dict = {  # element to character mapping
        'pre_card_rt': '卡支付比例',
        'pre_card_p': '卡支付笔单价',
        'pre_nqh_rt': '拿去花支付比例',
        'pre_nqh_p': '拿去花支付笔单价',
        'pre_otherself_rt': '其他自有支付比例',
        'pre_otherself_p': '其他自有支付笔单价',
        'pre_cnt': '支付成功订单数',
        'pre_amt': '1/总体净额',
        'pre_refund': '自有退款',
        'quick_new_rt': '新开通标准极速比例',
        'quick_new_self_rt': '新开通且自有支付比例',
        'quick_new_self_p': '新开通标准极速且自有支付笔单价',
        'quick_old_rt': '已开通标准极速比例',
        'quick_old_self_rt': '已开通且自有支付比例',
        'quick_old_self_p': '已开通标准极速且自有支付笔单价',
        'quick_cnt': '支付成功订单数',
        'quick_amt': '1/总体净额',
        'quick_refund': '自有退款',
        'post_new_rt': '新开通后付比例',
        'post_new_self_rt': '新开通且自有支付比例',
        'post_new_self_p': '新开通后付且自有支付笔单价',
        'post_old_rt': '已开通后付比例',
        'post_old_self_rt': '已开通且自有支付比例',
        'post_old_self_p': '已开通后付且自有支付笔单价',
        'post_cnt': '支付成功订单数',
        'post_amt': '1/总体净额',
        'post_refund': '自有退款',
    }

    Sec_Df = []
    for pre2_ele, values in First_Permea_Top3.items():
        Contri_Top3[pre2_ele] = {}
        for pre_ele, pre_pred, pre_gap, pre_contri, _ in values:
            pre_ele = pre_ele.split('-')[2]  # attain previous element from string joint with '-'
            Contri_Top3[pre2_ele][pre_ele] = []
            Sec_Analyse = permeability_split(DATE, PERIOD, pre2_ele, pre_ele, pre_pred)
            actual = Sec_Analyse[0]
            pred = Sec_Analyse[1]
            gap = Sec_Analyse[2]
            for ele in actual.index:
                Sec_Df.append(
                    [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre2_ele}-{pre_ele}', sec_dict[ele], actual.loc[ele], pred.loc[ele],
                     actual.loc[ele] - pred.loc[ele],
                     gap.loc[ele], gap.loc[ele] / pre_gap * pre_contri, gap.loc[ele] / pre_gap * pre_contri / TOTALANOMALY,
                     np.nan]
                )
                ct3 = (f'{pre2_ele}-{pre_ele}-{ele}', pred.loc[ele], actual.loc[ele] - pred.loc[ele],
                       gap.loc[ele] / pre_gap * pre_contri,
                       gap.loc[ele] / pre_gap * pre_contri / TOTALANOMALY)
                Contri_Top3[pre2_ele][pre_ele].append(ct3)
    HiveData = HiveData.append(pd.DataFrame(data=Sec_Df, columns=HiveData.columns), ignore_index=True)

    # Conclusion
    XXX = []
    valid_sec_to_third = ['pre_card_rt', 'pre_nqh_rt',
                          'quick_new_self_rt']  # the second elements that can be split into third elements
    for pre2_ele in Contri_Top3:
        Sec_Permea_Top3[pre2_ele] = {}
        for pre_ele in Contri_Top3[pre2_ele]:
            Contri_Top3[pre2_ele][pre_ele] = sorted(Contri_Top3[pre2_ele][pre_ele], key=lambda x: x[-1], reverse=True)[:3]
            Sec_Permea_Top3[pre2_ele][pre_ele] = []
            Conclusion_Append_End = []
            for ct in Contri_Top3[pre2_ele][pre_ele]:
                ct_l = ct[0].split('-')
                if ct_l[2] in valid_sec_to_third:
                    Sec_Permea_Top3[pre2_ele][pre_ele].append(ct)
                    Conclusion_Append_End.append(f'{ct_l[0]}业务{ct_l[1]}收银台的{sec_dict[ct_l[2]]}')
            Contri_Conclusion_Append = f"从{pre2_ele}业务线的{pre_ele}收银台{Layer}归因来看，对大盘贡献的Top3为: {'、'.join(['{1}({0:.02%})'.format(Contri_Top3[pre2_ele][pre_ele][i][-1], sec_dict[Contri_Top3[pre2_ele][pre_ele][i][0].split('-')[2]]) for i in range(3)])}。接下来要分析: {'、'.join(Conclusion_Append_End)}。"
            XXX.append(Contri_Conclusion_Append)
            Sec_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre2_ele}-{pre_ele}-渗透率-总结', '总结', np.nan, np.nan,
                              np.nan, np.nan, np.nan, np.nan, Contri_Conclusion_Append]
            HiveData.loc[len(HiveData)] = Sec_Conclusion

    SummaryConclusion.append(f"\t·二级渗透率归因: \n{new_line.join(['{0}{0}{1}'.format(new_tab, x) for x in XXX])}")

    # 10. Third Permeability
    Layer = '渗透率三级指标'  # layer column
    Contri_Top3 = {}  # contains top3 factors of structures selected above and then generates conclusions

    # Detail record
    third_dict = {  # element to character mapping
        'nocard_rt': '进入页无卡比例',
        'havecard_rt': '进入页有卡比例',
        'nocard_bindcard_rt': '绑卡成功率',
        'nocard_bindcard_sub_rt': '绑卡后支付提交率',
        'havecard_default_rt': '默勾卡率',
        'havecard_default_sub_rt': '默勾后支付提交率',
        'havecard_change_rt': '换勾卡率',
        'havecard_change_sub_rt': '换勾后支付提交率',
        'havecard_bindcard_rt': '新绑卡成功率',
        'havecard_bindcard_sub_rt': '新绑卡后支付提交率',
        'noactivate_rt': '进入页拿去花未激活比例',
        'activate_rt': '进入页拿去花已激活比例',
        'noactivate_sub_rt': '拿去花未激活开通提交率',
        'activate_sub_rt': '拿去花已激活勾选提交率',
        'quicknew_haveself_rt': '进入授权页有自有支付工具占比',
        'quicknew_self_sub_rt': '自有支付工具提交率',
    }

    Third_Df = []
    for pre3_ele in Sec_Permea_Top3:
        Contri_Top3[pre3_ele] = {}
        for pre2_ele, values in Sec_Permea_Top3[pre3_ele].items():
            if len(values) > 0: Contri_Top3[pre3_ele][pre2_ele] = {}
            for pre_ele, pre_pred, pre_gap, pre_contri, _ in values:
                pre_ele = pre_ele.split('-')[2]
                pre_ele_character = sec_dict[pre_ele]  # attain previous element from string joint with '-' and use mapping
                Contri_Top3[pre3_ele][pre2_ele][pre_ele_character] = []
                Third_Analyse = detail_split(DATE, PERIOD, pre3_ele, pre_ele, pre_pred)
                actual = Third_Analyse[0]
                pred = Third_Analyse[1]
                gap = Third_Analyse[2]
                for ele in actual.index:
                    Third_Df.append(
                        [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre3_ele}-{pre2_ele}-{pre_ele_character}', third_dict[ele],
                         actual.loc[ele], pred.loc[ele], actual.loc[ele] - pred.loc[ele],
                         gap.loc[ele], gap.loc[ele] / pre_gap * pre_contri,
                         gap.loc[ele] / pre_gap * pre_contri / TOTALANOMALY, np.nan]
                    )
                    ct3 = (f'{pre3_ele}-{pre2_ele}-{pre_ele_character}-{third_dict[ele]}', pred.loc[ele],
                           actual.loc[ele] - pred.loc[ele], gap.loc[ele] / pre_gap * pre_contri,
                           gap.loc[ele] / pre_gap * pre_contri / TOTALANOMALY)
                    Contri_Top3[pre3_ele][pre2_ele][pre_ele_character].append(ct3)
    HiveData = HiveData.append(pd.DataFrame(data=Third_Df, columns=HiveData.columns), ignore_index=True)

    # Conclusion
    XXX = []
    for pre3_ele in Contri_Top3:
        for pre2_ele in Contri_Top3[pre3_ele]:
            for pre_ele in Contri_Top3[pre3_ele][pre2_ele]:
                Contri_Top3[pre3_ele][pre2_ele][pre_ele] = sorted(Contri_Top3[pre3_ele][pre2_ele][pre_ele],
                                                                  key=lambda x: x[-1], reverse=True)[:3]
                Contri_Conclusion_Append = f"拆分{pre3_ele}业务线的{pre2_ele}收银台的{pre_ele}指标进行{Layer}归因，发现对大盘贡献的Top3为: {'、'.join(['{1}({0:.02%})'.format(Contri_Top3[pre3_ele][pre2_ele][pre_ele][i][-1], Contri_Top3[pre3_ele][pre2_ele][pre_ele][i][0].split('-')[3]) for i in range(3)])}。"
                XXX.append(Contri_Conclusion_Append)
                Third_Conclusion = [DATE, PERIOD, TARGET, sql.usersource, Layer, f'{pre3_ele}-{pre2_ele}-{pre_ele}-渗透率-总结', '总结',
                                    np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, Contri_Conclusion_Append]
                HiveData.loc[len(HiveData)] = Third_Conclusion

    SummaryConclusion.append(f"\t·三级渗透率归因: \n{new_line.join(['{0}{0}{1}'.format(new_tab, x) for x in XXX])}")

    # 11. Summary
    HiveData.loc[len(HiveData)] = [DATE, PERIOD, TARGET, sql.usersource, '总结', '总结', '总结', np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, '\n\n'.join(SummaryConclusion)]

    pass

if __name__ == '__main__':
    DATE = sys.argv[1]
    PERIOD = sys.argv[2]
    TARGET = sys.argv[3]

    # Determine which user source
    sql = SQL('CTRIP')
    main(DATE, PERIOD, TARGET)
    sql = SQL('QUNAR')
    main(DATE, PERIOD, TARGET)

    # Transform DataFrame to Spark DataFrame
    Spark_HiveData = spark.createDataFrame(HiveData)

    # Add partition columns(codes below means add column 'd' which only contains the constant value 'DATE')
    Spark_HiveData = Spark_HiveData.withColumn("d", lit(DATE))

    # Overwrite table
    Spark_HiveData.write \
        .mode("overwrite") \
        .partitionBy("d") \
        .saveAsTable("report_paydb.adm_anomaly_analyse")

