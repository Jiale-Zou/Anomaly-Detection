with bounds as (
  select
  *
  , row_number() over(partition by usersource order by d desc) rk
  from report_paydb.adm_anomaly_bounds
  where d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  having rk = 1
), this_year as (
  select
  label_period
  , usersource
  ,sum(case when paycategory='高费率信用卡' then self_amt end) highRate_amt
  ,sum(self_amt) self_amt
  ,sum(amt) amt
  ,row_number() over(partition by usersource order by label_period desc) rk
  from report_paydb.adm_kpi_selfpay_period
  where period = '日'
  and d between date_sub('${zdt.addDay(-1).format("yyyy-MM-dd")}', 299) and '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  group by 1,2
  order by 1 desc
), last_year as (
  select
  label_period
  , usersource
  ,sum(case when paycategory='高费率信用卡' then self_amt end) highRate_amt
  ,sum(self_amt) self_amt
  ,sum(amt) amt
  ,row_number() over(partition by usersource order by label_period desc) rk
  from report_paydb.adm_kpi_selfpay_period
  where period = '日'
  and d between date_sub(add_months('${zdt.addDay(-1).format("yyyy-MM-dd")}',-12), 299) and add_months('${zdt.addDay(-1).format("yyyy-MM-dd")}',-12)
  group by 1,2
  order by 1 desc
), tb as (
  select 
  a.label_period
  , a.usersource
  , a.self_amt/a.amt as this_year_self_rt
  , a.highRate_amt/a.amt as this_year_highRate_rt
  , b.self_amt/b.amt as last_year_self_rt
  , b.highRate_amt/b.amt as last_year_highRate_rt
  , sum(a.self_amt) over(partition by a.usersource rows between current row and 6 following) this_self_7
  , sum(a.highRate_amt) over(partition by a.usersource rows between current row and 6 following) this_highRate_7
  , sum(a.amt) over(partition by a.usersource rows between current row and 6 following) this_amt_7
  , sum(b.self_amt) over(partition by a.usersource rows between current row and 6 following) last_self_7
  , sum(b.highRate_amt) over(partition by a.usersource rows between current row and 6 following) last_highRate_7
  , sum(b.amt) over(partition by a.usersource rows between current row and 6 following) last_amt_7
  , sum(a.self_amt) over(partition by a.usersource rows between current row and 29 following) this_self_30
  , sum(a.highRate_amt) over(partition by a.usersource rows between current row and 29 following) this_highRate_30
  , sum(a.amt) over(partition by a.usersource rows between current row and 29 following) this_amt_30
  , sum(b.self_amt) over(partition by a.usersource rows between current row and 29 following) last_self_30
  , sum(b.highRate_amt) over(partition by a.usersource rows between current row and 29 following) last_highRate_30
  , sum(b.amt) over(partition by a.usersource rows between current row and 29 following) last_amt_30
  , ROW_NUMBER() over(partition by a.usersource order by a.label_period asc) rk
  from this_year a
  left join last_year b
    on a.rk = b.rk
    and a.usersource = b.usersource
  order by 1 desc
), res as (
  -- 日
  select
  label_period
  , tb.usersource
  , '日' as period
  , this_year_self_rt
  , this_year_self_rt / (LEAD(this_year_self_rt, 1) over(partition by tb.usersource order by label_period desc)) as self_rt_qoq
  , this_year_self_rt / last_year_self_rt as self_rt_yoy
  , this_year_self_rt * (sum(last_year_self_rt) over(partition by tb.usersource rows between current row and 6 following)) / (sum(this_year_self_rt) over(partition by tb.usersource rows between current row and 6 following) * last_year_self_rt) as self_rt_qy_value
  , self_rt_day_top as self_rt_top
  , self_rt_day_bottom as self_rt_bottom
  , this_year_self_rt / avg(this_year_self_rt) over(partition by tb.usersource rows between current row and 6 following) self_rt_qy_value_numerator
  , last_year_self_rt / avg(last_year_self_rt) over(partition by tb.usersource rows between current row and 6 following) self_rt_qy_value_denominator
  , this_year_highRate_rt
  , this_year_highRate_rt / (LEAD(this_year_highRate_rt, 1) over(partition by tb.usersource order by label_period desc)) as highRate_rt_qoq
  , this_year_highRate_rt / last_year_highRate_rt as highRate_rt_yoy
  , this_year_highRate_rt * sum(last_year_highRate_rt) over(partition by tb.usersource rows between current row and 6 following) / (sum(this_year_highRate_rt) over(partition by tb.usersource rows between current row and 6 following) * last_year_highRate_rt) as highRate_rt_qy_value
  , highrate_rt_day_top as highRate_rt_top
  , highrate_rt_day_bottom as highRate_rt_bottom
  , this_year_highRate_rt / avg(this_year_highRate_rt) over(partition by tb.usersource rows between current row and 6 following) highRate_rt_qy_value_numerator
  , last_year_highRate_rt / avg(last_year_highRate_rt) over(partition by tb.usersource rows between current row and 6 following) highRate_rt_qy_value_denominator
  from tb
  left join bounds bd
    on tb.usersource = bd.usersource
  where tb.rk >= 7
  
  UNION all
  
  -- 周
  select
  label_period
  , A.usersource
  , '周' as period
  , this_year_self_rt
  , this_year_self_rt / (LEAD(this_year_self_rt, 7) over(partition by A.usersource order by label_period desc)) as self_rt_qoq
  , this_year_self_rt / last_year_self_rt as self_rt_yoy
  , this_year_self_rt * (LEAD(last_year_self_rt, 7) over(partition by A.usersource order by label_period desc)) / (LEAD(this_year_self_rt, 7) over(partition by A.usersource order by label_period desc) * last_year_self_rt) as self_rt_qy_value
  , self_rt_week_top as self_rt_top 
  , self_rt_week_bottom as self_rt_bottom
  , this_year_self_rt / LEAD(this_year_self_rt, 7) over(partition by A.usersource order by label_period desc) self_rt_qy_value_numerator
  , last_year_self_rt / LEAD(last_year_self_rt, 7) over(partition by A.usersource order by label_period desc) self_rt_qy_value_denominator
  , this_year_highRate_rt
  , this_year_highRate_rt / (LEAD(this_year_highRate_rt, 7) over(partition by A.usersource order by label_period desc)) as highRate_rt_qoq
  , this_year_highRate_rt / last_year_highRate_rt as highRate_rt_yoy
  , this_year_highRate_rt * (LEAD(last_year_highRate_rt, 7) over(partition by A.usersource order by label_period desc)) / (LEAD(this_year_highRate_rt, 7) over(partition by A.usersource order by label_period desc) * last_year_highRate_rt) as highRate_rt_qy_value
  , highrate_rt_week_top as highRate_rt_top
  , highrate_rt_week_bottom as highRate_rt_bottom
  , this_year_highRate_rt / LEAD(this_year_highRate_rt, 7) over(partition by A.usersource order by label_period desc) highRate_rt_qy_value_numerator
  , last_year_highRate_rt / LEAD(last_year_highRate_rt, 7) over(partition by A.usersource order by label_period desc) highRate_rt_qy_value_denominator
  from (
    select 
    label_period
    , usersource
    , this_self_7/this_amt_7 as this_year_self_rt
    , this_highRate_7/this_amt_7 as this_year_highRate_rt
    , last_self_7/last_amt_7 as last_year_self_rt
    , last_highRate_7/last_amt_7 as last_year_highRate_rt
    , rk
    from tb
  ) A
  left join bounds bd
    on A.usersource = bd.usersource
  where A.rk >= 14
  
  UNION ALL
  
  -- 月
  select
  label_period
  , A.usersource
  , '月' as period
  , this_year_self_rt
  , this_year_self_rt / (LEAD(this_year_self_rt, 30) over(partition by A.usersource order by label_period desc)) as self_rt_qoq
  , this_year_self_rt / last_year_self_rt as self_rt_yoy
  , this_year_self_rt * (LEAD(last_year_self_rt, 30) over(partition by A.usersource order by label_period desc)) / (LEAD(this_year_self_rt, 30) over(partition by A.usersource order by label_period desc) * last_year_self_rt) as self_rt_qy_value
  , self_rt_month_top as self_rt_top
  , self_rt_month_bottom as self_rt_bottom
  , this_year_self_rt / LEAD(this_year_self_rt, 30) over(partition by A.usersource order by label_period desc) self_rt_qy_value_numerator
  , last_year_self_rt / LEAD(last_year_self_rt, 30) over(partition by A.usersource order by label_period desc) self_rt_qy_value_denominator
  , this_year_highRate_rt
  , this_year_highRate_rt / (LEAD(this_year_highRate_rt, 30) over(partition by A.usersource order by label_period desc)) as highRate_rt_qoq
  , this_year_highRate_rt / last_year_highRate_rt as highRate_rt_yoy
  , this_year_highRate_rt * (LEAD(last_year_highRate_rt, 30) over(partition by A.usersource order by label_period desc)) / (LEAD(this_year_highRate_rt, 30) over(partition by A.usersource order by label_period desc) * last_year_highRate_rt) as highRate_rt_qy_value
  , highrate_rt_month_top as highRate_rt_top
  , highrate_rt_month_bottom as highRate_rt_bottom
  , this_year_highRate_rt / LEAD(this_year_highRate_rt, 30) over(partition by A.usersource order by label_period desc) highRate_rt_qy_value_numerator
  , last_year_highRate_rt / LEAD(last_year_highRate_rt, 30) over(partition by A.usersource order by label_period desc) highRate_rt_qy_value_denominator
  from  (
    select 
    label_period
    , usersource
    , this_self_30/this_amt_30 as this_year_self_rt
    , this_highRate_30/this_amt_30 as this_year_highRate_rt
    , last_self_30/last_amt_30 as last_year_self_rt
    , last_highRate_30/last_amt_30 as last_year_highRate_rt
    , rk
    from tb
  ) A
  left join bounds bd
    on A.usersource = bd.usersource
  where A.rk >= 60
)

INSERT OVERWRITE TABLE report_paydb.adm_anomaly_detect
partition(d)

select 
label_period
,usersource
,period
,this_year_self_rt
,self_rt_qoq
,self_rt_yoy
,self_rt_qy_value
,self_rt_top
,self_rt_bottom
,self_rt_qy_value_numerator
,self_rt_qy_value_denominator
,this_year_highRate_rt
,highRate_rt_qoq
,highRate_rt_yoy
,highRate_rt_qy_value
,highRate_rt_top
,highRate_rt_bottom
,highRate_rt_qy_value_numerator
,highRate_rt_qy_value_denominator
,(case when self_rt_qy_value between self_rt_bottom and self_rt_top then 0 else 1 end) self_rt_flag
,(case when highRate_rt_qy_value between highRate_rt_bottom and highRate_rt_top then 0 else 1 end) highRate_rt_flag
,label_period as d
from res
-- where self_rt_yoy is not null
where label_period = '${zdt.addDay(-1).format("yyyy-MM-dd")}'