view: 30_day_attributions {
  derived_table: {
    sql: with a as (select date, campaign_name
      , sum(spend) as spend
      , sum(impressions) as impressions
      , sum(clicks) as clicks
      from analytics.marketing.adspend
      group by campaign_name, date)

, b as (
  select se.session_id, se.user_id, se.utm_campaign
, case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
, p.dollars as amount
, to_date(se.time) as date
, row_number() over (partition by se.user_id order by se.time) session_cnt
from analytics.heap.sessions se
left join analytics.heap.purchase p
on se.user_id = p.user_id
and se.session_id = p.session_id
where se.user_id in (select distinct user_id from analytics.heap.purchase where time >= '2018-10-15')
and (p.dollars > 0 or p.dollars is null))

select d.user_id, a.date, a.campaign_name, a.spend, a.clicks, a.impressions
, sum(d.thirty_day_any_touch) as thirty_day_any_touch
, sum(d.thirty_day_first_touch) as thirty_day_first_touch
, sum (d.thirty_day_last_touch) as thirty_day_last_touch
from a

left join (select b.date, b.user_id, b.session_id, b.utm_campaign, b.purchase_flag, b.amount
, sum(b2.amount) as thirty_day_any_touch
, sum(case when b.session_cnt ='1' then b2.amount end) thirty_day_first_touch
, sum(case when b.amount is not null and b.session_cnt <> '1' then b2.amount end) thirty_day_last_touch
from b
left join b as b2
on b.user_id = b2.user_id
and b2.date between b.date and dateadd(day,30,b.date)
group by b.date, b.user_id, b.session_id, b.utm_campaign, b.purchase_flag, b.amount) d

on d.utm_campaign = a.campaign_name
and d.date = a.date
where thirty_day_any_touch is not null
group by a.date, a.campaign_name, a.spend, a.clicks, a.impressions, d.user_id
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: date {
    type: date
    sql: ${TABLE}."DATE" ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

  measure: spend {
    type: sum
    value_format: "$#,##0.00"
    sql: ${TABLE}."SPEND" ;;
  }

  measure: clicks {
    type: sum
    sql: ${TABLE}."CLICKS" ;;
  }

  measure: impressions {
    type: sum
    sql: ${TABLE}."IMPRESSIONS" ;;
  }

  measure: thirty_day_any_touch {
    type: sum
    value_format: "$#,##0.00"
    sql: ${TABLE}."THIRTY_DAY_ANY_TOUCH" ;;
  }

  measure: thirty_day_first_touch {
    type: sum
    value_format: "$#,##0.00"
    sql: ${TABLE}."THIRTY_DAY_FIRST_TOUCH" ;;
  }

  measure: thirty_day_last_touch {
    type: sum
    value_format: "$#,##0.00"
    sql: ${TABLE}."THIRTY_DAY_LAST_TOUCH" ;;
  }

  set: detail {
    fields: [
      date,
      campaign_name,
      spend,
      clicks,
      impressions,
      thirty_day_any_touch,
      thirty_day_first_touch,
      thirty_day_last_touch
    ]
  }
}
