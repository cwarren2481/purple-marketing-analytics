view: 30_day_attributions {
  derived_table: {
    sql: with a as (
  select date, campaign_name
      , sum(spend) as spend
      , sum(impressions) as impressions
      , sum(clicks) as clicks
      from analytics.marketing.adspend
      group by campaign_name, date
)
, b as (
  select se.session_id, se.user_id
  , case when se.utm_campaign = l.internal_campaign_id then l.campaign_name else se.utm_campaign end as total_campaign_name
, case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
, p.dollars as amount
, to_date(se.time) as date
, row_number() over (partition by se.user_id order by se.time) session_cnt
from analytics.heap.sessions se
left join analytics.heap.purchase p
on se.user_id = p.user_id
and se.session_id = p.session_id
left join analytics.utm_lookup.utm_campaign l
on l.internal_campaign_id = try_to_numeric(se.utm_campaign)
where se.user_id in (select distinct user_id from analytics.heap.purchase where time >= '2018-10-15')
and (p.dollars > 0 or p.dollars is null)
and total_campaign_name is not null
)
select a.date, a.campaign_name, a.spend, a.clicks, a.impressions
, sum(d.thirty_day_any_touch) as thirty_day_any_touch
, (thirty_day_any_touch / a.spend) as ROI_ANY_TOUCH
, sum(d.thirty_day_first_touch) as thirty_day_first_touch
, (thirty_day_first_touch / a.spend) as ROI_FIRST_TOUCH
, sum (d.thirty_day_last_touch) as thirty_day_last_touch
, (thirty_day_last_touch / a.spend) as ROI_LAST_TOUCH
from a
left join (select b.date, b.user_id, b.session_id, b.total_campaign_name, b.purchase_flag, b.amount
, sum(b2.amount) as thirty_day_any_touch
, sum(case when b.session_cnt ='1' then b2.amount end) thirty_day_first_touch
, sum(case when b.amount is not null and b.session_cnt <> '1' then b2.amount end) thirty_day_last_touch
from b
left join b as b2
on b.user_id = b2.user_id
and b2.date between b.date and dateadd(day,30,b.date)
group by b.date, b.user_id, b.session_id, b.total_campaign_name, b.purchase_flag, b.amount) d
on d.total_campaign_name = a.campaign_name
and d.date = a.date
where a.date >= '2018-10-15'
and clicks > 0
and thirty_day_any_touch is not null
group by a.date, a.campaign_name, a.spend, a.clicks, a.impressions
, d.user_id, d.thirty_day_any_touch, d.thirty_day_first_touch, d.thirty_day_last_touch
order by clicks
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

  measure: roi_any_touch {
    type: sum
    value_format: "#.00\%"
    sql: ${TABLE}."ROI_ANY_TOUCH" ;;
  }

  measure: roi_first_touch {
    type: sum
    value_format: "#.00\%"
    sql: ${TABLE}."ROI_FIRST_TOUCH" ;;
  }

  measure: roi_last_touch {
    type: sum
    value_format: "#.00\%"
    sql: ${TABLE}."ROI_LAST_TOUCH" ;;
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
