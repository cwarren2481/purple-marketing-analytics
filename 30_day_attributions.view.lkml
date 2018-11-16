view: 30_day_attributions {
  derived_table: {
    sql: with a as (select date, campaign_name, platform
      , sum(spend) as spend
      , sum(impressions) as impressions
      , sum(clicks) as clicks
      from analytics.marketing.adspend
      group by campaign_name, date, platform)

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

, c as (
  select b.*
        , first_value(session_cnt) over (partition by user_id order by date) as first_value
        from b
        group by session_id, user_id, utm_campaign, purchase_flag, amount, date, session_cnt)

select a.date, a.campaign_name, a.platform, a.spend, a.clicks, a.impressions
, sum(d.thirty_day_any_touch) as thirty_day_any_touch
, sum(d.thirty_day_first_touch) as thirty_day_first_touch
, sum (d.thirty_day_last_touch) as thirty_day_last_touch
from a

left join (select c.date, c.user_id, c.session_id, c.utm_campaign, c.purchase_flag, c.amount
, sum(c2.amount) as thirty_day_any_touch
, sum(case when c.session_cnt = c.first_value then c2.amount end) thirty_day_first_touch
, sum(case when c.amount is not null and c.session_cnt <> '1' then c2.amount end) thirty_day_last_touch
from c
left join c as c2
on c.user_id = c2.user_id
and c.date between c2.date and dateadd(day,30,c2.date)
group by c.date, c.user_id, c.session_id, c.utm_campaign, c.purchase_flag, c.amount) d

on d.utm_campaign = a.campaign_name
and d.date = a.date
where a.date >= '2018-10-15'
and a.campaign_name is not null
group by a.date, a.campaign_name, a.platform, a.spend, a.clicks, a.impressions
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

  dimension:  platform {
    type: string
    sql: ${TABLE}."PLATFORM" ;;
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
      platform,
      spend,
      clicks,
      impressions,
      thirty_day_any_touch,
      thirty_day_first_touch,
      thirty_day_last_touch
    ]
  }
}
