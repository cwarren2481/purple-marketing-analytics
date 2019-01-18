view: 30_day_attributions {
  derived_table: {
    sql:with a as (
  select date, campaign_name
      , sum(spend) as spend
      , sum(impressions) as impressions
      , sum(clicks) as clicks
      from analytics.marketing.adspend
      group by campaign_name, date
)
, b as (
  select to_date(s.time) as date
  , s.session_id
  , s.user_id
  , p.dollars as amount
, case when s.utm_campaign = l.internal_campaign_id then l.campaign_name else s.utm_campaign end as campaign_name
, case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
, row_number() over (partition by s.user_id order by to_date(s.time)) session_cnt
  from analytics.heap.sessions s
  left join analytics.utm_lookup.utm_campaign l
  on s.utm_campaign = l.internal_campaign_id
  left join analytics.heap.purchase p
    on s.user_id = p.user_id
    and s.session_id = p.session_id
    where s.user_id in (select distinct user_id from analytics.heap.purchase where time >= '2018-10-15')
    and (p.dollars > 0 or p.dollars is null)
)
, c as (
select b.campaign_name, b.date
    , sum(b2.amount) as thirty_day_any_touch
    , sum(case when b.session_cnt ='1' then b2.amount end) thirty_day_first_touch
    , sum(case when b.amount is not null and b.session_cnt <> '1' then b2.amount end) thirty_day_last_touch
    from b
    left join b as b2
    on b.user_id = b2.user_id
    and b2.date between b.date and dateadd(day,30,b.date)
    group by b.campaign_name, b.date
)
select a.date
, a.campaign_name
, a.clicks
, a.impressions
, a.spend as spend
, thirty_day_any_touch
, thirty_day_first_touch
, thirty_day_last_touch
from a
left join c
on c.campaign_name = a.campaign_name
and c.date = a.date
where a.date >= '2018-10-15'
;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

  measure: spend {
    type: sum
    value_format: "$#,##0"
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
    value_format: "$#,##0"
    sql: ${TABLE}."THIRTY_DAY_ANY_TOUCH" ;;
  }

  measure: thirty_day_first_touch {
    type: sum
    value_format: "$#,##0"
    sql: ${TABLE}."THIRTY_DAY_FIRST_TOUCH" ;;
  }

  measure: thirty_day_last_touch {
    type: sum
    value_format: "$#,##0"
    sql: ${TABLE}."THIRTY_DAY_LAST_TOUCH" ;;
  }

  measure: roi_any_touch {
    type: number
    value_format: "0.###"
    sql: ${thirty_day_any_touch} / NULLIF(${spend}, 0) ;;

  }

  measure: roi_first_touch {
    type: number
    value_format: "0.###"
    sql: ${thirty_day_first_touch} / NULLIF(${spend}, 0) ;;

  }

  measure: roi_last_touch {
    type: number
    value_format: "0.###"
    sql: ${thirty_day_last_touch} / NULLIF(${spend}, 0) ;;


  }
  set: detail {
    fields: [
      campaign_name,
      spend,
      clicks,
      impressions,
      thirty_day_any_touch,
      thirty_day_first_touch,
      thirty_day_last_touch,
      roi_any_touch,
      roi_first_touch,
      roi_last_touch
    ]
  }
}
