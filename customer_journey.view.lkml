view: customer_journey {
  derived_table: {
    sql: select a.event_id, a.user_id, a.session_id, a.device_type, a.time
  , a.landing_page
  , a.path
  , b.time as order_date
  , b.event_id as order_event_id
  , c.EVENT_TABLE_NAME as EVENT_NAME
  , b.region
  , b.dollars
  , datediff(days, a.time, b.time) days_diff
  from PAGEVIEWS a
  left join PURCHASE b on a.USER_ID = b.USER_ID
  left join ALL_EVENTS c on a.EVENT_ID = c.EVENT_ID
  where a.time <= b.session_time),
ses as (
  -- session origination data
  select p.session_id, p.query, p.utm_source, p.utm_campaign, p.utm_medium, p.utm_term, p.utm_content, p.referrer
    , case
        when p.utm_source ilike '%google%' or p.referrer ilike '%google%' then 'GOOGLE'
        when p.utm_source ilike '%facebook%' or p.referrer ilike '%facebook%' then 'FACEBOOK'
        when p.utm_source is not null or p.referrer is not null then 'OTHER'
        else null
    end channel
  from (
    select *, row_number() over (partition by session_id order by time) row_cnt from pageviews) p
  where row_cnt = 1),
cmb as (
  -- join marketing data into customer journey data
  select cj.*, ses.query, ses.utm_source, ses.utm_campaign, ses.utm_medium, ses.utm_term, ses.utm_content, ses.referrer, ses.channel
  from cj left join ses on cj.session_id = ses.session_id),
spd as (
  -- spend data 2018 forward (since revenue is only since dec 2017)
  select date, platform,sum(impressions) impressions, sum(clicks) clicks, sum(spend) spend
  from ANALYTICS.MARKETING.ADSPEND
  where platform in ('GOOGLE','FACEBOOK')
  and date >= '2018-01-01'
  group by date, platform),
dai as (
  -- MAIN QUERY daily metrics by platform (set attribution window here)
  select date, platform, spend, impressions, clicks
      , count(distinct order_event_id) orders
  from spd
  left join cmb on cmb.channel = spd.platform and to_date(cmb.time) = spd.date
  where days_diff <= 30 -- ATTRIBUTION WINDOW HOW CAN WE MAKE THIS VARIABLE IN THE REPORT?? OR MAYBE 1, 7, 30, 60
  group by date, platform, spend, impressions, clicks
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: event_id {
    type: number
    sql: ${TABLE}."EVENT_ID" ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}."USER_ID" ;;
  }

  dimension: session_id {
    type: number
    sql: ${TABLE}."SESSION_ID" ;;
  }

  dimension: device_type {
    type: string
    sql: ${TABLE}."DEVICE_TYPE" ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}."REFERRER" ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}."UTM_SOURCE" ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}."UTM_CAMPAIGN" ;;
  }

  dimension: utm_medium {
    type: string
    sql: ${TABLE}."UTM_MEDIUM" ;;
  }

  dimension: path {
    type: string
    sql: ${TABLE}."PATH" ;;
  }

  dimension_group: order_date {
    type: time
    sql: ${TABLE}."ORDER_DATE" ;;
  }

  dimension: event_name {
    type: string
    sql: ${TABLE}."EVENT_NAME" ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}."REGION" ;;
  }

  dimension: dollars {
    type: string
    sql: ${TABLE}."DOLLARS" ;;
  }

  set: detail {
    fields: [
      event_id,
      user_id,
      session_id,
      device_type,
      referrer,
      utm_source,
      utm_campaign,
      utm_medium,
      path,
      order_date_time,
      event_name,
      region,
      dollars
    ]
  }
}
