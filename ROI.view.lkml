view: ROI {
  derived_table: {
    sql: -- ************************************************************************************
      -- ********** Customer Journey / ROI Dataset ******************************************
      -- ************************************************************************************
      with cj as (
        -- from Cameron
        select a.event_id, a.user_id, a.session_id, a.device_type, a.time
        , a.landing_page
        , a.path
        , b.time as order_date
        , b.event_id as order_event_id
        , c.EVENT_TABLE_NAME as EVENT_NAME
        , b.region
        , b.dollars
        , datediff(days, a.time, b.time) days_diff
        from heap.PAGEVIEWS a
        left join heap.PURCHASE b on a.USER_ID = b.USER_ID
        left join heap.ALL_EVENTS c on a.EVENT_ID = c.EVENT_ID
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
          select *, row_number() over (partition by session_id order by time) row_cnt from heap.pageviews) p
        where row_cnt = 1),
      cmb as (
        -- join marketing data into customer journey data
        select cj.*, ses.query, ses.utm_source, ses.utm_campaign, ses.utm_medium, ses.utm_term, ses.utm_content, ses.referrer, ses.channel
        from cj left join ses on cj.session_id = ses.session_id),
      spd as (
        -- spend data 2018 forward (since revenue is only since dec 2017)
        select date, platform,sum(impressions) impressions, sum(clicks) clicks, sum(spend) spend, campaign_name, source
        from ANALYTICS.MARKETING.ADSPEND
        where date >= '2018-01-01'
        group by date, platform, campaign_name, source),
      dai as (
        -- MAIN QUERY daily metrics by platform (set attribution window here)
        select date, platform,campaign_name, source, spend, impressions, clicks, dollars as revenue
            , count(distinct order_event_id) orders,
        case when days_diff <= 30 then 1 else 0 end as thirty_day,
        case when days_diff <= 60 then 1 else 0 end as sixty_day,
        case when days_diff <= 7 then 1 else 0 end as seven_day,
        case when days_diff <= 1 then 1 else 0 end as one_day
        from spd
        left join cmb on cmb.channel = spd.platform and to_date(cmb.time) = spd.date
        /*where days_diff <= 30*/ -- ATTRIBUTION WINDOW HOW CAN WE MAKE THIS VARIABLE IN THE REPORT?? OR MAYBE 1, 7, 30, 60
        group by date, platform, campaign_name, source, spend, impressions, clicks, dollars,
        case when days_diff <= 30 then 1 else 0 end,
        case when days_diff <= 60 then 1 else 0 end,
        case when days_diff <= 7 then 1 else 0 end,
        case when days_diff <= 1 then 1 else 0 end)
      -- SAMPLE GROUPING BY PLATFORM
      select * from dai

      -- ************************************************************************************
      -- ********** /END OF Customer Journey / ROI Dataset *************************************
      -- ************************************************************************************
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

  dimension: platform {
    type: string
    sql: ${TABLE}."PLATFORM" ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}."CAMPAIGN_NAME" ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}."SOURCE" ;;
  }

  measure: spend {
    type: sum
    sql: ${TABLE}."SPEND" ;;
  }

  measure: impressions {
    type: sum
    sql: ${TABLE}."IMPRESSIONS" ;;
  }

  measure: revenue {
    type:  sum
    sql:  ${TABLE}."REVENUE" ;;
  }

  measure: clicks {
    type: sum
    sql: ${TABLE}."CLICKS" ;;
  }

  measure: orders {
    type: sum
    sql: ${TABLE}."ORDERS" ;;
  }

  dimension: thirty_day {
    type: number
    sql: ${TABLE}."THIRTY_DAY" ;;
  }

  dimension: sixty_day {
    type: number
    sql: ${TABLE}."SIXTY_DAY" ;;
  }

  dimension: seven_day {
    type: number
    sql: ${TABLE}."SEVEN_DAY" ;;
  }

  dimension: one_day {
    type: number
    sql: ${TABLE}."ONE_DAY" ;;
  }

  set: detail {
    fields: [
      date,
      platform,
      campaign_name,
      source,
      spend,
      impressions,
      clicks,
      orders,
      thirty_day,
      sixty_day,
      seven_day,
      one_day
    ]
  }
}
