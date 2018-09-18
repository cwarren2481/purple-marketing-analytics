view: customer_journey {
  derived_table: {
    sql: with x as (
        --Customer Journey
        select s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
            , case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
            , p.dollars
        from analytics.HEAP.sessions s
        left join (select user_id, session_id, sum(dollars) dollars from analytics.HEAP.purchase group by user_id, session_id) p
        on s.user_id = p.user_id
        and s.session_id = p.session_id
        where s.user_id in (select distinct user_id from analytics.HEAP.purchase)
        and (p.dollars > 0 or p.dollars is null)
      )
      select row_number() over (partition by user_id order by time) session_cnt
          , case when purchase_flag = 'PURCHASE' then row_number() over (partition by user_id, purchase_flag order by time) end purchase_session_cnt
          , x.*
      from x
      order by user_id, time;
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: session_cnt {
    type: number
    sql: ${TABLE}."SESSION_CNT" ;;
  }

  dimension: purchase_session_cnt {
    type: number
    sql: ${TABLE}."PURCHASE_SESSION_CNT" ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}."USER_ID" ;;
  }

  dimension: session_id {
    type: number
    sql: ${TABLE}."SESSION_ID" ;;
  }

  dimension_group: time {
    type: time
    sql: ${TABLE}."TIME" ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}."REFERRER" ;;
  }

  dimension: landing_page {
    type: string
    sql: ${TABLE}."LANDING_PAGE" ;;
  }

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}."UTM_CAMPAIGN" ;;
  }

  dimension: purchase_flag {
    type: string
    sql: ${TABLE}."PURCHASE_FLAG" ;;
  }

  dimension: dollars {
    type: number
    sql: ${TABLE}."DOLLARS" ;;
  }

  measure: sessions {
    type: count_distinct
    sql: ${TABLE}."session_cnt" ;;
  }

  set: detail {
    fields: [
      session_cnt,
      purchase_session_cnt,
      user_id,
      session_id,
      time_time,
      referrer,
      landing_page,
      utm_campaign,
      purchase_flag,
      dollars,
      sessions
    ]
  }
}
