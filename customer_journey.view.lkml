view: customer_journey {
  derived_table: {
    sql: select s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
          , case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
          , p.dollars
      from analytics.HEAP.sessions s
      left join (select user_id, session_id, sum(dollars) dollars from analytics.HEAP.purchase group by user_id, session_id) p
      on s.user_id = p.user_id
      and s.session_id = p.session_id
      where s.user_id in (select distinct user_id from analytics.HEAP.purchase)
      and (p.dollars > 0 or p.dollars is null)
      group by s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
      --and purchase_flag = 'PURCHASE'
      --and s.user_id = 4702333951502204
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  measure: dollars {
    type: number
    sql: ${TABLE}."DOLLARS" ;;
  }


  set: detail {
    fields: [
      user_id,
      session_id,
      time_time,
      referrer,
      landing_page,
      utm_campaign,
      purchase_flag,
      dollars
    ]
  }
}
