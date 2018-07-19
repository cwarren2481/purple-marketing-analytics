view: customer_journey_1 {
  derived_table: {
    sql: select a.event_id, a.user_id, a.session_id, a.device_type,a.referrer, a.utm_source,a.utm_campaign,a.utm_medium, a.path, b.session_time as order_date, c.EVENT_TABLE_NAME as EVENT_NAME, b.region, b.dollars from HEAP.PAGEVIEWS a
      left join HEAP.PURCHASE b on a.USER_ID = b.USER_ID
      left join HEAP.ALL_EVENTS c on a.EVENT_ID = c.EVENT_ID
      where a.time <= b.session_time
      and a.time >= '2018-01-01'
      order by USER_ID,a.session_time desc;
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
