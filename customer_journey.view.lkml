view: customer_journey {
  derived_table: {
    sql: with x as (
        --Customer Journey
        select s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
            , case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
            , p.dollars
            , p.product
            , p.token
        from analytics.HEAP.sessions s
        left join (select a.user_id, session_id, b.checkout_token,split_part(a.path,'/',-2) as token
        , listagg(c.name,', ') within group (order by c.name) as Product
        , a.dollars
        from analytics.heap.purchase a
        left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."ORDER" b
        on split_part(a.path,'/',-2) = b.checkout_token
        left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."ORDER_LINE" c
        on b.id = c.order_id
        left join "ANALYTICS_STAGE"."SHOPIFY_US_FT"."PRODUCT_VARIANT" d
        on c.variant_id = d.id
        where checkout_token is not null
        group by a.user_id, session_id, b.checkout_token, split_part(path,'/',-2), a.dollars
        order by user_id) p
        on s.user_id = p.user_id
        and s.session_id = p.session_id
        where s.user_id in (select distinct user_id from analytics.HEAP.purchase)
        and (p.dollars > 0 or p.dollars is null)
      ),
      xcs as(
      select row_number() over (partition by user_id order by time) session_cnt
          , case when purchase_flag = 'PURCHASE' then row_number() over (partition by user_id, purchase_flag order by time) end purchase_session_cnt
          , x.*
      from x),
      xaa as(
      select xcs.*, case when purchase_flag = 'PURCHASE' and purchase_session_cnt = 1 then time else NULL end as purchase_time from xcs),
      xbb as(
      select xaa.*, b.first_purchase, c.num_purchases, d.pageviews from xaa
      left join (select distinct user_id, min(purchase_time) first_purchase from xaa
                group by user_id) b
      on xaa.user_id = b.user_id
      left join (select user_id, count(distinct purchase_session_cnt) num_purchases from xaa
                group by user_id) c
      on xaa.user_id = c.user_id
      left join (select session_id,user_id,count(time) as pageviews from analytics.heap.pageviews group by session_id, user_id) d
      on xaa.user_id = d.user_id and xaa.session_id = d.session_id
      )
      select * from xbb where time <= first_purchase
       ;;
  }

 dimension: count {
    type: number
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

  measure: user_id {
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

  dimension_group: purchase_time {
    type: time
    sql: ${TABLE}."PURCHASE_TIME" ;;
  }

  dimension_group: first_purchase {
    type: time
    sql: ${TABLE}."FIRST_PURCHASE" ;;
  }

  measure: num_purchases {
    type: sum
    sql: ${TABLE}."NUM_PURCHASES" ;;
  }

  measure: pageviews {
    type: sum
    sql: ${TABLE}."PAGEVIEWS" ;;
  }

  measure: sessions {
    type: count_distinct
    sql: ${TABLE}."SESSION_ID" ;;
  }
  dimension: product {
    type: string
    sql: ${TABLE}."PRODUCT" ;;
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
      purchase_time_time,
      first_purchase_time,
      num_purchases,
      pageviews,
      product
    ]
  }
}
