view: customer_journey_repeat_purchasers_ref {
  derived_table: {
    sql: with x as (
        --Customer Journey
        select s.user_id, s.session_id, s.time, s.referrer, s.landing_page, s.utm_campaign
            , row_number() over(partition by s.user_id order by time) as session_number
            , case when p.user_id is not null then 'PURCHASE' else 'NON-PURCHASE' end purchase_flag
            , p.dollars
        from analytics.HEAP.sessions s
        left join (select user_id, session_id, sum(dollars) dollars from analytics.HEAP.purchase group by user_id, session_id) p
        on s.user_id = p.user_id
        and s.session_id = p.session_id
        where s.user_id in (select distinct user_id from analytics.HEAP.purchase)
        and (p.dollars > 0 or p.dollars is null)
        and time between '2018-01-01' and CURRENT_DATE)

      , xaa as (
          -- count purchases
      select case when purchase_flag = 'PURCHASE' then row_number() over (partition by user_id, purchase_flag order by time) end purchase_session_cnt
          , x.*
      from x
      where purchase_flag = 'PURCHASE')

      , xbb as (
          -- number of purchases
      select xaa.*
        , z.num_purchases
      from xaa
      left join (select user_id, count(distinct purchase_session_cnt) num_purchases from xaa group by user_id) z
      on xaa.user_id = z.user_id)

      , xcc as (
          -- count repeat_purchase by landing_page
      select user_id
        , count(case when num_purchases > 1 then landing_page end) repeat_purchase
        , landing_page
        , referrer
      from xbb
      where num_purchases > 1
      group by landing_page, referrer, user_id)

          --avg repeat_purchase with total_purchases by landing_page
      select avg(repeat_purchase) avg_repeat_purchase, a.total_purchases, xcc.referrer
      from xcc
      left join (select count(purchase_flag) total_purchases , referrer from x group by referrer, purchase_flag
                 having purchase_flag = 'PURCHASE') a
      on a.referrer = xcc.referrer
      left join (select referrer, user_id from x where session_number = 1) as b
      on xcc.user_id = b.user_id
      group by xcc.referrer, a.total_purchases
      having avg_repeat_purchase > 0
      order by 2 desc;
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: avg_repeat_purchase {
    type: sum
    sql: ${TABLE}."AVG_REPEAT_PURCHASE" ;;
  }

  measure: total_purchases {
    type: number
    sql: ${TABLE}."TOTAL_PURCHASES" ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}."REFERRER" ;;
  }

  set: detail {
    fields: [avg_repeat_purchase, total_purchases, referrer]
  }
}
