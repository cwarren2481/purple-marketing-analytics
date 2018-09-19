view: customer_journey_landing_page {
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
        and time between '2018-01-01' and CURRENT_DATE
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
      ),
      xff as(
      select count(distinct session_cnt) num_sessions, avg(pageviews) avg_views_sess,user_id, landing_page, referrer, time from xbb
      where time <= first_purchase
      group by landing_page, referrer, user_id, time)

      select avg(num_sessions) avg_num_sessions,avg(avg_views_sess) avg_views_per_session, a.total_sessions,  xff.referrer from xff
      left join (select count(session_id) total_sessions, referrer from x group by referrer) a
      on xff.referrer = a.referrer
      where xff.referrer not like '%purple.com%'
      group by xff.referrer, a.total_sessions
      order by total_sessions desc
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: avg_num_sessions {
    type: number
    sql: ${TABLE}."AVG_NUM_SESSIONS" ;;
  }

  measure: avg_views_per_session {
    type: number
    sql: ${TABLE}."AVG_VIEWS_PER_SESSION" ;;
  }

  measure: total_sessions {
    type: number
    sql: ${TABLE}."TOTAL_SESSIONS" ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}."REFERRER" ;;
  }

  set: detail {
    fields: [avg_num_sessions, avg_views_per_session, total_sessions, referrer]
  }
}
