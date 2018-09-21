view: customer_journey_path {
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
),
zza as(
select x.user_id, case when a.path = '/' then a.title else a.path end as path, a.time, a.landing_page, x.session_id, row_number() over (partition by a.user_id,a.session_id order by a.time) as event_number
from x
left join analytics.heap.pageviews a on x.user_id = a.user_id and x.session_id = a.session_id
where purchase_flag = 'PURCHASE'),
zzd as (
SELECT user_id
                ,session_id
                ,MAX(CASE
                                WHEN event_number = 1
                                        THEN landing_page
                                ELSE NULL
                                END) AS e1
                ,MAX(CASE
                                WHEN event_number = 2
                                        THEN path
                                ELSE NULL
                                END) AS e2
                ,MAX(CASE
                                WHEN event_number = 3
                                        THEN path
                                ELSE NULL
                                END) AS e3
                ,MAX(CASE
                                WHEN event_number = 4
                                        THEN path
                                ELSE NULL
                                END) AS e4
                ,MAX(CASE
                                WHEN event_number = 5
                                        THEN path
                                ELSE NULL
                    END) as e5
                    from zza
         group by user_id, session_id)
select count(session_id) as occurences, E1, E2, E3, E4, E5 from zzd
group by E1, E2, E3, E4, E5
order by occurences desc

       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: occurences {
    type: sum
    sql: ${TABLE}."OCCURENCES" ;;
  }

  dimension: e1 {
    type: string
    sql: ${TABLE}."E1" ;;
  }

  dimension: e2 {
    type: string
    sql: ${TABLE}."E2" ;;
  }

  dimension: e3 {
    type: string
    sql: ${TABLE}."E3" ;;
  }

  dimension: e4 {
    type: string
    sql: ${TABLE}."E4" ;;
  }

  dimension: e5 {
    type: string
    sql: ${TABLE}."E5" ;;
  }

  set: detail {
    fields: [
      occurences,
      e1,
      e2,
      e3,
      e4,
      e5
    ]
  }
}
