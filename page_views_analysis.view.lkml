view: page_views_analysis {
  derived_table: {
    sql: select * from (
      select count(path) page_count, path from analytics.heap.pageviews
        where path not like '%checkouts%' and path not like '%orders%' group by path order by count(path) desc)a
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: page_count {
    type: sum
    sql: ${TABLE}."PAGE_COUNT" ;;
  }

  dimension: path {
    type: string
    sql: ${TABLE}."PATH" ;;
  }

  set: detail {
    fields: [page_count, path]
  }
}
