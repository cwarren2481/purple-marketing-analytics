connection: "analytics_warehouse"

# include all the views
include: "*.view"
include: "*.dashboard"

datagroup: marketing_analytics_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

explore: customer_journey {
  label: "Customer Journey"
  description: "Customery Journey"
}
explore: ROI {
  label: "ROI"
  description: "Daily adspend details, including channel, clicks, impressions, spend, device, platform, etc."
}
