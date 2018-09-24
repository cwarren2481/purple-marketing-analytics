connection: "analytics_warehouse"

# include all the views
include: "*.view"

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
explore: customer_journey_referrer {
  label: "Referral"
  description: "Breakdown of the customer journey by Referring URL"
}
explore: customer_journey_landing_page {
  label: "Landing Page"
  description: "Breakdown of the customer journey by Landing Page"
}
explore: customer_journey_path {
  label: "Path Analysis"
  description: "What are the most common customer journeys to a purchase?"
}
explore: customer_journey_repeat_purchasers{
  label: "Repeat Purchase Analysis"
  description: "Repeat Purchases"
}
