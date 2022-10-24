-- Step 1: Declare the input variables
DECLARE entity_id_var ARRAY <STRING>;
DECLARE start_date DATE;
DECLARE end_date DATE;
SET entity_id_var = ['FP_TW', 'FP_HK', 'FP_SG', 'FP_MY', 'FP_TH', 'FP_PH', 'FP_BD', 'FP_PK'];
SET start_date = DATE('2022-06-01');
SET end_date = DATE('2022-08-31');

-- Step 2: Extract the polygon shapes of the experiment's target zones
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.geo_data_subscription_program` AS
SELECT 
    p.entity_id,
    co.country_code,
    ci.name AS city_name,
    ci.id AS city_id,
    zo.shape AS zone_shape, 
    zo.name AS zone_name,
    zo.id AS zone_id,
FROM `fulfillment-dwh-production.cl.countries` co
LEFT JOIN UNNEST(co.platforms) p
LEFT JOIN UNNEST(co.cities) ci
LEFT JOIN UNNEST(ci.zones) zo
WHERE TRUE
    AND p.entity_id IN UNNEST(entity_id_var)
    AND zo.is_active -- Active city
    AND ci.is_active; -- Active zone

###----------------------------------------------------------END OF STEP 2----------------------------------------------------------###

-- Step 3: Pull the business KPIs from dps_sessions_mapped_to_orders_v2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_subscription_program` AS
WITH entities AS (
    SELECT
        ent.region,
        p.entity_id,
        ent.country_iso,
        ent.country_name,
FROM `fulfillment-dwh-production.cl.entities` ent
LEFT JOIN UNNEST(platforms) p
INNER JOIN (SELECT DISTINCT entity_id FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`) dps ON p.entity_id = dps.entity_id 
WHERE TRUE
    AND p.entity_id NOT LIKE 'ODR%' -- Eliminate entities starting with DN_ as they are not part of DPS
    AND p.entity_id NOT LIKE 'DN_%' -- Eliminate entities starting with ODR (on-demand riders)
    AND p.entity_id NOT IN ('FP_DE', 'FP_JP') -- Eliminate JP and DE because they are not DH markets any more
    AND p.entity_id != 'TB_SA' -- Eliminate this incorrect entity_id for Saudi
    AND p.entity_id != 'HS_BH' -- Eliminate this incorrect entity_id for Bahrain
)

SELECT 
    -- Identifiers and supplementary fields     
    -- Date and time
    a.created_date AS created_date_utc,
    
    -- Location of order
    ent.region,
    a.entity_id,
    a.country_code,
    a.city_name,
    a.city_id,
    a.zone_name,
    a.zone_id,
    zn.zone_shape,
    ST_GEOGPOINT(dwh.delivery_location.longitude, dwh.delivery_location.latitude) AS customer_location,

    -- Order/customer identifiers and session data
    a.variant,
    a.experiment_id AS test_id,
    a.perseus_client_id,
    a.ga_session_id,
    a.dps_sessionid,
    a.dps_customer_tag,
    a.customer_total_orders,
    a.customer_first_order_date,
    DATE_DIFF(a.order_placed_at, a.customer_first_order_date, DAY) AS days_since_first_order,
    a.order_id,
    a.platform_order_code,
    a.scheme_id,
    a.vendor_price_scheme_type,	-- The assignment type of the scheme to the vendor during the time of the order, such as 'Automatic', 'Manual', 'Campaign', and 'Country Fallback'.
    
    -- Vendor data and information on the delivery
    a.vendor_id,
    a.chain_id,
    a.chain_name,
    a.vertical_type,
    CASE 
      WHEN a.vendor_vertical_parent IS NULL THEN NULL 
      WHEN LOWER(a.vendor_vertical_parent) IN ('restaurant', 'restaurants') THEN 'restaurant'
      WHEN LOWER(a.vendor_vertical_parent) = 'shop' THEN 'shop'
      WHEN LOWER(a.vendor_vertical_parent) = 'darkstores' THEN 'darkstores'
    END AS vendor_vertical_parent,
    a.delivery_status,
    a.is_own_delivery,
    a.exchange_rate,

    -- Business KPIs in local currency
    a.dps_surge_fee_local,
    a.dps_travel_time_fee_local,
    a.dps_delivery_fee_local,
    a.delivery_fee_local,
    pd.delivery_fee_vat_rate / 100 AS delivery_fee_vat_rate_pd, -- If you are dealing with a country like TW where VAT is included in the DF, you need to divide by 1 + delivery_fee_vat_rate to get to the actual DF revenue
    a.dps_delivery_fee_local / (1 + pd.delivery_fee_vat_rate / 100) AS dps_delivery_fee_local_before_vat,
    a.dps_last_non_zero_df_local,
    a.dps_standard_fee_local,
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local END)
    END AS actual_df_paid_by_customer_local,
    a.gfv_local,
    a.gmv_local,
    customer_paid_local,
    a.commission_local,
    a.joker_vendor_fee_local,
    COALESCE(a.service_fee_local, 0) AS service_fee_local,
    dwh.value.mov_customer_fee_local AS sof_local_cdwh,
    IF(a.gfv_local - a.dps_minimum_order_value_local >= 0, 0, COALESCE(dwh.value.mov_customer_fee_local, (a.dps_minimum_order_value_local - a.gfv_local))) AS sof_local,
    a.delivery_costs_local,

    -- Special fields
    a.is_delivery_fee_covered_by_discount, -- Needed in the profit formula
    a.is_delivery_fee_covered_by_voucher, -- Needed in the profit formula
    CASE WHEN a.is_delivery_fee_covered_by_discount = FALSE AND a.is_delivery_fee_covered_by_voucher = FALSE THEN 'No DF Voucher' ELSE 'DF Voucher' END AS df_voucher_flag,
    COALESCE(pdos_ap.is_free_delivery_subscription_order, pdos_eu.is_free_delivery_subscription_order) AS is_free_delivery_subscription_order,
    a.joker_customer_discount_local,
    a.discount_dh_local,			
    a.discount_other_local,			
    a.voucher_dh_local,	
    a.voucher_other_local,

    ###-----------------------------------------------------------SEPARATOR BETWEEN LOCAL CURRENCY AND EUR-----------------------------------------------------------###

    -- Business KPIs in EUR
    a.dps_surge_fee_local / a.exchange_rate AS dps_surge_fee_eur,
    a.dps_travel_time_fee_local / a.exchange_rate AS dps_travel_time_fee_eur,
    a.dps_delivery_fee_local / a.exchange_rate AS dps_delivery_fee_eur,
    a.delivery_fee_local / a.exchange_rate AS delivery_fee_eur,
    (a.dps_delivery_fee_local / a.exchange_rate) / (1 + pd.delivery_fee_vat_rate / 100) AS dps_delivery_fee_eur_before_vat,
    a.dps_last_non_zero_df_local / a.exchange_rate AS dps_last_non_zero_df_eur,
    a.dps_standard_fee_local / a.exchange_rate AS dps_standard_fee_eur,
    CASE
        WHEN ent.region IN ('Europe', 'Asia') THEN COALESCE( -- Get the delivery fee data of Pandora countries from Pandata tables
            pd.delivery_fee_local / a.exchange_rate, 
            -- In 99 pct of cases, we won't need to use that fallback logic as pd.delivery_fee_local is reliable
            IF(a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE, 0, a.delivery_fee_local / a.exchange_rate)
        )
        -- If the order comes from a non-Pandora country, use delivery_fee_local
        WHEN ent.region NOT IN ('Europe', 'Asia') THEN (CASE WHEN a.is_delivery_fee_covered_by_discount = TRUE OR a.is_delivery_fee_covered_by_voucher = TRUE THEN 0 ELSE a.delivery_fee_local / a.exchange_rate END)
    END AS actual_df_paid_by_customer_eur,
    a.gfv_local / a.exchange_rate AS gfv_eur,
    a.gmv_local / a.exchange_rate AS gmv_eur,
    customer_paid_local / a.exchange_rate AS customer_paid_eur,
    a.commission_local / a.exchange_rate AS commission_eur,
    a.joker_vendor_fee_local / a.exchange_rate AS joker_vendor_fee_eur,
    COALESCE(a.service_fee_local / a.exchange_rate, 0) AS service_fee_eur,
    dwh.value.mov_customer_fee_local / a.exchange_rate AS sof_eur_cdwh,
    IF(
      a.gfv_local / a.exchange_rate - a.dps_minimum_order_value_local / a.exchange_rate >= 0, 
      0, 
      COALESCE(dwh.value.mov_customer_fee_local / a.exchange_rate, (a.dps_minimum_order_value_local / a.exchange_rate - a.gfv_local / a.exchange_rate))
    ) AS sof_eur,
    a.delivery_costs_local / a.exchange_rate AS delivery_costs_eur,

    -- Special fields
    a.joker_customer_discount_local / a.exchange_rate AS joker_customer_discount_eur,
    a.discount_dh_local / a.exchange_rate AS discount_dh_eur,			
    a.discount_other_local / a.exchange_rate AS discount_other_eur,			
    a.voucher_dh_local / a.exchange_rate AS voucher_dh_eur,	
    a.voucher_other_local / a.exchange_rate AS voucher_other_eur,
FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` a
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh 
  ON TRUE 
    AND a.entity_id = dwh.global_entity_id
    AND a.platform_order_code = dwh.order_id -- There is no country_code field in this table
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` pd -- Contains info on the orders in Pandora countries
  ON TRUE 
    AND a.entity_id = pd.global_entity_id
    AND a.platform_order_code = pd.code 
    AND a.created_date = pd.created_date_utc -- There is no country_code field in this table
-- LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_sb_subscriptions` pdos ON pd.uuid = pdos.uuid AND pd.created_date_utc = pdos.created_date_utc
LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_sb_subscriptions` pdos_ap ON pd.uuid = pdos_ap.uuid AND pd.created_date_utc = pdos_ap.created_date_utc
LEFT JOIN `fulfillment-dwh-production.pandata_report.regional_eu__pd_orders_agg_sb_subscriptions` pdos_eu ON pd.uuid = pdos_eu.uuid AND pd.created_date_utc = pdos_eu.created_date_utc
LEFT JOIN `dh-logistics-product-ops.pricing.geo_data_subscription_program` zn 
  ON TRUE 
    AND a.entity_id = zn.entity_id 
    AND a.country_code = zn.country_code
    AND a.zone_id = zn.zone_id 
INNER JOIN entities ent ON a.entity_id = ent.entity_id -- Get the region associated with every entity_id
WHERE TRUE
  AND a.entity_id IN UNNEST(entity_id_var)
  AND a.created_date BETWEEN start_date AND end_date; -- Filter for tests that started from July 19th, 2022 (date of the first switchback test)

###----------------------------------------------------------END OF STEP 3----------------------------------------------------------###

-- Step 4.1: Add two columns showing the avg actual_df_paid_by_customer_local/eur partitioned by entity_id and zone. These two columns will be used in step 4.2
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_subscription_program` AS
SELECT 
  *,
  -- P.S. We add the "actual_df_paid_by_customer_local" IS NOT NULL condition because there are rows where "actual_df_paid_by_customer_local" IS NULL but platform_order_code exists, which means that the denom may be inflated
  SUM(CASE WHEN is_fd_subscription = 'N' AND actual_df_paid_by_customer_local IS NOT NULL THEN actual_df_paid_by_customer_local ELSE NULL END) OVER (PARTITION BY entity_id, zone_name) 
  / COUNT(DISTINCT CASE WHEN is_fd_subscription = 'N' AND actual_df_paid_by_customer_local IS NOT NULL THEN platform_order_code ELSE NULL END) OVER (PARTITION BY entity_id, zone_name) AS avg_actual_df_paid_local_impute,
  
  SUM(CASE WHEN is_fd_subscription = 'N' AND actual_df_paid_by_customer_eur IS NOT NULL THEN actual_df_paid_by_customer_eur ELSE NULL END) OVER (PARTITION BY entity_id, zone_name) 
  / COUNT(DISTINCT CASE WHEN is_fd_subscription = 'N' AND actual_df_paid_by_customer_eur IS NOT NULL THEN platform_order_code ELSE NULL END) OVER (PARTITION BY entity_id, zone_name) AS avg_actual_df_paid_eur_impute
FROM `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_subscription_program`;

-- Step 4.2: Impute the missing values in "dps_delivery_fee_local_before_vat" and "dps_delivery_fee_eur_before_vat" using the avg "actual_df_paid_by_customer_local/eur" for non-subscription orders as a proxy
UPDATE `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_subscription_program`
SET 
  dps_delivery_fee_local_before_vat = avg_actual_df_paid_local_impute,
  dps_delivery_fee_eur_before_vat = avg_actual_df_paid_eur_impute
WHERE dps_delivery_fee_local IS NULL;

###----------------------------------------------------------END OF STEP 4----------------------------------------------------------###

-- Step 5: We did not add the subscription flag, profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_augmented_subscription_program` AS
SELECT
  a.*,
  -- Subscription flag
  CASE WHEN is_free_delivery_subscription_order = TRUE THEN 'Y' ELSE 'N' END AS is_fd_subscription, -- Only two possible values --> True or False

  -- Revenue and profit formulas
  actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) AS revenue_local,
  
  CASE -- IF is_fd_subscription = 'N', then use actual_df_paid_by_customer_local and NOT dps_delivery_fee_local because the customer paid a DF for these orders and we know its value
    WHEN is_fd_subscription = 'N' THEN actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local)
    ELSE dps_delivery_fee_local_before_vat + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) 
  END AS revenue_local_cf, -- "cf" refers to counterfactual
  
  actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local AS gross_profit_local,
  
  CASE -- IF is_fd_subscription = 'N', then use actual_df_paid_by_customer_local and NOT dps_delivery_fee_local because the customer paid a DF for these orders and we know its value
    WHEN is_fd_subscription = 'N' THEN actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local
    ELSE dps_delivery_fee_local_before_vat + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local
  END AS gross_profit_local_cf,

  ###-----------------------------------------------------------SEPARATOR BETWEEN LOCAL CURRENCY AND EUR-----------------------------------------------------------###

  (actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local)) / exchange_rate AS revenue_eur,
  
  CASE
    WHEN is_fd_subscription = 'N' THEN (actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local)) / exchange_rate
    ELSE (dps_delivery_fee_local_before_vat + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local)) / exchange_rate
  END AS revenue_eur_cf,

  (actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local) / exchange_rate AS gross_profit_eur,
  
  CASE
    WHEN is_fd_subscription = 'N' THEN (
      actual_df_paid_by_customer_local / (1 + delivery_fee_vat_rate_pd) + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local
    ) / exchange_rate 
    ELSE (dps_delivery_fee_local_before_vat + commission_local + joker_vendor_fee_local + service_fee_local + COALESCE(sof_local_cdwh, sof_local) - delivery_costs_local) / exchange_rate 
  END AS gross_profit_eur_cf,
FROM `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_subscription_program` a;

###----------------------------------------------------------END OF STEP 5----------------------------------------------------------###

-- Step 6: We did not add the profit metrics and the parent_vertical filter to the previous query because some of the fields used below had to be computed first
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_selection_agg_metrics_subscription_program` AS
SELECT 
  region,
  entity_id,
  vertical_type,
  vendor_id,
  is_fd_subscription,
  COUNT(DISTINCT platform_order_code) AS order_count,
  SUM(gmv_eur) AS gmv_eur,
  SUM(gross_profit_eur) AS gross_profit_eur,
  SUM(gross_profit_eur_cf) AS gross_profit_eur_cf,
FROM `dh-logistics-product-ops.pricing.vendor_selection_individual_orders_augmented_subscription_program`
WHERE gross_profit_eur IS NOT NULL -- Eliminate records where gross_profit_eur = NULL (~ 2% of the data)
GROUP BY 1,2,3,4,5;