
WITH  __dbt__cte__stg_opportunity_request as (


WITH `opportunity_request` AS (
    SELECT DISTINCT
        `auction_requests`.`id` AS `request_id`,
        `opportunity`.`opportunity_id` AS `opportunity_id`,
        `opportunity`.`opportunity_current_status` AS `opportunity_stage_name`,
        `opportunity`.`opportunity_creation_datetime`,
        `opportunity`.`opportunity_close_date`,
        `opportunity`.`opportunity_log_payment_status_datetime`,
        `opportunity`.`opportunity_payment_status_datetime`,
        `opportunity`.`opportunity_contract_status_datetime`,
        `opportunity`.`opportunity_customer_handover_status_datetime`,
        `opportunity`.`opportunity_sold_status_datetime`,
        `opportunity`.`opportunity_returned_status_datetime`,
        `opportunity`.`is_sold` AS `opportunity_is_sold`,
        `opportunity`.`is_returned` AS `opportunity_is_returned`,
        `opportunity`.`is_tbi` AS `opportunity_is_tbi`,
        `opportunity`.`is_tbi_in_progress` AS `opportunity_is_tbi_in_progress`,
        `opportunity`.`is_delivered` AS `opportunity_is_delivered`,
        ROW_NUMBER()
            OVER (
                PARTITION BY `opportunity`.`opportunity_id`
                ORDER BY
                    DATE_DIFF(
                        `opportunity`.`opportunity_creation_datetime`,
                        `auction_requests`.`created_at`,
                        SECOND
                    ) ASC
            )
            AS `recency_rank`
    FROM
        `pricing-338819`.`ajans_db`.`auction_requests` AS `auction_requests`
    INNER JOIN
        `pricing-338819`.`ajans_db`.`users` AS `users`
        ON
            `auction_requests`.`dealer_id` = `users`.`id`
    INNER JOIN
        `pricing-338819`.`ajans_db`.`vehicle_auctions` AS `vehicle_auctions`
        ON
            `auction_requests`.`auction_id` = `vehicle_auctions`.`id`
    INNER JOIN
        `pricing-338819`.`ajans_db`.`vehicles` AS `vehicles`
        ON
            `vehicle_auctions`.`vehicle_id` = `vehicles`.`id`
    INNER JOIN
        `pricing-338819`.`salesforce`.`Car__c` AS `car`
        ON
            `vehicles`.`sf_vehicle_id` = `car`.`id`
    INNER JOIN
        `pricing-338819`.`reporting`.`wholesale_selling_opportunity` AS `opportunity`
        ON
            `users`.`sf_dealer_id` = `opportunity`.`account_id`
            AND `car`.`Id` = `opportunity`.`car_id`
    WHERE
        DATE_DIFF(
            `opportunity`.`opportunity_creation_datetime`,
            `auction_requests`.`created_at`,
            SECOND
        )
        >= 0
        AND `auction_requests`.`type` = 'Buy Now'
)

SELECT *
FROM `opportunity_request`
WHERE `recency_rank` = 1
), `dates` AS (
    SELECT `date_array` AS `date_key`
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY("2022-10-01", CURRENT_DATE(), INTERVAL 1 DAY)
        ) AS `date_array`
    
        WHERE `date_array` >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
),

`all_sessions` AS (
    SELECT
        `aj_sessions`.`event_date` AS `date_key`,
        COUNT(DISTINCT `aj_sessions`.`mp_session_id`) AS `all_sessions`
    FROM `mixpanel_processed.ajans_sessions` AS `aj_sessions`
    
        WHERE
            DATE(`aj_sessions`.`event_date`)
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_sessions`.`event_date`
),

`vehicle_requests` AS (
    SELECT
        `aj_vehicle`.`id` AS `vehicle_id`,
        `aj_vehicle`.`sf_vehicle_id`,
        `d`.`date_key`,
        COUNT(DISTINCT `ar`.`id`) AS `opened_requests`,
        COUNT(
            DISTINCT CASE
                WHEN `arl`.`auction_request_status` = "Succeeded" THEN `ar`.`id`
            END
        ) AS `successful_requests`,
        COUNT(DISTINCT `ro`.`opportunity_id`) AS `opened_salesforce_opportunities`,
        COUNT(
            DISTINCT CASE
                WHEN `ro`.`opportunity_stage_name` = "Sold" THEN `ro`.`opportunity_id`
            END
        ) AS `successful_salesforce_opportunities`
    FROM `pricing-338819`.`ajans_db`.`vehicles` AS `aj_vehicle`
    CROSS JOIN `dates` AS `d`
    LEFT JOIN
        `pricing-338819`.`ajans_db`.`vehicle_auctions` AS `vehicle_auction`
        ON
            `aj_vehicle`.`id` = `vehicle_auction`.`vehicle_id`
            AND `d`.`date_key` = DATE(`vehicle_auction`.`created_at`)
    LEFT JOIN
        `pricing-338819`.`ajans_db`.`auction_requests` AS `ar`
        ON
            `vehicle_auction`.`id` = `ar`.`auction_id`
            AND DATE(`ar`.`created_at`) = `d`.`date_key`
    LEFT JOIN
        `pricing-338819`.`ajans_db`.`auction_request_logs` AS `arl`
        ON `ar`.`id` = `arl`.`auction_request_id`
    LEFT JOIN
        __dbt__cte__stg_opportunity_request AS `ro`
        ON `ar`.`id` = `ro`.`request_id`
    WHERE
        `d`.`date_key` >= DATE(`aj_vehicle`.`created_at`)

    GROUP BY `aj_vehicle`.`id`, `aj_vehicle`.`sf_vehicle_id`, `d`.`date_key`
    HAVING `opened_requests` > 0
),

`vehicle_activity` AS (
    SELECT
        `aj_vehicle`.`id` AS `vehicle_id`,
        `d`.`date_key`,
        COUNT(DISTINCT `aj_screen_auction_profile`.`mp_event_id`)
            AS `car_profile_events`,
        COUNT(DISTINCT `aj_action_interested`.`mp_event_id`) AS `interested_events`,
        COUNT(DISTINCT `aj_action_buy_now`.`mp_event_id`) AS `buy_now_events`,
        COUNT(DISTINCT `aj_screen_buy_now_confirmation_popup`.`mp_event_id`)
            AS `buy_now_confirmation_events`,
        COUNT(DISTINCT `aj_action_showroom_request`.`event_id`) AS `showroom_events`
    FROM `pricing-338819`.`ajans_db`.`vehicles` AS `aj_vehicle`
    CROSS JOIN `dates` AS `d`
    LEFT JOIN
        `pricing-338819`.`mixpanel_processed`.`ajans_screen_auction_profile`
            AS `aj_screen_auction_profile`
        ON
            `aj_vehicle`.`id` = `aj_screen_auction_profile`.`vehicle_id`
            AND `d`.`date_key` = `aj_screen_auction_profile`.`event_date`
    LEFT JOIN
        `pricing-338819`.`mixpanel_processed`.`ajans_action_interested`
            AS `aj_action_interested`
        ON
            `aj_vehicle`.`id` = `aj_action_interested`.`vehicle_id`
            AND `d`.`date_key` = `aj_action_interested`.`event_date`
    LEFT JOIN
        `pricing-338819`.`mixpanel_processed`.`ajans_action_buy_now`
            AS `aj_action_buy_now`
        ON
            `aj_vehicle`.`id` = `aj_action_buy_now`.`vehicle_id`
            AND `d`.`date_key` = `aj_action_buy_now`.`event_date`
    LEFT JOIN
        `pricing-338819`.`mixpanel_processed`.`ajans_screen_buy_now_confirmation_popup`
            AS `aj_screen_buy_now_confirmation_popup`
        ON
            `aj_vehicle`.`id` = `aj_screen_buy_now_confirmation_popup`.`vehicle_id`
            AND `d`.`date_key` = `aj_screen_buy_now_confirmation_popup`.`event_date`
    LEFT JOIN
        `pricing-338819`.`mixpanel_processed`.`ajans_action_showroom_request`
            AS `aj_action_showroom_request`
        ON
            `aj_vehicle`.`id` = `aj_action_showroom_request`.`vehicle_id`
            AND `d`.`date_key` = `aj_action_showroom_request`.`event_date`

    WHERE
        `d`.`date_key` >= DATE(`aj_vehicle`.`created_at`)

    GROUP BY `aj_vehicle`.`id`, `d`.`date_key`
    HAVING `car_profile_events` > 0
)

SELECT
    `d`.`date_key` AS `event_date`,
    `aj_vehicle`.`id`,
    `aj_vehicle`.`make`,
    `aj_vehicle`.`model`,
    `aj_vehicle`.`year`,
    `aj_vehicle`.`kilometrage_type`,
    `aj_vehicle`.`kilometrage`,
    `aj_vehicle`.`transmission`,
    `aj_vehicle`.`images`,
    `aj_vehicle`.`status`,
    `aj_vehicle`.`has_valid_license`,
    `aj_vehicle`.`license_valid_until`,
    `aj_vehicle`.`count_previous_owners`,
    `aj_vehicle`.`body_type`,
    `aj_vehicle`.`created_at`,
    `aj_vehicle`.`updated_at`,
    `aj_vehicle`.`sf_vehicle_id`,
    `aj_vehicle`.`sf_vehicle_name`,
    `aj_vehicle`.`class`,
    `aj_vehicle`.`ownership_type`,
    `aj_vehicle`.`engine_type`,
    `aj_vehicle`.`fuel_type`,
    `aj_vehicle`.`traffic_unit`,
    `aj_vehicle`.`engine_displacement`,
    `aj_vehicle`.`number_of_cylinders`,
    `aj_vehicle`.`number_of_keys`,
    `aj_vehicle`.`service_history_report`,
    `aj_vehicle`.`maintenance_invoices`,
    `aj_vehicle`.`summary`,
    `sf_vehicle`.`ajans_is_published`,
    `sf_vehicle`.`ajans_published_at`,
    `sf_vehicle`.`ajans_unpublished_at`,
    `sf_vehicle`.`car_category`,
    COALESCE(`sf_vehicle`.`last_allocation_at`, `sf_vehicle`.`acquisition_datetime`)
        AS `last_allocation_at`,
    `sf_vehicle`.`last_allocation_from`,
    COALESCE(`sf_vehicle`.`allocation_category`, `sf_vehicle`.`last_allocation_to`)
        AS `last_allocation_to`,
    `sf_vehicle`.`sold_at`,
    `sf_vehicle`.`acquisition_datetime` AS `acquired_at`,
    `sf_vehicle`.`is_sold`,
    `sf_vehicle`.`days_on_hand`,
    `sf_vehicle`.`current_status`,
    COALESCE(`all_sessions`.`all_sessions`, 0) AS `all_sessions`,
    COALESCE(`vehicle_activity`.`car_profile_events`, 0) AS `car_profile_events`,
    COALESCE(`vehicle_activity`.`interested_events`, 0) AS `interested_events`,
    COALESCE(`vehicle_activity`.`buy_now_events`, 0) AS `buy_now_events`,
    COALESCE(`vehicle_activity`.`buy_now_confirmation_events`, 0)
        AS `buy_now_confirmation_events`,
    COALESCE(`vehicle_activity`.`showroom_events`, 0) AS `showroom_events`,
    COALESCE(`vehicle_requests`.`opened_requests`, 0) AS `opened_requests`,
    COALESCE(`vehicle_requests`.`successful_requests`, 0) AS `successful_requests`,
    COALESCE(`vehicle_requests`.`opened_salesforce_opportunities`, 0)
        AS `opened_salesforce_opportunities`,
    COALESCE(`vehicle_requests`.`successful_salesforce_opportunities`, 0)
        AS `successful_salesforce_opportunities`
FROM `pricing-338819`.`ajans_db`.`vehicles` AS `aj_vehicle`
LEFT JOIN `pricing-338819`.`reporting`.`vehicle_acquisition_to_selling` AS `sf_vehicle`
    ON `aj_vehicle`.`sf_vehicle_id` = `sf_vehicle`.`car_id`
CROSS JOIN `dates` AS `d`
LEFT JOIN `vehicle_activity` AS `vehicle_activity`
    ON
        `aj_vehicle`.`id` = `vehicle_activity`.`vehicle_id`
        AND `d`.`date_key` = `vehicle_activity`.`date_key`
LEFT JOIN `vehicle_requests` AS `vehicle_requests`
    ON
        `aj_vehicle`.`id` = `vehicle_requests`.`vehicle_id`
        AND `d`.`date_key` = `vehicle_activity`.`date_key`
LEFT JOIN `all_sessions`
    ON `d`.`date_key` = `all_sessions`.`date_key`
WHERE
    `d`.`date_key` >= DATE(`aj_vehicle`.`created_at`)
    AND (
        `vehicle_activity`.`car_profile_events` > 0
        OR `vehicle_requests`.`opened_requests` > 0
    )