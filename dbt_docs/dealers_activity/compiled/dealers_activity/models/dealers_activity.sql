

WITH  __dbt__cte__stg_dealers as (


WITH `dealer_first_visit` AS (
    SELECT
        `users`.`id` AS `dealer_id`,
        MIN(`ajans_sessions`.`event_date`) AS `dealer_first_app_visit`
    FROM `pricing-338819`.`ajans_db`.`users` AS `users`
    INNER JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `ajans_mp_users`
        ON
            LTRIM(
        REGEXP_REPLACE(users.phone, r'\+2|[^0-9|,]', ''),
        '0'
    )
            = LTRIM(
        REGEXP_REPLACE(ajans_mp_users.mobile_number, r'\+2|[^0-9|,]', ''),
        '0'
    )
    INNER JOIN
        `pricing-338819`.`mixpanel_processed`.`ajans_sessions` AS `ajans_sessions`
        ON `ajans_sessions`.`mp_user_id` IN UNNEST(`ajans_mp_users`.`mp_user_id`)
    GROUP BY `users`.`id`
),

`dealer_first_transaction` AS (
    SELECT
        `users`.`id` AS `dealer_id`,
        MIN(CASE
            WHEN `opportunity`.`opportunity_current_status` IN (
                "Documents Review", "Customer Handover", "Sold"
            ) THEN `opportunity`.`opportunity_contract_status_datetime`
        END)
            AS `dealer_first_trasaction`
    FROM `pricing-338819`.`ajans_db`.`users` AS `users`
    LEFT JOIN
        `pricing-338819`.`reporting`.`wholesale_selling_opportunity` AS `opportunity`
        ON `users`.`sf_dealer_id` = `opportunity`.`account_id`
    WHERE `opportunity`.`opportunity_contract_status_datetime` IS NOT NULL
    GROUP BY `users`.`id`
),

`dealer_activation` AS (
    SELECT
        `users`.`id` AS `dealer_id`,
        MIN(`register_applications`.`created_at`) AS `activated_at`
    FROM `pricing-338819`.`ajans_db`.`register_applications` AS `register_applications`
    INNER JOIN `pricing-338819`.`ajans_db`.`users` AS `users` USING (`phone`)
    WHERE `register_applications`.`status` = "activated"
    GROUP BY `users`.`id`
)

SELECT
    `users`.`id` AS `dealer_id`,
    `users`.`sf_dealer_id` AS `sf_dealer_id`,
    `users`.`name` AS `dealer_name`,
    `users`.`phone` AS `dealer_phone`,
    `users`.`created_at` AS `dealer_created_at`,
    `users`.`email` AS `dealer_email`,
    `sf_account`.`Area__c` AS `area`,
    `sf_account`.`City__c` AS `city`,
    `users`.`first_bid` AS `dealer_first_bid`,
    `dealers_contacts`.`dealer_type`,
    `dealer_first_visit`.`dealer_first_app_visit`,
    COALESCE(
        `dealer_activation`.`activated_at`,
        CASE WHEN `users`.`active` THEN `users`.`created_at` END
    ) AS `activated_at`,
    `dealer_first_transaction`.`dealer_first_trasaction`
FROM `pricing-338819`.`ajans_db`.`users` AS `users`
LEFT JOIN `dealer_first_visit` ON `users`.`id` = `dealer_first_visit`.`dealer_id`
LEFT JOIN `dealer_first_transaction`
    ON `users`.`id` = `dealer_first_transaction`.`dealer_id`
LEFT JOIN `dealer_activation`
    ON `users`.`id` = `dealer_activation`.`dealer_id`
LEFT JOIN
    `pricing-338819`.`ajans_db`.`dealers_contacts` AS `dealers_contacts`
    ON `users`.`dealer_contact_id` = `dealers_contacts`.`id`
LEFT JOIN
    `pricing-338819`.`ajans_db`.`dealer_ships` AS `dealer_ships`
    ON `dealers_contacts`.`dealer_id` = `dealer_ships`.`id`
LEFT JOIN
    `pricing-338819`.`salesforce`.`Account` AS `sf_account`
    ON `users`.`sf_dealer_id` = `sf_account`.`Id`
LEFT JOIN `pricing-338819`.`ajans_db`.`users_roles` AS `users_roles`
    ON `users`.`id` = `users_roles`.`user_id`
LEFT JOIN `pricing-338819`.`ajans_db`.`roles` AS `roles`
    ON `users_roles`.`role_id` = `roles`.`id`
),  __dbt__cte__stg_opportunity_request as (


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
            GENERATE_DATE_ARRAY("2022-02-01", CURRENT_DATE(), INTERVAL 1 DAY)
        ) AS `date_array`
    
        WHERE `date_array` >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
),

`all_cars_events` AS (
    SELECT
        `aj_screen_all_auctions`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_screen_all_auctions`.`mp_event_id`) AS `all_cars_events`
    FROM
        `pricing-338819`.`mixpanel_processed`.`ajans_screen_all_auctions`
            AS `aj_screen_all_auctions`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_screen_all_auctions`.`mp_user_id` IN UNNEST(`aj_mp_users`.`mp_user_id`)
    
        WHERE
            `aj_screen_all_auctions`.`event_date`
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_screen_all_auctions`.`event_date`, `aj_mp_users`.`mobile_number`
),

`car_profile_events` AS (
    SELECT
        `aj_screen_auction_profile`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_screen_auction_profile`.`mp_event_id`)
            AS `car_profile_events`
    FROM
        `pricing-338819`.`mixpanel_processed`.`ajans_screen_auction_profile`
            AS `aj_screen_auction_profile`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_screen_auction_profile`.`mp_user_id` IN UNNEST(
                `aj_mp_users`.`mp_user_id`
            )
    
        WHERE
            `aj_screen_auction_profile`.`event_date`
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_screen_auction_profile`.`event_date`, `aj_mp_users`.`mobile_number`
),

`interested_events` AS (
    SELECT
        `aj_action_interested`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_action_interested`.`mp_event_id`) AS `interested_events`
    FROM
        `pricing-338819`.`mixpanel_processed`.`ajans_action_interested`
            AS `aj_action_interested`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_action_interested`.`mp_user_id` IN UNNEST(`aj_mp_users`.`mp_user_id`)
    
        WHERE
            `aj_action_interested`.`event_date`
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_action_interested`.`event_date`, `aj_mp_users`.`mobile_number`
),

`buy_now_events` AS (
    SELECT
        `aj_action_buy_now`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_action_buy_now`.`mp_event_id`) AS `buy_now_events`
    FROM
        `pricing-338819`.`mixpanel_processed`.`ajans_action_buy_now`
            AS `aj_action_buy_now`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_action_buy_now`.`mp_user_id` IN UNNEST(`aj_mp_users`.`mp_user_id`)
    
        WHERE
            `aj_action_buy_now`.`event_date`
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_action_buy_now`.`event_date`, `aj_mp_users`.`mobile_number`
),

`buy_now_confirmation_events` AS (
    SELECT
        `aj_screen_buy_now_confirmation_popup`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_screen_buy_now_confirmation_popup`.`mp_event_id`)
            AS `buy_now_confirmation_events`
    FROM
        `pricing-338819`.`mixpanel_processed`.`ajans_screen_buy_now_confirmation_popup`
            AS `aj_screen_buy_now_confirmation_popup`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_screen_buy_now_confirmation_popup`.`mp_user_id` IN UNNEST(
                `aj_mp_users`.`mp_user_id`
            )
    
        WHERE
            `aj_screen_buy_now_confirmation_popup`.`event_date`
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY
        `aj_screen_buy_now_confirmation_popup`.`event_date`,
        `aj_mp_users`.`mobile_number`
),

`aj_action_showroom_request` AS (
    SELECT
        `aj_action_showroom_request`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_action_showroom_request`.`event_id`) AS `showroom_events`
    FROM
        `pricing-338819`.`mixpanel_processed`.`ajans_action_showroom_request`
            AS `aj_action_showroom_request`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_action_showroom_request`.`mp_user_id` IN UNNEST(
                `aj_mp_users`.`mp_user_id`
            )
    
        WHERE
            `aj_action_showroom_request`.`event_date`
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_action_showroom_request`.`event_date`, `aj_mp_users`.`mobile_number`
),

`all_sessions` AS (
    SELECT
        `aj_sessions`.`event_date` AS `date_key`,
        `aj_mp_users`.`mobile_number`,
        COUNT(DISTINCT `aj_sessions`.`mp_session_id`) AS `all_sessions`
    FROM `pricing-338819`.`mixpanel_processed`.`ajans_sessions` AS `aj_sessions`
    LEFT JOIN `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
        ON
            `aj_sessions`.`mp_user_id` IN UNNEST(`aj_mp_users`.`mp_user_id`)
    
        WHERE `aj_sessions`.`event_date` >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
    GROUP BY `aj_sessions`.`event_date`, `aj_mp_users`.`mobile_number`
)


SELECT
    `d`.`date_key` AS `event_date`,
    `dealers`.`dealer_id`,
    `dealers`.`sf_dealer_id`,
    `dealers`.`dealer_name`,
    `dealers`.`dealer_phone`,
    `dealers`.`dealer_created_at`,
    `dealers`.`dealer_email`,
    `dealers`.`area`,
    `dealers`.`city`,
    `dealers`.`dealer_first_bid`,
    `dealers`.`dealer_type`,
    `dealers`.`dealer_first_app_visit`,
    `dealers`.`dealer_first_trasaction`,
    `dealers`.`activated_at`,
    `segmentation`.`dealer_segment` AS `current_dealer_segment`,
    COALESCE(ANY_VALUE(`all_sessions`.`all_sessions`), 0) AS `all_sessions`,
    COALESCE(ANY_VALUE(`all_cars_events`.`all_cars_events`), 0) AS `all_cars_events`,
    COALESCE(ANY_VALUE(`car_profile_events`.`car_profile_events`), 0)
        AS `car_profile_events`,
    COALESCE(ANY_VALUE(`interested_events`.`interested_events`), 0)
        AS `interested_events`,
    COALESCE(ANY_VALUE(`buy_now_events`.`buy_now_events`), 0) AS `buy_now_events`,
    COALESCE(ANY_VALUE(`buy_now_confirmation_events`.`buy_now_confirmation_events`), 0)
        AS `buy_now_confirmation_events`,
    COALESCE(ANY_VALUE(`aj_action_showroom_request`.`showroom_events`), 0)
        AS `showroom_events`,
    COUNT(DISTINCT `dealers_requests`.`id`) AS `opened_requests`,
    COALESCE(COUNT(
        DISTINCT CASE
            WHEN `arl`.`auction_request_status` = "Succeeded"
                THEN `dealers_requests`.`id`
        END
    ), 0) AS `successful_requests`,
    COALESCE(COUNT(DISTINCT `ro`.`opportunity_id`), 0)
        AS `opened_salesforce_opportunities`,
    COALESCE(COUNT(
        DISTINCT CASE
            WHEN `ro`.`opportunity_stage_name` = "Sold" THEN `ro`.`opportunity_id`
        END
    ), 0) AS `successful_salesforce_opportunities`
FROM __dbt__cte__stg_dealers AS `dealers`
LEFT JOIN
    `pricing-338819`.`dealers_activity`.`dealers_segmentation` AS `segmentation`
    ON
        `dealers`.`dealer_id` = `segmentation`.`dealer_id`
        AND DATE(DATE_TRUNC(CURRENT_DATE(), MONTH)) = `segmentation`.`month_date`
CROSS JOIN `dates` AS `d`
LEFT JOIN
    `pricing-338819`.`mixpanel_processed`.`ajans_mp_users` AS `aj_mp_users`
    ON
        LTRIM(
        REGEXP_REPLACE(dealers.dealer_phone, r'\+2|[^0-9|,]', ''),
        '0'
    )
        = LTRIM(
        REGEXP_REPLACE(aj_mp_users.mobile_number, r'\+2|[^0-9|,]', ''),
        '0'
    )
LEFT JOIN `pricing-338819`.`ajans_db`.`auction_requests` AS `dealers_requests`
    ON
        `dealers`.`dealer_id` = `dealers_requests`.`dealer_id`
        AND DATE(`dealers_requests`.`created_at`) = `d`.`date_key`
LEFT JOIN `all_cars_events` USING (`mobile_number`, `date_key`)
LEFT JOIN `car_profile_events` USING (`mobile_number`, `date_key`)
LEFT JOIN `interested_events` USING (`mobile_number`, `date_key`)
LEFT JOIN `buy_now_events` USING (`mobile_number`, `date_key`)
LEFT JOIN `buy_now_confirmation_events` USING (`mobile_number`, `date_key`)
LEFT JOIN `aj_action_showroom_request` USING (`mobile_number`, `date_key`)
LEFT JOIN `all_sessions` USING (`mobile_number`, `date_key`)
LEFT JOIN
    `pricing-338819`.`ajans_db`.`auction_request_logs` AS `arl`
    ON `dealers_requests`.`id` = `arl`.`auction_request_id`
LEFT JOIN
    __dbt__cte__stg_opportunity_request AS `ro`
    ON `dealers_requests`.`id` = `ro`.`request_id`
WHERE
    `d`.`date_key` >= DATE(`dealers`.`dealer_created_at`)
    AND LOWER(`dealers`.`dealer_email`) NOT LIKE "%sylndr%"
GROUP BY
    `dealers`.`dealer_id`,
    `dealers`.`sf_dealer_id`,
    `d`.`date_key`,
    `dealers`.`dealer_name`,
    `dealers`.`dealer_phone`,
    `dealers`.`dealer_created_at`,
    `dealers`.`dealer_email`,
    `dealers`.`area`,
    `dealers`.`city`,
    `dealers`.`dealer_first_bid`,
    `dealers`.`dealer_type`,
    `dealers`.`dealer_first_app_visit`,
    `dealers`.`dealer_first_trasaction`,
    `dealers`.`activated_at`,
    `segmentation`.`dealer_segment`
HAVING `all_cars_events` > 0 OR `all_sessions` > 0 OR `opened_requests` > 0