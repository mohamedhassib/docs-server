

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
), `users` AS (
    SELECT
        `id`,
        `name`,
        `phone`
    FROM `pricing-338819`.`ajans_db`.`users` AS `users`
),

`inner_logs` AS (
    SELECT
        `auction_request_logs`.`auction_request_id`,
        `auction_request_logs`.`auction_request_status`,
        `auction_request_logs`.`created_at`,
        `users`.`name`,
        ROW_NUMBER()
            OVER (
                PARTITION BY
                    `auction_request_logs`.`auction_request_id`
                ORDER BY `auction_request_logs`.`created_at`
            ) AS `rn`
    FROM
        `pricing-338819`.`ajans_db`.`auction_request_logs` AS `auction_request_logs`
    INNER JOIN `users`
        ON
            `auction_request_logs`.`user_id` = `users`.`id`
    
        WHERE
            DATE(`auction_request_logs`.`created_at`)
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
),

`auction_request_user_history` AS (
    SELECT
        `auction_request_id`,
        `Contacted` AS `contacted_user`,
        `Received` AS `received_user`,
        `Failed After Visit` AS `failed_after_visit_user`, -- noqa: RF05
        `Failed Before Visit` AS `failed_before_visit_user`, -- noqa: RF05
        `Visited` AS `visited_user`,
        `Succeeded` AS `succeeded_user`
    FROM (
        SELECT
            `inner_logs`.`auction_request_id`,
            `inner_logs`.`auction_request_status`,
            `inner_logs`.`name`
        FROM `inner_logs`
        WHERE
            `rn` = 1
    ) PIVOT (ANY_VALUE(
        `name`) FOR `auction_request_status` IN (
        "Contacted",
        "Received",
        "Failed After Visit",
        "Failed Before Visit",
        "Visited",
        "Succeeded"
    ))
),


`current_status` AS (
    SELECT
        `auction_request_id`,
        `auction_request_status`
    FROM (
        SELECT
            `inner_logs`.`auction_request_id`,
            `inner_logs`.`auction_request_status`
        FROM `inner_logs`
        WHERE
            `rn` = 1
    )
),


`current_status_2` AS (
    SELECT
        `auction_request_id`,
        `status` AS `auction_request_status`
    FROM `pricing-338819`.`ajans_db`.`auction_request_details`
    
        WHERE
            COALESCE(DATE(`updated_at`), DATE(`created_at`))
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
),

`comments` AS (
    SELECT
        `auction_request_id`,
        `auction_request_status`,
        `comment`
    FROM `pricing-338819`.`ajans_db`.`auction_request_comments`
    
        WHERE
            COALESCE(DATE(`updated_at`), DATE(`created_at`))
            >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    
),

`auction_request_history` AS (
    SELECT
        `auction_request_id`,
        `Contacted` AS `contacted_at`,
        `Received` AS `received_at`,
        `Failed After Visit` AS `failed_after_visit_at`, -- noqa: RF05
        `Failed Before Visit` AS `failed_before_visit_at`, -- noqa: RF05
        `Visited` AS `visited_at`,
        `Succeeded` AS `succeeded_at`
    FROM (
        SELECT
            `auction_request_id`,
            `auction_request_status`,
            `created_at`
        FROM
            `pricing-338819`.`ajans_db`.`auction_request_logs`
        
            WHERE
                COALESCE(DATE(`updated_at`), DATE(`created_at`))
                >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
        
    ) PIVOT (MIN(`created_at`) FOR `auction_request_status` IN (
        "Contacted",
        "Received",
        "Failed After Visit",
        "Failed Before Visit",
        "Visited",
        "Succeeded"
    ))
)


SELECT
    `auction_requests`.`id` AS `auction_request_id`,
    `auction_requests`.`dealer_id`,
    `users`.`phone` AS `dealer_phone`,
    `auction_requests`.`created_at` AS `auction_request_created_at`,
    `auction_requests`.`type` AS `request_type`,
    `auction_request_history`.`contacted_at`,
    `auction_request_user_history`.`contacted_user`,
    `auction_request_history`.`received_at`,
    `auction_request_user_history`.`received_user`,
    `auction_request_history`.`failed_after_visit_at`,
    `auction_request_user_history`.`failed_after_visit_user`,
    `auction_request_history`.`failed_before_visit_at`,
    `auction_request_user_history`.`failed_before_visit_user`,
    `auction_request_history`.`visited_at`,
    `auction_request_user_history`.`visited_user`,
    `auction_request_history`.`succeeded_at`,
    `sf_car`.`Name` AS `car_name`,
    `vehicles`.`id` AS `vehicle_id`,
    `vehicles`.`make` AS `car_make`,
    `vehicles`.`model` AS `car_model`,
    `vehicles`.`year` AS `car_year`,
    `vehicles`.`kilometrage` AS `car_kilometrage`,
    SAFE_CAST(`sf_car`.`Sylndr_Offer_Price_0__c` AS FLOAT64) AS `sylndr_offer_price`,
    SAFE_CAST(`rp`.`median_asking_price` AS FLOAT64) AS `median_asking_price`,
    SAFE_CAST(`rp`.`gross_selling_price` AS FLOAT64) AS `gross_selling_price`,
    COALESCE(
        `current_status_2`.`auction_request_status`,
        `comments`.`auction_request_status`,
        `current_status`.`auction_request_status`
    ) AS `request_status`,
    `comments`.`comment` AS `request_comment`,
    `sf_car`.`Category__c` AS `acquisition_source`,
    `opportunity`.`opportunity_id`,
    `opportunity`.`opportunity_stage_name`,
    `opportunity`.`opportunity_creation_datetime`,
    `opportunity`.`opportunity_close_date`
FROM `pricing-338819`.`ajans_db`.`auction_requests` AS `auction_requests`
LEFT JOIN `users`
    ON `users`.`id` = `auction_requests`.`dealer_id`
LEFT JOIN
    `pricing-338819`.`ajans_db`.`vehicle_auctions` AS `vehicle_auctions`
    ON `auction_requests`.`auction_id` = `vehicle_auctions`.`id`
LEFT JOIN
    `pricing-338819`.`ajans_db`.`vehicles` AS `vehicles`
    ON `vehicle_auctions`.`vehicle_id` = `vehicles`.`id`
LEFT JOIN
    `pricing-338819`.`salesforce`.`Car__c` AS `sf_car`
    ON `vehicles`.`sf_vehicle_id` = `sf_car`.`id`
LEFT JOIN
    `pricing-338819`.`google_sheets`.`retail_pricing` AS `rp`
    ON `vehicles`.`sf_vehicle_name` = `rp`.`car_name`
LEFT JOIN
    __dbt__cte__stg_opportunity_request AS `opportunity`
    ON
        `auction_requests`.`id` = `opportunity`.`request_id`
LEFT JOIN
    `current_status` AS `current_status`
    ON
        `auction_requests`.`id` = `current_status`.`auction_request_id`
LEFT JOIN
    `auction_request_history` AS `auction_request_history`
    ON
        `auction_requests`.`id` = `auction_request_history`.`auction_request_id`
LEFT JOIN
    `auction_request_user_history` AS `auction_request_user_history`
    ON
        `auction_requests`.`id` = `auction_request_user_history`.`auction_request_id`
LEFT JOIN
    `current_status_2` AS `current_status_2`
    ON
        `auction_requests`.`id` = `current_status_2`.`auction_request_id`
LEFT JOIN
    `comments` AS `comments`
    ON
        `auction_requests`.`id` = `comments`.`auction_request_id`