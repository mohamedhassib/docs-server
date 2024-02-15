with __dbt__cte__stg_dealers as (


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
) SELECT
    `dealers`.*,
    `segmentation`.`dealer_segment` AS `current_dealer_segment`
FROM __dbt__cte__stg_dealers AS `dealers`
LEFT JOIN
    `pricing-338819`.`dealers_activity`.`dealers_segmentation` AS `segmentation`
    ON
        `dealers`.`dealer_id` = `segmentation`.`dealer_id`
        AND DATE(DATE_TRUNC(CURRENT_DATE(), MONTH)) = `segmentation`.`month_date`