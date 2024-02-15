

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