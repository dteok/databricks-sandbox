CREATE OR REFRESH MATERIALIZED VIEW cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
    SELECT 
        customer_id,
        f_name,
        l_name,
        date_trunc("DD", order_timestamp) AS order_date,
        SUM(quantity) AS books_counts
    FROM 
        orders_cleaned
    WHERE
        country = 'China'
    GROUP BY
        customer_id,
        f_name,
        l_name,
        date_trunc("DD", order_timestamp)
    ;