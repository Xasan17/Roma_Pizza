create database Roma_pizza
use Roma_pizza;
select * from stagging_table_iiko_products
select * from stagging_table_iiko_product_groups
select * from stagging_table_iiko_product_categories
select * from stagging_table_iiko_product_type
select * from iiko_cash_shifts_fact
select * from stagging_table_iiko_payinout_types
select * from stagging_table_iiko_payments_type
select * from stagging_table_iiko_price_categories
select * from stagging_quicklabels_iiko
select * from stagging_table_iiko_balance
select * from stagging_table_iiko_stores
select * from stagging_accounts_iiko

select distinct name from stagging_table_iiko_products
select * from stagging_table_iiko_stores
select p.name as product_name,type,c.name as category_name from stagging_table_iiko_products p
inner join stagging_table_iiko_product_categories c on p.category_id = c.id

select c.name as category_name, count(*) as count_product from stagging_table_iiko_products p
inner join stagging_table_iiko_product_categories c on p.category_id = c.id
group by c.name 
order by count_product desc

select g.name,count(*) count_group from stagging_table_iiko_product_groups g
inner join stagging_table_iiko_products p on g.parent = p.parent_id
group by g.name
order by count_group desc



SELECT 
    bal.productId,
    p.name AS product_name,
    g.name AS group_name,
    c.name AS category_name,
    bal.storeId,
    s.name AS store_name,
    bal.amount,
    bal.sum
FROM stagging_table_iiko_balance bal
LEFT JOIN stagging_table_iiko_products p ON bal.productId = p.id
LEFT JOIN stagging_table_iiko_product_groups g ON p.parent_id = g.id
LEFT JOIN stagging_table_iiko_product_categories c ON p.category_id = c.id
LEFT JOIN stagging_table_iiko_stores s ON bal.storeId = s.id