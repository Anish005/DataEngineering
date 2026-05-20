## Use of all window functions to compare the price

Rank all the products


```sql
select 
    product_name,
    list_price,
    row_number() over (order by list_price) as row_num,
    dense_rank() over( order by list_price) as dense_rank,
    rank() over(order by list_price) as rank,
    percent_rank() over(order by list_price) as pct_rank,
    ntile(75) over(order by list_price) as ntile,
    cum_dist() over(order by list_price) as cume_dist
    from products


```