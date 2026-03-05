## Customer details

Find the details of each customer regardless of whether the customer made an order. Output the customer's first name, last name, and the city along with the order details.
Sort records based on the customer's first name and the order details in ascending order.

### Table

customers (<br>
address:text <br>
city:text<br>
first_name:text<br>
id:bigint<br>
last_name:text<br>
phone_number:text<br>
)

orders (<br>
cust_id:bigint<br>
id:bigint<br>
order_date:date<br>
order_details:text<br>
total_order_cost:bigint<br>
)

```sql
select c.first_name, c.last_name , c.city , o.order_details from
customers c left join orders o on c.id = o.cust_id
order by c.first_name, o.order_details;
```