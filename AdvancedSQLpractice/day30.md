## Number Of Units Per Nationality

Write a query that returns how many different apartment-type units (counted by distinct unit_id) 
are owned by people under 30, grouped by their nationality. Sort the results by the number of apartments in descending order.

### Table 1 - airbnb_hosts
age: bigint <br>
gender: text <br>
host_id: bigint <br>
nationality: text <br>

### Table 2 - airbnb_units
city:text <br>
country:text <br>
host_id:bigint <br>
n_bedrooms:bigint <br>
n_beds:bigint <br>
unit_id:text <br>
unit_type:text <br>

```sql 
select a.nationality, count(distinct b.unit_id) as apartment_count from 
airbnb_hosts a join airbnb_units b on a.host_id = b.host_id
where b.unit_type = "Apartment" and a.age < 30 group by a.nationality order by apartment_count desc;

```