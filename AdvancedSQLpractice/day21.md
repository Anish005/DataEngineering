## Number of Violations

You are given a dataset of health inspections that includes details about violations. Each row represents an inspection, and if an inspection resulted in a violation, the violation_id column will contain a value.


Count the total number of violations that occurred at 'Roxanne Cafe' for each year, based on the inspection date. Output the year and the corresponding number of violations in ascending order of the year.

Table -> sf_restaurant_health_violations


```sql 
select count(violation_id) as total_violations , extract(year from date(inspection_date)) as year  from sf_restaurant_health_violations where business_name = 'Roxanne Cafe' and inspection_score is not null group by year  order by year asc;
```