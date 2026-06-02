## Find the number of inspections for each risk category by inspection type

Find the number of inspections that resulted in each risk category per each inspection type.
Consider the records with no risk category value belongs to a separate category.
Output the result along with the corresponding inspection type and the corresponding total number of inspections per that type. The output should be pivoted, meaning that each risk category + total number should be a separate column.
Order the result based on the number of inspections per inspection type in descending order.

## Table - sf_restaurant_health_violations
business_address:text <br>
business_city:text <br>
business_id:bigint <br>
business_latitude:double <br>
business_location:text <br>
business_longitude:double <br>
business_name:text <br>
business_phone_number:double <br>
business_postal_code:double <br>
business_state:text <br>
inspection_date:date <br>
inspection_id:text <br>
inspection_score:double <br>
inspection_type:text <br>
risk_category:text <br>
violation_description:text <br>
violation_id:text <br>

```sql
with cte as (
    select inspection_type,
    sum(case when risk_category is null then 1 else 0 end) as no_risk_results,
    sum(case when risk_category = "Low Risk" then 1 else 0 end) as low_risk_results,
    sum(case when risk_category = "Moderate Risk" then 1 else 0 end) as medium_risk_results,
    sum(case when risk_category = "High Risk" then 1 else 0 end) as high_risk_results
    from sf_restaurant_health_violations
    group by inspection_type
)
select * ,
        no_risk_results + low_risk_results + medium_risk_results + high_risk_results as total_inspections
    from cte order by total_inspections desc;

```