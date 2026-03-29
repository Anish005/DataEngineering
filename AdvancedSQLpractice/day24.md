## Users By Average Time

Calculate each user's average session time, where a session is defined as the time difference between a page_load and a page_exit. Assume each user has only one session per day. If there are multiple page_load or page_exit events on the same day, use only the latest page_load and the earliest page_exit. Only consider sessions where the page_load occurs before the page_exit on the same day. Output the user_id and their average session time.

### table - facebook_web_log
user_id:bigint<br>
timestamp:datetime<br>
action:text<br>
```sql 

with cte as (
    select user_id, date(timestamp) as date,
    max(case when action = 'page_load' then timestamp else null end) 
    as late_load,
    min(case when action = 'page_exit' then timestamp else null end)
    as early_exit
    from facebook_web_log
    group by user_id, date
)

select user_id, avg(timestampdiff(second,late_load,early_exit)) as avg_time from cte
where early_exit is not null and late_load is not null
group by user_id;

```