## Bikes Last Used
Find the last time each bike was in use. Output both the bike number and the date-timestamp of the bike's last use (i.e., the date-time the bike was returned). Order the results by bikes that were most recently used.

### Table
dc_bikeshare_q1_2012(<br>
bike_number:text <br>
duration:text<br>
duration_seconds:bigint<br>
end_station:text<br>
end_terminal:bigint<br>
end_time:timestamp<br>
id:bigint<br>
rider_type:text<br>
start_station:text<br>
start_terminal:bigint<br>
start_time:timestamp<br>
)
```sql
with bike as(select distinct bike_number, end_time , dense_rank() over(partition by 
bike_number order by end_time desc) rnk from dc_bikeshare_q1_2012 )
select bike_number, end_time from bike where rnk = 1;
```