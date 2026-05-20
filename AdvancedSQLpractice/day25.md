## Acceptance rate by Date
Calculate the friend acceptance rate for each date when friend requests were sent. A request is sent if action = sent and accepted if action = accepted. If a request is not accepted, there is no record of it being accepted in the table.


The output will only include dates where requests were sent and at least one of them was accepted (acceptance can occur on any date after the request is sent).

### Table - fb_friend_requests
user_id_sender:text <br>
user_id_receiver:text <br>
date:date <br>
action:text <br>

```sql
WITH sent AS (
    SELECT * 
    FROM fb_friend_requests 
    WHERE action = 'sent'
),                                                  
accept AS (
    SELECT * 
    FROM fb_friend_requests 
    WHERE action = 'accepted'
)
SELECT 
    s.date,
    ROUND(
        COUNT(a.user_id_receiver) /               
        COUNT(s.user_id_sender),                    
    2) AS rate
FROM sent s                                       
LEFT JOIN accept a                                   
    ON  s.user_id_sender   = a.user_id_sender      
    AND s.user_id_receiver = a.user_id_receiver     
GROUP BY s.date 
ORDER BY s.date;
```