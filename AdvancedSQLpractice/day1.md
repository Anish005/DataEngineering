You are analyzing a social network dataset at Google. Your task is to find mutual friends between two users, Karl and Hans. There is only one user named Karl and one named Hans in the dataset.

The output should contain 'user_id' and 'user_name' columns.

𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE users(user_id INT, user_name varchar(30));
INSERT INTO users VALUES (1, 'Karl'), (2, 'Hans'), (3, 'Emma'), (4, 'Emma'), (5, 'Mike'), (6, 'Lucas'), (7, 'Sarah'), (8, 'Lucas'), (9, 'Anna'), (10, 'John');

CREATE TABLE friends(user_id INT, friend_id INT);
INSERT INTO friends VALUES (1,3),(1,5),(2,3),(2,4),(3,1),(3,2),(3,6),(4,7),(5,8),(6,9),(7,10),(8,6),(9,10),(10,7),(10,9);

𝐄𝐱𝐩𝐥𝐚𝐧𝐚𝐭𝐢𝐨𝐧 𝐨𝐟 𝐭𝐡𝐞 𝐐𝐮𝐞𝐫𝐲:
1. The CTEs (karl_friends and hans_friends) efficiently find all friends for Karl and Hans, respectively.

2. The main query joins these CTEs with the users table to find the users who are present in both Karl's and Hans's friend lists (mutual friends).

```sql


With karl_friends as (
    Select friend_id
    From friends
    Where user_id = (select user_id from user where user_name = ‘Karl’)
)
With hans_friends as (
    Select friend_id
    From friends
    Where user_id = (select user_id from user where user_name = ‘hans’’)
)
Select u.user_id ,u.user_name
From users u
Join karl_friends kf on u.user_id = kf.friend_id
Join hans_friends hf on u.user_id = hf.friend_id;

```