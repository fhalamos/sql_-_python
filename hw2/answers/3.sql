-- Find the category id and count of games in the category,
-- for categories that have at least 15 games in them. (given as c_id, cnt)
select c_id as c_id, count(g_id) as cnt from gamecat group by c_id having count(g_id) > 14 order by cnt desc;