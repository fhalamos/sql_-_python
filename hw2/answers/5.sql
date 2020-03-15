-- Extend the query for problem 4 to only show categories
-- that have at least 15 games in them. (given as category, avg)
select c.category as category, avg(g.avgscore) as avg from gamecat as gc,categories as c,games as g where gc.c_id = c.c_id and gc.g_id = g.g_id group by c.c_id having count(gc.g_id)>14 order by avg desc limit 5;