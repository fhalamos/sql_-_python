-- For all groupings of minplayers and maxplayers
-- (e.g.  group by minplayers,maxplayers) show the minimum number of hours
-- and maximum number of hours sorted minplayers and then maxplayers
-- (ascending for both). Note that min/maxplaytime is given in minutes.
-- Do not round or truncate hours.
-- (given as minhours,maxhours,minplayers,maxplayers)
select CAST(min(minplaytime) as FLOAT)/CAST(60 as FLOAT) as minhours, CAST(max(maxplaytime) as FLOAT)/CAST(60 as FLOAT) as maxhours, minplayers as minplayers, maxplayers as maxplayers from games group by minplayers, maxplayers order by minplayers, maxplayers;