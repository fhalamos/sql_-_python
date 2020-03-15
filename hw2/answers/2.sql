-- Find the 10 longest game titles (by characters) in that allow for
-- between 3 and 5 players inclusive
-- (e.g. games that can have 3, 4, or 5 players play the game) .
-- (given as name, namelen)

select name as name, length(name) as namelen from games where minplayers > 2 and maxplayers<6 order by length(name) desc limit 10;