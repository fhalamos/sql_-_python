-- Find games whose name contains the word edition with either uppercase
-- or lowercase 'e'. (given as name)
select name as name from games where name like '%edition%' or name like '%Edition%';