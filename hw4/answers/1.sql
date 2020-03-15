-- Find the 10 users with the most reviews.
-- Only consider reviews that have a corresponding user tuple.
-- In case of ties, break with name
-- ascending (e.g. bob before sarah).  (given as user_id, name, review_count);

with userIdAndCountReviews as
	(select r.user_id as user_id, count(r.review_id) as review_count
	from review as r inner join users as u on r.user_id = u.user_id
	group by r.user_id)

select u.user_id as user_id, u.name as name, uiacr.review_count as review_count
from users as u inner join userIdAndCountReviews as uiacr
on u.user_id=uiacr.user_id
order by review_count desc, name asc
limit 10; 

--         user_id         |    name     | review_count 
-- ------------------------+-------------+--------------
--  5iSmZO0SrKU6EoXK_1M8Kw | Lauren      |         2506
--  bsrj9_hFAql3dlSf244zpg | Staci       |         1679
--  ByFMv3p5X1aNeZhU61rDcA | Carole      |         1666
--  4GXII-GU7S0ZyU6ElkhscQ | Paula Girl  |         1334
--  plWuv4gda7m0KlqCNhrb4w | Christopher |          935
--  pLMPpaJ7whp86uL_3ezwMg | Dcjo        |          820
--  SZEFE5hL7aN5nM-A44iPwQ | Omar        |          772
--  HWl2EbZhkhAkFgqt44yUkA | Tram        |          724
--  sOYsxYYFl03PhHmz_rBDZQ | Mottita     |          705
--  0W_pPAiTXgazY2mtX6o0_w | Luke        |          640
-- (10 rows)