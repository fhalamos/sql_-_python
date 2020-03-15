-- For the 10 users from the prior question,
-- calculate the average stars of all their reviews.
--  Use a nested subquery to calculate the  result. (given as avg)

select CAST(sum(stars) AS float)/CAST(count(stars) AS float) as avg
from review where user_id in
	(with userIdAndCountReviews as
		(select r.user_id as user_id, count(r.review_id) as review_count
		from review as r inner join users as u on r.user_id = u.user_id
		group by r.user_id)

	select u.user_id as user_id
	from users as u inner join userIdAndCountReviews as uiacr
	on u.user_id=uiacr.user_id
	order by review_count desc, name asc
	limit 10
	);


--        avg        
-- ------------------
--  3.21263050674815
