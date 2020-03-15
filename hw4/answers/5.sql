-- Consider the users who have at least 50 reviews,
-- find the users who have the lowest average star reviews (might be one or more).
-- From this set for each of the reviewers, find the business they gave the highest score to.
-- (alterntive description: Amongst users from the user table  with more than 50 reviews,
-- 	for the user id whose reviews have the lowest average star rating, find the names and
-- 	ids of the businesses that user gave the highest star ratings to.)
-- Use an outer join to account for business_ids that do not match records in the business table
-- (given as user_id, user_name, business_id, business_name)

with usersWith50reviews (user_id) as(
	select r.user_id
	from review as r
	inner join users as u on r.user_id = u.user_id
	group by r.user_id
	having count(review_id)>49),
	
	usersWithLowestAverageStarReviewsAndTheirBusiness (stars, user_id, user_name, business_id, business_name) as (
		select r.stars as stars, r.user_id as user_id, u.name as user_name, r.business_id as business_id, b.name as business_name
		from review as r
		inner join users as u on r.user_id = u.user_id
		left join business as b on r.business_id = b.business_id
		where r.user_id = (
			select r.user_id
			from review as r inner join users as u on r.user_id = u.user_id  
			group by r.user_id
			having avg(stars) =(
				select min(avg) from(
					select avg(stars) as avg
					from review as r inner join usersWith50reviews as uw50r
					on r.user_id = uw50r.user_id
					group by r.user_id)
					as avgstars
				)
			)
		order by r.stars desc)

	select user_id, user_name, business_id, business_name from usersWithLowestAverageStarReviewsAndTheirBusiness
	where stars = (select max(stars) from (select stars from usersWithLowestAverageStarReviewsAndTheirBusiness as s1) as s2);




--  stars |        user_id         | user_name |      business_id       | business_name 
-- -------+------------------------+-----------+------------------------+---------------
--      5 | gyO6cj2zy6Cj7lL7Mx53_A | Darwin    | p22j58Kd6UBbJrhH5QVkqA | 
-- (1 row)
