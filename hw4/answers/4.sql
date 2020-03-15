-- Amongst users from the user table that have at least 200 reviews, find the 2 “most similar” users, 
-- where the similarity of two users is defined as
-- the number of shared businesses they’ve visited. Assume a user has visited a business if he
-- or she has a review or a tip on a business. (given as user_id1, user_id2, similarity)

-- For partial credit you may calculate a visit only as review. I would suggest starting by solving this problem, then extend to have a visit 
-- as review or tip.


with usersWith200Reviews (user_id) as 
		(select r.user_id 
		from review as r
		inner join users as u
		on r.user_id = u.user_id
		group by r.user_id
		having count(review_id)>199),

	usersTuplesAndTheirSimilarities (user_id1, user_id2, similarity) as (

		select user_id1, user_id2, sum(similarity) as similarity from (

		select r1.user_id as user_id1, r2.user_id as user_id2, count(r1.business_id) as similarity
		from review as r1, review as r2
		where r1.business_id = r2.business_id and
		r1.user_id < r2.user_id and
		r1.user_id in (select user_id from usersWith200Reviews) and
		r2.user_id in (select user_id from usersWith200Reviews)
		group by r1.user_id, r2.user_id

		union all

		select t1.user_id as user_id1, t2.user_id as user_id2, count(t1.business_id) as similarity
		from tip as t1, tip as t2
		where t1.business_id = t2.business_id and
		t1.user_id < t2.user_id and
		t1.user_id in (select user_id from usersWith200Reviews) and
		t2.user_id in (select user_id from usersWith200Reviews)
		group by t1.user_id, t2.user_id) as aggregatedTable

		group by user_id1, user_id2
		order by similarity desc)

select * from usersTuplesAndTheirSimilarities
where similarity = (select max(similarity) from usersTuplesAndTheirSimilarities);



--         user_id1        |        user_id2        | similarity 
-- ------------------------+------------------------+------------
--  4GXII-GU7S0ZyU6ElkhscQ | 5iSmZO0SrKU6EoXK_1M8Kw |         54
-- (1 row)