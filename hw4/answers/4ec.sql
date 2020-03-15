-- Amongst users from the user table that have at least 200 reviews,
-- find the 2 “most similar” users, where the similarity of two users
-- is defined as
-- the fraction of shared businesses they’ve visited. Specifically if
-- A and B are the sets of businesses the two users have
-- visited, define similarity as |A∩B|/|A∪B| , where |x| is the number
-- of elements in x. Assume a user has visited a business if he
-- or she has a review or a tip on a business. (given as user_id1,
-- 	user_id2, similarity)


with usersWith200Reviews (user_id) as 
		(select r.user_id 
		from review as r
		inner join users as u
		on r.user_id = u.user_id
		group by r.user_id
		having count(review_id)>199),

	usersTuplesAndTheirStandardSimilarities (user_id1, user_id2, similarity) as (

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
		order by similarity desc),

	usersAndNumbersOfBusinessVisited (user_id, n_b_visited) as (
		select r.user_id, count(r.business_id)
		from review as r
		full outer join tip as t on r.business_id = t.business_id
		where r.user_id in (select user_id from usersWith200Reviews)
		group by r.user_id
		order by count(r.business_id) desc


	),
	usersTuplesAndTheirFinalSimilarities (user_id1, user_id2, similarity) as (
		select utats.user_id1, utats.user_id2, utats.similarity/(uanobv1.n_b_visited + uanobv2.n_b_visited-utats.similarity) as similarity
		from usersTuplesAndTheirStandardSimilarities as utats,
		usersAndNumbersOfBusinessVisited as uanobv1,
		usersAndNumbersOfBusinessVisited as uanobv2
		where
		utats.user_id1=uanobv1.user_id and
		utats.user_id2=uanobv2.user_id
		order by utats.similarity desc
	)

select *
from usersTuplesAndTheirFinalSimilarities
where similarity = (
	select max(similarity)
	from usersTuplesAndTheirFinalSimilarities
);
