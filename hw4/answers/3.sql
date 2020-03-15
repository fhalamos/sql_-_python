-- Find the number of distinct business_ids that do not have any reviews. (given as count)

select count(business_id) as count
from business
where business_id not in (
	select business_id
	from review
	group by business_id);

--  count  
-- --------
--  171618
-- (1 row)