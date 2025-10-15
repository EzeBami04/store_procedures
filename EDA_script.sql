-- SQL Data Exploration script 
select * from public.influencer_instagram;

create index inf_index
on public.influencer_instagram(user_id);


-- top 2 users with the highest followers

WITH max_per_user AS (
    SELECT 
        user_id,
        username,
        MAX(follower_count) AS max_followers
    FROM public.influencer_instagram
    GROUP BY user_id, username
)
SELECT *
FROM max_per_user
ORDER BY max_followers DESC
LIMIT 2;

finding duplicate records

Select user_id, Username
from public.influencer_instagram
group by user_id, Username
having count(user_id) > 1

----- duplicates keeping the last two
select user_id, username
from (
	select *, row_number() over(partition by user_id) as rnk
	from public.influencer_instagram
)
where rnk > 1;


-- retrieving distinct name on the table

select distinct username
from public.influencer_instagram;

select *  from public.influencer_instagram;

-- user with post that  highest number of likes in the last 30days
SELECT username, like_count, timestamp, profile_url, post_media_url
FROM public.influencer_instagram
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY like_count DESC
limit 5;
-- select current_date - interval '20days';
---- updating columns
update public.influencer_instagram
set timestamp = Date(timestamp);


select username, SUM(like_count) as Total_likes
from public.influencer_instagram
where username like 'is%'
group by  username
order by Total_likes desc;
--- referencing a column use only single quote

select *
from public.influencer_youtube;

-- Getting influencer with highest engagement rate on both platforms
SELECT 
    inst.username,
    SUM(inst.like_count) AS total_insta_likes,
    COALESCE(SUM(yt.subscriber_count), 0) AS total_subscriber,
    COALESCE(SUM(yt.total_view_count), 0) AS total_youtube_view
FROM public.influencer_instagram AS inst
LEFT JOIN public.influencer_youtube AS yt
    ON inst.username = yt.username
WHERE inst.like_count > 0 
  AND inst.comments_count > 0
  AND inst.follower_count > 0
GROUP BY inst.username
ORDER BY inst.username
LIMIT 5;


