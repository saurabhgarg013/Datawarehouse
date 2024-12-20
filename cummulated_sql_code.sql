create schema tech_1;

CREATE TABLE tech_1.events (
    user_id INTEGER,
    event_type VARCHAR,
    event_timestamp TIMESTAMP,
    event_date DATE
);

INSERT INTO tech_1.events (user_id, event_type, event_timestamp, event_date)
VALUES 
    (1, 'like', '2023-12-14 10:00:00', '2023-12-14'),
    (2, 'comment', '2023-12-14 11:00:00', '2023-12-14'),
    (3, 'share', '2023-12-14 12:00:00', '2023-12-14'),
    (1, 'view', '2023-12-13 09:00:00', '2023-12-13'),
    (2, 'like', '2023-12-12 15:00:00', '2023-12-12'),
    (3, 'like', '2023-12-13 18:00:00', '2023-12-13'),
    (1, 'comment', '2023-12-14 09:30:00', '2023-12-14'),
    (4, 'like', '2023-12-11 20:00:00', '2023-12-11'),
    (5, 'share', '2023-12-10 12:00:00', '2023-12-10'),
    (3, 'comment', '2023-12-12 08:00:00', '2023-12-12'),
    (4, 'like', '2023-12-14 13:00:00', '2023-12-14'),
    (1, 'share', '2023-12-14 14:00:00', '2023-12-14');

select * from tech_1.events order by event_date;

CREATE TABLE tech_1.active_users_daily (
    user_id INTEGER,
    is_active_today INTEGER,
    num_likes INTEGER,
    num_comments INTEGER,
    num_shares INTEGER,
    snapshot_date DATE
);

INSERT INTO tech_1.active_users_daily
SELECT
    user_id,
    -- If the user_id has at least 1 event, they are daily active
    CASE WHEN COUNT(user_id) > 0 THEN 1 ELSE 0 END AS is_active_today,
    COUNT(CASE WHEN event_type = 'like' THEN 1 END) AS num_likes,
    COUNT(CASE WHEN event_type = 'comment' THEN 1 END) AS num_comments,
    COUNT(CASE WHEN event_type = 'share' THEN 1 END) AS num_shares,
    CAST('2023-12-12' AS DATE) AS snapshot_date
FROM tech_1.events
WHERE event_date = '2023-12-12'
GROUP BY user_id;

--delete from tech_1.active_users_daily;
select * from tech_1.active_users_daily;

--separet table open below table query 

CREATE TABLE tech_1.active_users_cumulated (
    user_id INTEGER,
    is_daily_active INTEGER,
    is_weekly_active INTEGER,
    is_monthly_active INTEGER,
    activity_array INTEGER[],
    like_array INTEGER[],
    share_array INTEGER[],
    comment_array INTEGER[],
    num_likes_7d INTEGER,
    num_comments_7d INTEGER,
    num_shares_7d INTEGER,
    num_likes_30d INTEGER,
    num_comments_30d INTEGER,
    num_shares_30d INTEGER,
    snapshot_date DATE
);

select * from tech_1.active_users_cumulated order by snapshot_date;




INSERT INTO tech_1.active_users_cumulated

-- First read in yesterday's data from the cumulated table
WITH yesterday AS (
    SELECT * 
    FROM tech_1.active_users_cumulated
    WHERE snapshot_date = '2023-12-11'
),
-- Read in today's active user data from the daily table
today AS (
    SELECT * 
    FROM tech_1.active_users_daily
    WHERE snapshot_date = '2023-12-12'
),

-- Combine yesterday's and today's data using FULL OUTER JOIN
combined AS (
    SELECT
        COALESCE(y.user_id, t.user_id) AS user_id,
        -- Activity array logic
		-- tracks up to 30 days of daily activity status for each user.
         -- The most recent activity (today's) is always the first element in the array.
              -- Older activity data is gradually pushed out as new days are added.
			  --eg ARRAY[1] || [1, 0, 1, 1, 0] = [1, 1, 0, 1, 1, 0]
        COALESCE(
            CASE 
                WHEN array_length(y.activity_array, 1) IS NULL THEN ARRAY[COALESCE(t.is_active_today, 0)]
                WHEN array_length(y.activity_array, 1) < 30 THEN ARRAY[COALESCE(t.is_active_today, 0)] || y.activity_array
                ELSE ARRAY[COALESCE(t.is_active_today, 0)] || y.activity_array[1:29]
            END,
            ARRAY[COALESCE(t.is_active_today, 0)]
        ) AS activity_array,

        -- Likes array logic
        COALESCE(
            CASE 
                WHEN array_length(y.like_array, 1) IS NULL 
				    THEN ARRAY[COALESCE(t.num_likes, 0)]
                WHEN array_length(y.like_array, 1) < 30 
				         THEN ARRAY[COALESCE(t.num_likes, 0)] || y.like_array
                ELSE ARRAY[COALESCE(t.num_likes, 0)] || y.like_array[1:29]
            END,
            ARRAY[COALESCE(t.num_likes, 0)]
        ) AS like_array,

        -- Comments array logic
        COALESCE(
            CASE 
                WHEN array_length(y.comment_array, 1) IS NULL 
				          THEN ARRAY[COALESCE(t.num_comments, 0)]
                WHEN array_length(y.comment_array, 1) < 30 
				        THEN ARRAY[COALESCE(t.num_comments, 0)] || y.comment_array
                ELSE 
				   ARRAY[COALESCE(t.num_comments, 0)] || y.comment_array[1:29]
            END,
                  ARRAY[COALESCE(t.num_comments, 0)]
        ) AS comment_array,

        -- Shares array logic
        COALESCE(
            CASE 
                WHEN array_length(y.share_array, 1) IS NULL 
				    THEN ARRAY[COALESCE(t.num_shares, 0)]
                
				WHEN array_length(y.share_array, 1) < 30 
				    THEN ARRAY[COALESCE(t.num_shares, 0)] || y.share_array
                ELSE 
				     ARRAY[COALESCE(t.num_shares, 0)] || y.share_array[1:29]
            END,
            ARRAY[COALESCE(t.num_shares, 0)]
        ) AS share_array,
        
       COALESCE(t.snapshot_date,'2023-12-12') as snapshot_date   
	   -- 2023-12-14'  this date when todays run data today snap shot date come run
		--COALESCE(t.snapshot_date, CURRENT_DATE) AS snapshot_date  ideally ir run on current date daily bases
		--t.snapshot_date
    FROM yesterday y
    FULL OUTER JOIN today t
    ON y.user_id = t.user_id
)

SELECT
    user_id,
    activity_array[1] AS is_daily_active,
    -- Weekly active logic (using unnest to expand the array)
    CASE 
        WHEN COALESCE(
            (SELECT COUNT(*) FROM unnest(activity_array[1:7]) AS activity WHERE activity > 0),
            0
        ) > 0 THEN 1
        ELSE 0
    END AS is_weekly_active,
    
    -- Monthly active logic (using unnest to expand the array)
    CASE 
        WHEN COALESCE(
            (SELECT COUNT(*) FROM unnest(activity_array) AS activity WHERE activity > 0),
            0
        ) > 0 THEN 1
        ELSE 0
    END AS is_monthly_active,

    activity_array,
    like_array,
    share_array,
    comment_array,
    -- Replacing 'value' with specific activity columns
    -- 7-day activities (unnest the arrays and filter for > 0)
	-- unnest This function call converts the specified array subset into a set of rows.
	-- filters the rows to include only those where the value is greater than 0.
	-- and then This counts the number of rows that satisfy the condition.
    COALESCE(
        (SELECT COUNT(*) FROM unnest(like_array[1:7]) AS like_value WHERE like_value > 0),
        0
    ) AS num_likes_7d,
    
    COALESCE(
        (SELECT COUNT(*) FROM unnest(comment_array[1:7]) AS comment_value WHERE comment_value > 0),
        0
    ) AS num_comments_7d,
    
    COALESCE(
        (SELECT COUNT(*) FROM unnest(share_array[1:7]) AS share_value WHERE share_value > 0),
        0
    ) AS num_shares_7d,

    -- 30-day activities (unnest the arrays and filter for > 0)
    COALESCE(
        (SELECT COUNT(*) FROM unnest(like_array) AS like_value WHERE like_value > 0),
        0
    ) AS num_likes_30d,
    
    COALESCE(
        (SELECT COUNT(*) FROM unnest(comment_array) AS comment_value WHERE comment_value > 0),
        0
    ) AS num_comments_30d,
    
    COALESCE(
        (SELECT COUNT(*) FROM unnest(share_array) AS share_value WHERE share_value > 0),
        0
    ) AS num_shares_30d,
    
    snapshot_date
FROM combined;





