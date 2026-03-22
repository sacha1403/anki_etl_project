{{ config(
    materialized='table',
    unique_key='review_id'
)}}

with joined_tables as (
    select *
    from {{ ref('stg_anki_data') }}
),
ranked_reviews as (
    select note_id, count(*) as total_reviews 
    from {{ ref('stg_anki_data') }} 
    group by note_id
)
select
        j.review_id,
        j.ease,
        j.ivl,
        j.time,
        j.type,
        j.review_date,
        j.word,
        j.language,
        j.note_id,
        rr.total_reviews as total_note_reviews,
        rank() over(partition by j.note_id order by j.review_id) as review_rank_for_note,
        percent_rank() over(partition by j.note_id order by j.review_id) as review_rank_for_note_percentile,
        cast((review_id - LAG(review_id, 1, 0) OVER (PARTITION BY j.note_id ORDER BY j.review_id)) / 3600000 as int) + 1 AS days_since_last_review,
    ease - LAG(ease, 1, 0) OVER (PARTITION BY j.note_id ORDER BY j.review_id) AS ease_difference_from_last,
    FIRST_VALUE(ease) OVER (PARTITION BY j.note_id ORDER BY j.review_id) AS first_ease,
    ease - FIRST_VALUE(ease) OVER (PARTITION BY j.note_id ORDER BY j.review_id) AS ease_difference_from_first
from joined_tables j
join ranked_reviews rr on j.note_id = rr.note_id
order by j.note_id
