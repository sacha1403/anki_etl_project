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
        rank() over(partition by j.note_id order by j.review_id) as review_rank_for_note
from joined_tables j
join ranked_reviews rr on j.note_id = rr.note_id
order by j.note_id