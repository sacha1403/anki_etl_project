{{ config(
    materialized='table',
    unique_key='id'
)}}

with review as (
    select *
    from {{ source('anki_raw', 'raw_revlog') }}
), 
cards as (
    select *
    from {{ source('anki_raw', 'raw_cards') }}
),
notes as (
    select *
    from {{source('anki_raw', 'raw_notes') }}
),

joined_tables as (
    select 
        r.id as review_id,
        r.ease,
        r.ivl,
        r.time,
        r.type,
        r.review_date,
        n.id as note_id,
        n.word,
        n.language
    from review r
    left join cards c on r.card_id = c.id
    left join notes n on c.note_id = n.id
)
select * from joined_tables