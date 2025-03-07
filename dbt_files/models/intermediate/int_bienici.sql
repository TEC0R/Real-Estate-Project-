with duplicate as (

SELECT id, count(*) as count
FROM {{ ref('stg_bienici') }}
group by id
having count > 1
)

select *
FROM {{ ref('stg_bienici') }}
where id in (select id from duplicate )