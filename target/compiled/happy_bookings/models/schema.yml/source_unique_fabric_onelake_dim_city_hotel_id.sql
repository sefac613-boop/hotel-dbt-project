
    
    

select
    hotel_id as unique_field,
    count(*) as n_records

from [bronze_booking].[dbo].[dim_city]
where hotel_id is not null
group by hotel_id
having count(*) > 1


