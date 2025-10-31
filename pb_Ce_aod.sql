--insert into pb_mlm_aod
with  
mlm as ( 
select 
distinct media_title , -- split(media_title,'/')[1] as c_id,   
LOWER(element_at(split(replace(replace(media_title ,'&#039;' ,''  ),' ',''),'/'),2)) AS mapped_title ,
server_time,
idvisitor ,
idvisit, 
sum(watched_time) as watched_time -- media_title  ,--  replace(media_title ,'&#039;' , '' )  ,
from analyticsdatabase.pb_matomo_log_media
 
where watched_time < 10800 and  server_time >= (current_timestamp - interval '24' hour)
-- and  lower(media_title)  like lower('%HAWA MAHAL%')  
-- and  lower(media_title)  like lower('%track%')
group by 1,2,3,4,5
) ,
 
mlm_ep as ( 
  SELECT
    media_title,
    mapped_title,
    server_time,
    idvisit,
    idvisitor,
    watched_time,
    case when CAST(REGEXP_EXTRACT(mapped_title, '(?i)s([0-9]+)e([0-9]+)', 1) AS INTEGER) is not null 
        then CAST(REGEXP_EXTRACT(mapped_title, '(?i)s([0-9]+)e([0-9]+)', 1) AS INTEGER)
   END  AS mlm_season ,
    case when CAST(REGEXP_EXTRACT(mapped_title, '(?i)s([0-9]+)e([0-9]+)', 2) AS INTEGER) is not null 
         then CAST(REGEXP_EXTRACT(mapped_title, '(?i)s([0-9]+)e([0-9]+)', 2) AS INTEGER)
         else CAST(REGEXP_EXTRACT(mapped_title, '(?i)track([0-9]+)', 1) AS INTEGER) 
    END AS mlm_episode 

  FROM mlm
  WHERE mapped_title IS NOT NULL
 
 
  ) , 
  eps AS (
  SELECT
    show_id,
    episode_id,
    show_title,
   --insert idvisit,
    media_title AS episode_title,
    season_number,
    episode_number,
    REPLACE(LOWER(REPLACE(show_title,'''','')),' ','') AS show_slug
  FROM analyticsdatabase.pb_content_snapshot_v2
  WHERE categories_title IN ('Music' , 'Audio')
    AND episode_id IS NOT NULL
),
 
joined AS (
  SELECT
    e.show_id,
    e.episode_id,
    e.show_title,
    e.episode_title,
    e.season_number,
    e.episode_number,
    m.mapped_title,
    m.media_title,
    m.server_time,
    m.idvisitor,
    m.idvisit,
    m.watched_time
  FROM eps e
  LEFT JOIN mlm_ep m
    ON (
      m.mapped_title = e.show_slug
      OR starts_with(m.mapped_title, e.show_slug || '-')
      OR starts_with(m.mapped_title, e.show_slug || ' ')
    )
    AND (
      CASE 
        WHEN m.mlm_season > 0 or  m.mlm_season is not null
          THEN (m.mlm_season = e.season_number) 
               AND (m.mlm_episode = e.episode_number)
        else  (m.mlm_episode = e.episode_number)
      END
    )
),
 
final AS (
  SELECT
    show_id,
    show_title,
   -- media_title,
    mapped_title,
    episode_id,
    episode_title,
    season_number,
    episode_number,
    server_time,
    idvisitor,
    idvisit,
    SUM(watched_time) AS watched_time
  FROM joined
  GROUP BY 1,2,3,4,5,6,7,8,9,10
) ,
 
mlm_2 as ( 
select 
distinct media_title , 
split(media_title , '/')[1] as map_id , 
--date(server_time) as
server_time , 
idvisitor , 
idvisit,
sum(watched_time) as watched_time 
from analyticsdatabase.pb_matomo_log_media 
where  watched_time < 10800
and server_time >= (current_timestamp - interval '7' hour)
and  lower(media_title)  like lower('%/track%')  
group by 1,2,3,4,5
) ,
 
eps_2 AS (
  SELECT
    show_id,
    episode_id,
    show_title,
    media_title AS episode_title,
    season_number,
    episode_number,
    REPLACE(LOWER(REPLACE(show_title,'''','')),' ','') AS show_slug
  FROM analyticsdatabase.pb_content_snapshot_v2
  WHERE categories_title IN ('Music' , 'Audio')
    AND episode_id IS NOT NULL
) ,
 
final_2 AS ( select show_id , show_title , media_title , episode_id , episode_title , season_number , episode_number , server_time , idvisitor ,idvisit, watched_time 
from eps_2 e
left join mlm_2 m  
on cast(e.episode_id as varchar) = m.map_id
),
 
prep as(
 
select show_id , episode_id , idvisitor ,idvisit, sum(watched_time)/60.0 as watched_minut , server_time from final_2
where idvisitor is not null 
group by show_id , episode_id , idvisitor , idvisit,server_time
 
union all
 
select show_id  , episode_id ,idvisitor ,idvisit, sum(watched_time)/60.0 as watched_minut , server_time from final
where  --  show_id = 12772 and 
idvisitor is not null 
group by show_id  , episode_id ,idvisitor , idvisit ,server_time
)
 
select *
FROM prep