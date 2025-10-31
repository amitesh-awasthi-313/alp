  --insert into pb_mlm_vod 

  WITH 

    mlm AS (

    SELECT

      media_title,

      LOWER(

        element_at(

          split(REPLACE(REPLACE(media_title,'&#039;',''),' ',''),'/'),2)

      ) AS mapped_title,

      TRY_CAST(

        REGEXP_EXTRACT(

          element_at(

            split(REPLACE(REPLACE(media_title,'&#039;',''),' ',''),'/'),1),

            '([0-9]+)'

        ) AS INTEGER

      ) AS mapped_id,

      server_time,

      idvisitor,

      idvisit,

      SUM(watched_time) AS watched_time

    FROM analyticsdatabase.pb_matomo_log_media

    WHERE media_title IS NOT NULL

      AND watched_time < 10800

      AND TRIM(media_title) <> ''

      and  server_time >= (current_timestamp - interval '24' hour)
 
    GROUP BY 1,2,3,4,5,6

  ),
 
  mlm_ep AS (

    SELECT

      media_title, mapped_title, mapped_id,

      server_time, idvisitor,idvisit, watched_time

    FROM mlm

    WHERE mapped_title IS NOT NULL

  ),
 
  eps AS (

    SELECT

      show_id, episode_id, show_title,

      REPLACE(LOWER(REPLACE(show_title,'''','')),' ','') AS show_slug

    FROM analyticsdatabase.pb_content_snapshot_v2

    WHERE categories_title IN ('Movies','Shorts' , 'Live','Radio Channels','TV Channels')

  ),
 
  joined AS (

    SELECT

      e.show_id,

      e.episode_id,

      m.idvisitor,

      m.idvisit,

      m.server_time,

      m.watched_time

    FROM mlm_ep m

    JOIN eps e

      ON (m.mapped_id = e.episode_id OR m.mapped_id = e.show_id)

    AND (

          m.mapped_title = e.show_slug

        OR m.mapped_title LIKE e.show_slug || '-%'

        OR m.mapped_title LIKE e.show_slug || ' %'

        OR m.mapped_title LIKE '%-' || e.show_slug

      )

  ),
 
  final AS (

    SELECT

      show_id,

      COALESCE(episode_id,0) AS episode_id,

      idvisitor,

      idvisit,

      server_time,

      SUM(watched_time)/60.0 AS watched_minutes

    FROM joined

    GROUP BY 1,2,3,4,5

  ),
 
  -- find top episode per show

  top_ep AS (

    SELECT
      show_id,

      episode_id,

      ROW_NUMBER() OVER (PARTITION BY show_id ORDER BY SUM(watched_minutes) DESC) AS rn

    FROM final

    WHERE episode_id <> 0

      AND episode_id IN (SELECT DISTINCT mapped_id FROM mlm_ep)

    GROUP BY show_id, episode_id

  ),
 
  -- replace only episode_id = 0 with top episode_id

  main AS (

    SELECT

      f.show_id,

      CASE WHEN f.episode_id = 0 THEN t.episode_id ELSE f.episode_id END AS episode_id,

      f.idvisitor,

      f.idvisit,

      f.watched_minutes,

      f.server_time

    FROM final f

    LEFT JOIN top_ep t

      ON f.show_id = t.show_id AND t.rn = 1

    WHERE (f.episode_id = 0 OR f.episode_id IN (SELECT DISTINCT mapped_id FROM mlm_ep))

  )
 
  -- aggregate final

  SELECT 

    *

  FROM main
 