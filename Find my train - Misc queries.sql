select ftd.custom_name Name, ftd.image_url  , fts.title Location, fr.full_route_name, fl.last_updated Last_Updated, fl.heading_to_britomart To_Britomart
from fmt_locations fl, fmt_track_sections fts, fmt_routes fr, fmt_train_details ftd 
where fl.section_id = fts.id 
and fl.route_id = fr.id 
and fl.train_number = ftd.train_number 
and fl.train_number  in (823,321)
order by last_updated DESC ; 


select * from fmt_locations fl, fmt_track_sections fts 
where fl.section_id = fts.id
and fts.title  like "%Swanson S%"
order by fl.last_updated desc;


with latest_per_train_locations as (
	SELECT fl.*, ROW_NUMBER() OVER (PARTITION BY fl.train_number  ORDER BY last_updated  DESC) AS rn FROM fmt_locations fl)
select * from latest_per_train_locations where rn = 1;


SELECT 
   friendly_name, 
   most_recent_list_connected_trains train_set, 
   train_at_britomart_end, 
   route_name_to_britomart, 
   route_name_from_britomart,  
   title, 
   section_id_updated, 
   heading_to_britomart, 
   odometer,
   has_trip_details 
from 
   fmt_train_details ftd, 
   fmt_routes fr, 
   fmt_track_sections fts 
where 
   train_number = 443
   and ftd.most_recent_route_id = fr.id 
   and ftd.section_id = fts.id
;


select * from fmt_locations fl where train_number = 578 order by last_updated desc;




select FROM fmt_event_log fel  
WHERE row_id < ( SELECT api_timestamp_posix  FROM 
                   (SELECT * FROM fmt_event_log  
                    ORDER BY api_timestamp_posix DESC 
                    LIMIT ,1) AS us) ;
           
DELETE FROM fmt_event_log  
WHERE event_type = "info" AND api_timestamp_posix  < (SELECT api_timestamp_posix FROM (SELECT * FROM fmt_event_log WHERE event_type = "info" 
                    ORDER BY api_timestamp_posix DESC LIMIT 4,1) as oldest_record);                   
                   

                   
select * FROM fmt_event_log  
WHERE event_type = "error" AND event_id  < (SELECT event_id FROM (SELECT * FROM fmt_event_log WHERE event_type = "error" 
                    ORDER BY event_id DESC LIMIT 20,1) as oldest_record);
                    
                   
SELECT * FROM fmt_event_log  
WHERE event_type = "warn"  
	AND event_title = "Track details not found for train 'AMP945'" 
	AND event_id <= (
		SELECT event_id 
		FROM (
			SELECT * 
			FROM fmt_event_log 
			WHERE event_type = "warn" 
			AND event_title = "Track details not found for train 'AMP945'"
			ORDER BY event_id 
			DESC LIMIT 2,1
		)
	AS oldest_record
	);