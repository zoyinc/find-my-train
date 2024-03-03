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
	




select * from fmt_locations fl, fmt_track_sections fts  where train_number = 578 AND fl.section_id = fts.id  ORDER BY last_updated DESC;

/*
 * Update train details for special trains
 */
UPDATE fmt_train_details 
SET train_featured_img_url = "fred", custom_name = "custom name"
WHERE special_train ;

/*
 * Update train details for non special trains
 */
UPDATE fmt_train_details 
SET 
	train_featured_img_url = "fred default image", 
	custom_name = "custom name default"
WHERE train_number NOT IN (661,509)
;


SELECT 
		   custom_name  , 
		   most_recent_list_connected_trains train_set, 
		   train_at_britomart_end, 
		   route_name_to_britomart, 
		   route_name_from_britomart,  
		   title, 
		   section_id_updated, 
		   heading_to_britomart, 
		   odometer,
		   has_trip_details
		FROM 
		   fmt_train_details ftd, 
		   fmt_routes fr, 
		   fmt_track_sections fts 
		WHERE 
		   special_train
		   AND ftd.most_recent_route_id = fr.id 
		   AND ftd.section_id = fts.id
		;

/*
 * 
 * Get details of current train
 * 
 */
SELECT 
   custom_name , 
   most_recent_list_connected_trains train_set, 
   train_at_britomart_end, 
   route_name_to_britomart, 
   route_name_from_britomart,  
   title, 
   section_id_updated, 
   heading_to_britomart, 
   odometer,
   has_trip_details,
   train_featured_img_url,
   train_small_img_url,
   section_id_updated
FROM 
   fmt_train_details ftd, 
   fmt_routes fr, 
   fmt_track_sections fts 
WHERE 
   train_number = " . get_query_var('train_number') . "
   AND ftd.most_recent_route_id = fr.id 
   AND ftd.section_id = fts.id
LIMIT 1
;

/*
 * Get details of all currently active trains
 */
SELECT 
   custom_name , 
   most_recent_list_connected_trains train_set, 
   train_at_britomart_end, 
   route_name_to_britomart, 
   route_name_from_britomart,  
   title, 
   section_id_updated, 
   heading_to_britomart, 
   odometer,
   has_trip_details
FROM 
   fmt_train_details ftd, 
   fmt_routes fr, 
   fmt_track_sections fts 
WHERE 
   latest_event_id  > 49338
   AND ftd.most_recent_route_id = fr.id 
   AND ftd.section_id = fts.id
ORDER BY custom_name 

;



/*
 * Get details of all wrapped and special trains
 */
SELECT 
   custom_name , 
   most_recent_list_connected_trains train_set, 
   train_at_britomart_end, 
   route_name_to_britomart, 
   route_name_from_britomart,  
   title, 
   section_id_updated, 
   heading_to_britomart, 
   odometer,
   has_trip_details
FROM 
   fmt_train_details ftd, 
   fmt_routes fr, 
   fmt_track_sections fts 
WHERE 
   train_number IN (144, 471, 509, 578, 593, 620, 661, 674)
   AND ftd.most_recent_route_id = fr.id 
   AND ftd.section_id = fts.id
;

/*
 * Truncate old records 
 */

DELETE FROM fmt_locations  WHERE row_inserted < now() - interval 1 DAY;

-- Truncate a specific table
TRUNCATE TABLE fmt_locations; 



SELECT 
				   custom_name , 
				   most_recent_list_connected_trains train_set, 
				   train_at_britomart_end, 
				   route_name_to_britomart, 
				   route_name_from_britomart,  
				   title, 
				   section_id_updated, 
				   heading_to_britomart, 
				   odometer,
				   has_trip_details,
				   image_url
				FROM 
				   fmt_train_details ftd, 
				   fmt_routes fr, 
				   fmt_track_sections fts 
				WHERE 
				   train_number = 144
				   AND ftd.most_recent_route_id = fr.id 
				   AND ftd.section_id = fts.id
				;

			
			
			
/*
 * Check if the current trip id exists
 */
SELECT * FROM fmt_trips ft WHERE trip_id = '123456';


/*
 * Get trip details for current train
 */
SELECT 
	stop_details_str
FROM
	fmt_trips
WHERE 
	trip_id = "246-850082-41280-2-4253182-06f9ba01"
;

/*
 *  Get trip details for specific train
 */
SELECT 
			stop_details_str 
		FROM 
			fmt_train_details ftd, 
			fmt_trips ft  
		WHERE 
			train_number = "509" 
			AND ftd.whole_train_trip_id = ft.trip_id
		;


/*
 * Update the route id for a specfic train
 */

UPDATE fmt_train_details 
SET 
	most_recent_route_id = 7
WHERE train_number = 565;


/*
 * Get all active trip ids
 */
SELECT DISTINCT whole_train_trip_id FROM fmt_train_details ftd WHERE whole_train_trip_id != "" ;



/*
 * Update trip delay value
 */
UPDATE fmt_trips SET trip_delay = 44 WHERE trip_id = "246-850053-74100-2-2M66182-7165d017";




SELECT whole_train_trip_id FROM fmt_train_details WHERE whole_train_trip_id = "247-810104-73440-2-9178583-d3feb35a";


/*
 * Get current delays
 */

SELECT 
	ftd.most_recent_list_connected_trains, trip_delay_msg, section_id_updated, at_route_id, trip_delay
FROM 
	fmt_train_details ftd, 
	fmt_trips ft,
	fmt_routes fr 
WHERE 
	ftd.whole_train_trip_id = ft.trip_id AND 
	ft.route_id = fr.at_route_id
ORDER BY section_id_updated DESC 
;
	
	
SELECT 
	custom_name , 
	most_recent_list_connected_trains train_set, 
	train_at_britomart_end, 
	route_name_to_britomart, 
	route_name_from_britomart,  
	title, 
	section_id_updated, 
	heading_to_britomart, 
	odometer,
	has_trip_details,
	train_featured_img_url,
	train_small_img_url,
	DATE_FORMAT(`section_id_updated`,'%d/%e/%Y - %l:%i %p') AS `section_id_updated_str`,
	train_number
FROM 
	fmt_train_details ftd, 
	fmt_routes fr, 
	fmt_track_sections fts 
WHERE 
	special_train
	AND ftd.most_recent_route_id = fr.id 
	AND ftd.section_id = fts.id
ORDER BY 
	train_number;
	
/*
 * Get details for current train
 */

		SELECT 
			trip_delay_msg, 
			trip_delay,
			friendly_name
		FROM 
			fmt_train_details ftd, 
			fmt_trips ft  
		WHERE 
			train_number = "319"
			AND ftd.whole_train_trip_id = ft.trip_id
		;
	
/*
 * Delete any api keys that are not list
 */
DELETE FROM 
	fmt_api_keys
WHERE 
	api_key_name NOT IN ("tapisubscriptionkey_1","tapisubscriptionkey_2","tapisubscriptionkey_3");

