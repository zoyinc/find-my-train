

ROLLBACK ;

/*
 * Remove all front_train_history for any rows that include
 * our train on the basis that the train has turned around
 */
UPDATE 
	fmt_train_sets
SET
	front_train_history = null
WHERE 
	FIND_IN_SET('701', train_set) > 0
;
COMMIT;

SELECT * FROM  
	fmt_train_sets
WHERE 
	FIND_IN_SET('701', train_set) > 0
;


/*
 * Trains By Route - Trains On Route
 */
		SELECT 
			custom_name , 
			train_set_display, 
			title, 
			odometer,
			train_featured_img_url,
			train_small_img_url,
			DATE_FORMAT(`last_updated`,'%d/%m/%Y - %l:%i %p') AS `last_updated_str`,
			train_number,
			geo_location,
			trip_headsign_short,
			trip_end_sec_past_midnight
		FROM 
			fmt_train_details ftd, 
			fmt_track_sections fts,
			fmt_trips ftt
		WHERE 
			ftd.section_id = fts.id
			AND ftt.trip_id = ftd.trip_id
			AND ftt.trip_headsign_short = "Pukekohe To Waitemata"
		ORDER BY 
			trip_end_sec_past_midnight ASC
		;

/*
 * Update train_set_display column of fmt_train_details from fmt_train_sets
 */
UPDATE fmt_train_details d
INNER JOIN fmt_train_sets s
    ON d.train_set = s.train_set
SET d.train_set_display = s.train_set_display;

/*
 * Cleanup fmt_train_sets
 */
SELECT * FROM fmt_train_sets  WHERE updated < now() - interval 260 MINUTE;

/*
 * Update/Insert new details into fmt_train_sets
 */
INSERT INTO 
	fmt_train_sets 
(
	train_set,
	train_set_display,
	front_train_history
)
WHERE


SELECT TIMESTAMP(CURDATE());
/*
 * Select all of fmt_trips but add calculated time
 */
SELECT  
	trip_id, 
	DATE_ADD(TIMESTAMP(CURDATE()) , INTERVAL (ft2.trip_end_sec_past_midnight + ft2.trip_delay) SECOND ) AS calculated_trip_end_datetime,
	now() - interval 11 MINUTE AS cutoff_time
FROM 
	fmt_trips ft2
;


/*
 * select where a traip should have finished
 */
SELECT  
	ftd.trip_id, 
	DATE_ADD(TIMESTAMP(CURDATE()) , INTERVAL (ft2.trip_end_sec_past_midnight + ft2.trip_delay) SECOND ) AS calculated_trip_end_datetime, 
	now() - interval 5 MINUTE AS cutoff_time,
	ftd.train_number
FROM 
	fmt_train_details ftd,
	fmt_trips ft2
WHERE ftd.trip_id IN 
	(
		SELECT 
			trip_id
		FROM 
			fmt_trips ft
		WHERE 
			DATE_ADD(TIMESTAMP(CURDATE()) , INTERVAL (ft.trip_end_sec_past_midnight + ft.trip_delay) SECOND ) < now() - INTERVAL 5 MINUTE
	)
AND 
	ftd.trip_id = ft2.trip_id
;

/*
 * Mark trains as Out Of Service if the trip should have
 * completed more than x minutes ago
 */
UPDATE  
	fmt_train_details ftd
SET
	ftd.trip_id = concat(ftd.trip_id, " oos - cleanup 2")
WHERE ftd.trip_id IN 
	(
		SELECT 
			trip_id
		FROM 
			fmt_trips ft
		WHERE 
			DATE_ADD(TIMESTAMP(CURDATE()) , INTERVAL (ft.trip_end_sec_past_midnight + ft.trip_delay) SECOND ) < now() - INTERVAL 5 MINUTE
	)
;

/*
 * Get details of the trains at this location
 * #### This is after the major updates 1/2/26
 */
		SELECT 
			custom_name , 
			title, 
			last_updated, 
			odometer,
			train_featured_img_url,
			train_small_img_url,
			DATE_FORMAT(`last_updated`,'%d/%m/%Y - %l:%i %p') AS `last_updated_str`,
			train_number,
			geo_location,
			trip_headsign_short,
			train_set
		FROM 
			fmt_train_details ftd, 
			fmt_track_sections fts,
			fmt_trips ftt
		WHERE 
			ftd.section_id = 34
			AND ftd.section_id = fts.id
			AND ftt.trip_id = ftd.trip_id
		ORDER BY 
			train_number
		;

/* 
 * Get trip details for the current train
 * #### thisis after major updates 1/2/26
 */
		SELECT 
			trip_delay_msg, 
			trip_delay,
			friendly_name
		FROM 
			fmt_train_details ftd, 
			fmt_trips ft  
		WHERE 
			train_number = 484 
			AND ftd.trip_id = ft.trip_id
		;

/*
 *  Get details for the current train
 * ### This is after major updates 1/2/26
 */
	SELECT 
		custom_name , 
		title, 
		last_updated, 
		train_featured_img_url,
		train_small_img_url,
		DATE_FORMAT(`last_updated`,'%d/%m/%Y - %l:%i %p') AS `last_updated_str`,
		train_number,
		friendly_name,
		train_description,
		geo_location,
		trip_headsign_short,
		train_set
	FROM 
		fmt_train_details ftd, 
		fmt_track_sections fts,
		fmt_trips ftt
	WHERE 
		train_number = 659
		AND ftd.section_id = fts.id
		AND ftt.trip_id = ftd.trip_id
		;

/*
 * Query for all special train details
 * #### This is for after major updates 1/2/26
 */
	SELECT 
		custom_name , 
		title, 
		last_updated, 
		odometer,
		train_featured_img_url,
		train_small_img_url,
		DATE_FORMAT(`last_updated`,'%d/%m/%Y - %l:%i %p') AS `last_updated_str`,
		train_number,
		geo_location,
		trip_headsign_short,
		train_set
	FROM 
		fmt_train_details ftd, 
		fmt_track_sections fts,
		fmt_trips ftt
	WHERE 
		special_train
		AND ftd.section_id = fts.id
		AND ftt.trip_id = ftd.trip_id
	ORDER BY 
		train_number
		;



/*
 * Clean up trips (Query)
 */
SELECT * from fmt_train_details ftd
  WHERE 
  (
      ftd.trip_id != "oos"
  )
  AND ftd.last_updated < now() - interval 12 HOUR;


/*
 * Clean up trips ( Do an update)
 */
UPDATE fmt_train_details ftd
  SET 
      ftd.trip_id = "oos (cleanup)" 
  WHERE 
  (
      ftd.trip_id != "oos"
  )
  AND ftd.last_updated < now() - interval 12 HOUR;

/*
 * Get list of routes
 */
SELECT 	
	ft.trip_headsign, ft.trip_headsign_full, ft.headsign_hash
FROM 
	fmt_trips ft 
;


/*
 * get train locations where trains are not in a yard
 */
SELECT 
	ftd.friendly_name, fts.title, ftd.most_recent_list_connected_trains, ftd.multi_train_most_recent_section,  ftd.section_id, ftd.section_id_updated, ftd.has_trip_details, ftd.trip_id
FROM 
	fmt_train_details ftd , 
	fmt_track_sections fts 
WHERE 
	ftd.section_id = fts.id
	AND fts.`type` != "Y"
;





/*
 * Query for all known 'special' train details
 */
SELECT 
		custom_name , 
		most_recent_list_connected_trains train_set, 
		train_at_britomart_end,  
		title, 
		section_id_updated, 
		heading_to_britomart, 
		odometer,
		has_trip_details,
		train_featured_img_url,
		train_small_img_url,
		DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
		train_number,
		geo_location,
		trip_headsign_short 
	FROM 
		fmt_train_details ftd, 
		fmt_track_sections fts,
		fmt_trips ftt
	WHERE 
		special_train
		AND ftd.section_id = fts.id
		AND ftt.trip_id = ftd.trip_id
	ORDER BY 
		train_number
		;

/*
 * Query the DB for trains on a particular route
 */
		SELECT 
			custom_name , 
			most_recent_list_connected_trains train_set, 
			train_at_britomart_end,
			title, 
			section_id_updated, 
			heading_to_britomart, 
			odometer,
			has_trip_details,
			train_featured_img_url,
			train_small_img_url,
			DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
			train_number,
			geo_location,
			trip_headsign_short 
		FROM 
			fmt_train_details ftd, 
			fmt_track_sections fts,
			fmt_trips ftt
		WHERE 
			ftd.section_id = fts.id
			AND ftt.trip_id = ftd.trip_id
			AND ftt.trip_headsign_short = "Out Of Service"
		ORDER BY 
			title
		;

/*
 * Get details for current train
 */
SELECT 
		custom_name , 
		most_recent_list_connected_trains train_set, 
		title, 
		section_id_updated, 
		heading_to_britomart, 
		has_trip_details,
		train_featured_img_url,
		train_small_img_url,
		DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
		train_number,
		friendly_name,
		train_description,
		geo_location,
		trip_headsign_short
	FROM 
		fmt_train_details ftd, 
		fmt_track_sections fts,
		fmt_trips ftt
	WHERE 
		train_number = 877
		AND ftd.section_id = fts.id
		AND ftt.trip_id = ftd.trip_id
		;


/*
 * Get current location name
 */
SELECT * FROM fmt_track_sections WHERE id =55;

/*
 * Get featured locations
 */
SELECT * FROM fmt_stations WHERE featured = "Y";

/*
 * Get the location name
 */
SELECT * FROM fmt_track_sections WHERE id   = 11;

/*
 * Get current location details
 */
SELECT * FROM fmt_stations WHERE section_id = -1;




UPDATE fmt_train_details ftd
	SET 
		ftd.trip_id = "oos", 
		ftd.whole_train_trip_id = "oos" 
WHERE 
	(
		ftd.trip_id != "oos" 
		OR ftd.whole_train_trip_id != "oos" 
	)
	AND ftd.section_id_updated < now() - interval 140 HOUR;

SELECT ftd.friendly_name , ftd.section_id_updated, now() - interval 140 HOUR FROM fmt_train_details ftd  
WHERE 
	(
		ftd.trip_id != "oos" 
		OR ftd.whole_train_trip_id != "oos" 
	)
	AND ftd.section_id_updated < now() - interval 100 HOUR;

/*
 *  #### Experiment ####
 * 
 * Query the DB for all trains at the selected location
 * Include headsign details
 */
SELECT 
			custom_name , 
			most_recent_list_connected_trains train_set, 
			train_at_britomart_end,
			title, 
			section_id_updated, 
			heading_to_britomart, 
			odometer,
			has_trip_details,
			train_featured_img_url,
			train_small_img_url,
			DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
			train_number,
			geo_location,
			trip_headsign_short 
		FROM 
			fmt_train_details ftd, 
			fmt_track_sections fts,
			fmt_trips ftt
		WHERE 
			ftd.section_id = 89
			AND ftd.section_id = fts.id
			AND ftt.trip_id = ftd.trip_id
		ORDER BY 
			train_number
		;





SELECT *
FROM 
	fmt_trips ftt,
	fmt_train_details ftd
WHERE 
	ftt.trip_id = ftd.trip_id
	
ORDER BY friendly_name ;

/*
 * Trains By Route Dropdown
 * 
 * This query only brings back headsigns that are assigned to trains. In other words
 * if a particular route/headsign isn't being used by any train it won't appear.
 */
SELECT DISTINCT  trip_headsign_short
FROM 
	fmt_trips ftt,
	fmt_train_details ftd
WHERE 
	ftt.trip_id = ftd.trip_id
ORDER BY trip_headsign_short ;




SELECT * FROM fmt_trips WHERE trip_id = '-1';

INSERT INTO fmt_trips (trip_id, trip_headsign, trip_headsign_short ) 
VALUES ('-1', 'Not in service', 'Not in service')


/*
 * Query the DB for all trains at the selected location
 * Include headsign details
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
			DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
			train_number,
			geo_location,
			trip_headsign_short 
		FROM 
			fmt_train_details ftd, 
			fmt_routes fr, 
			fmt_track_sections fts,
			fmt_trips ftt
		WHERE 
			ftd.section_id = 89
			AND ftd.most_recent_route_id = fr.id
			AND ftd.section_id = fts.id
			AND ftt.trip_id = ftd.trip_id
		ORDER BY 
			train_number
		;


/*
 * Query the DB for all known 'special' train details
 * Updated to include headsigns
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
		DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
		train_number,
		geo_location,
		ftt.trip_headsign_short 
	FROM 
		fmt_train_details ftd, 
		fmt_routes fr, 
		fmt_track_sections fts,
		fmt_trips ftt
	WHERE 
		special_train
		AND ftd.most_recent_route_id = fr.id 
		AND ftd.section_id = fts.id
		AND ftt.trip_id = ftd.trip_id
	ORDER BY 
		train_number
		;


/*
 * Get details for current train
 * 
 * This is updated to use trip_headsign
 */
	SELECT 
		custom_name , 
		most_recent_list_connected_trains train_set, 
		route_name_to_britomart, 
		route_name_from_britomart,  
		title, 
		section_id_updated, 
		heading_to_britomart, 
		has_trip_details,
		train_featured_img_url,
		train_small_img_url,
		DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
		train_number,
		friendly_name,
		train_description,
		geo_location,
		trip_headsign_short,
		whole_train_trip_id,
		ftt.trip_id
	FROM 
		fmt_train_details ftd, 
		fmt_routes fr, 
		fmt_track_sections fts,
		fmt_trips ftt
	WHERE 
		( train_number = 864 OR train_number = 116)
		AND ftd.most_recent_route_id = fr.id 
		AND ftd.section_id = fts.id
		AND ftt.trip_id = ftd.whole_train_trip_id
		;






/*
 *  Get list of stations
 * There is a problem in that currently trains don't report as being at Britomart
 * which is I assume because GPS doesn't work in Britomart. Instead trains that
 * are at britomart report as being at Britomart Entrance. So we need to add
 * Britomart entrance as though it was a station :-)
 */
SELECT * FROM fmt_track_sections fts 
WHERE fts.type <> 'N' OR fts.id = 39
ORDER BY title;


/*
 * Get all trains at selected location
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
		DATE_FORMAT(`section_id_updated`,'%d/%m/%Y - %l:%i %p') AS `section_id_updated_str`,
		train_number,
		geo_location
	FROM 
		fmt_train_details ftd, 
		fmt_routes fr, 
		fmt_track_sections fts  
	WHERE 
		ftd.section_id = 64 AND 
		fts.id = ftd.section_id AND 
		ftd.section_id = fts.id
		
	ORDER BY 
		train_number
		;

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

/*
 * Update the live_after_posix
 */
UPDATE
	fmt_api_keys 
SET 
	live_after_posix = 44
WHERE 
	api_key_name = 'tapisubscriptionkey_2';

/*
 * Delete record from api_keys
 */
DELETE FROM 
	fmt_api_keys 
WHERE 
	api_key_name = 'tapisubscriptionkey_2';



