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