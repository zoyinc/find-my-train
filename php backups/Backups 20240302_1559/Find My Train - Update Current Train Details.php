<script>
//
// Update the details for the currently selected train
//

// We need to wait until the entire page is loaded so use "window.addEventListener('load'..."
window.addEventListener('load', function () {
	
	// Get reference to current train status heading
	currTrainStatusElement = document.getElementsByClassName("zoyinc-find_my_train-curr_train_status")[0];
	
	// Get a reference to the title heading
	currTrainTitleElement = document.getElementsByClassName("zoyinc_post_title")[0];
	
	// Get a reference to the current train group
	currTrainGroupElement = document.getElementsByClassName("zoyinc_curr_train_details")[0];
	
	// Get references to the current train status
	currTrainCustomName = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_custom_name")[0];
	currTrainDescription = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_description")[0];
	
	//
	// The status details for the current train are in separate tables and
	// also using columns. This was primarily done so that it displayed correctly on phones
	// 
	
	// Get a reference to the cell containing location details
	currTrainLocationTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_location")[0].getElementsByTagName('table')[0];
	currTrainLocationTable.rows[0].cells[0].width = "35%";
	currTrainLocationStr = currTrainLocationTable.rows[0].cells[1];
	
	// Get a reference to the cell containing the list of trains in this set details
	currTrainConnectedTrainsTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_connected_trains")[0].getElementsByTagName('table')[0];
	currTrainConnectedTrainsTable.rows[0].cells[0].width = "35%";
	currTrainConnectedTrainsStr = currTrainConnectedTrainsTable.rows[0].cells[1];
	
	// Get a reference to the cell containing service details
	currTrainServiceTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_service")[0].getElementsByTagName('table')[0];
	currTrainServiceTable.rows[0].cells[0].width = "35%";
	currTrainServiceStr = currTrainServiceTable.rows[0].cells[1];
	
	// Get a reference to the cell containing updated time details
	currTrainUpdatedTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_updated_trains")[0].getElementsByTagName('table')[0];
	currTrainUpdatedTable.rows[0].cells[0].width = "35%";
	currTrainUpdatedStr = currTrainUpdatedTable.rows[0].cells[1];

<?php	
	global $wpdb; // Use the existing WordPress DB connection
	
	// 
	// Get the details for the current train
	//
	$query_results = $wpdb->get_results("
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
		train_description
	FROM 
		fmt_train_details ftd, 
		fmt_routes fr, 
		fmt_track_sections fts 
	WHERE 
		train_number = " . get_query_var('train_number') . "
		AND ftd.most_recent_route_id = fr.id 
		AND ftd.section_id = fts.id
		;");
	$friendlyName = "Train Unknown";
 	foreach($query_results as $curr_result){
		
		// Work out the service name
		if ($curr_result->heading_to_britomart == "Y") {
			$route_full_name = $curr_result->route_name_to_britomart ;
		} else {
			$route_full_name = $curr_result->route_name_from_britomart ;
		};
 		echo "routeFullName = \"" . $route_full_name . "\";";
		echo "currTrainTitleElement.innerHTML = currTrainTitleElement.innerHTML + \" - " . $curr_result->friendly_name . "\"; \n";
		echo "currTrainCustomName.innerText = \"" . $curr_result->custom_name . "\"; \n";
		echo "currTrainDescription.innerHTML = \"" . $curr_result->train_description . "\"; \n";
		echo "currTrainLocationStr.innerHTML = \"" . $curr_result->title . "\"; \n";                               // Station
		echo "currTrainServiceStr.innerHTML  = routeFullName; \n";                                                 // Service
		echo "currTrainConnectedTrainsStr.innerHTML = \"" . $curr_result->train_set . "\"; \n";                           // Trains
		echo "currTrainUpdatedStr.innerHTML = \"" . strtolower($curr_result->section_id_updated_str) . "\"; \n";  // Date
		
		// Add the featured url for later use further down this script
		$trainFeaturedImgURL = $curr_result->train_featured_img_url ;
		echo "trainFeaturedImgURLRelative = \"" . substr($trainFeaturedImgURL, strpos($trainFeaturedImgURL,"/",9))  . "\"; \n";
//		echo "alert('Pos A trainFeaturedImgURLRelative = ' + trainFeaturedImgURLRelative); \n";
	};
	
	// Get the trip details for the current train
	$query_results = $wpdb->get_results("
		SELECT 
			trip_delay_msg, 
			trip_delay,
			friendly_name
		FROM 
			fmt_train_details ftd, 
			fmt_trips ft  
		WHERE 
			train_number = " . get_query_var('train_number') . " 
			AND ftd.whole_train_trip_id = ft.trip_id
		;");
	// Display appropriate delay information for this trip 
	$statusMessage = "";
	foreach($query_results as $curr_result){
		$statusMessage = $curr_result->trip_delay_msg ;
	};
	if ($statusMessage != ""){
		echo "currTrainStatusElement.innerText = \"" . "Train is " . $statusMessage . ".\"; \n";
	} else {
		echo "currTrainStatusElement.innerText = \"No delay info - train is out of service.\"; \n";
	};
	
	// This is for tweaks that you would do depending on whether its a mobile device
	if ( wp_is_mobile() ) {
		// Do nothing currently
	} else{
		// If it's not a mobile
		// 
		// Align the heights of the mini tables for status
		// Row 0 - location table and connected trains table
		echo "heightLocationTable = currTrainLocationTable.rows[0].offsetHeight; \n";
		echo "heightConnectedTrainsTable = currTrainConnectedTrainsTable.rows[0].offsetHeight; \n";
		echo "if ( heightLocationTable > heightConnectedTrainsTable){  \n";
		echo "	currTrainConnectedTrainsTable.setAttribute(\"height\", heightLocationTable);  \n";
		echo "} else { \n";
		echo "	currTrainLocationTable.setAttribute(\"height\", heightConnectedTrainsTable);  \n";
		echo "}; \n";
		
		// Row 1 - service and updated tables
		echo "heightTrainServiceTable = currTrainServiceTable.rows[0].offsetHeight; \n";
		echo "heightTrainUpdatedTable = currTrainUpdatedTable.rows[0].offsetHeight; \n";
		echo "if ( heightTrainServiceTable > heightTrainUpdatedTable){  \n";
		echo "	currTrainUpdatedTable.setAttribute(\"height\", heightTrainServiceTable);  \n";
		echo "} else { \n";
		echo "	currTrainServiceTable.setAttribute(\"height\", heightTrainUpdatedTable);  \n";
		echo "}; \n";
	}; 
?>
	
	//
	// Update the trip details table for the current train
	//  
	figureElement = document.getElementsByClassName("zoyinc-find_my_train-cur_train_trips")[0]; 
	curTrainTripTable = figureElement.getElementsByTagName("table")[0];

<?php	
	// Get trip details for the current train
	$query_results = $wpdb->get_results("
		SELECT 
			stop_details_str, 
			trip_delay
		FROM 
			fmt_train_details ftd, 
			fmt_trips ft  
		WHERE 
			train_number = \"" . get_query_var('train_number') . "\" 
			AND ftd.whole_train_trip_id = ft.trip_id
		;");
	
	// 
	// Trip details are contained in the column "stop_details_str"
	// This is a semicolon separated list of platform details. This looks something like:
	// 
	//    "1,Britomart Train Station 2,20280,05:38:00;2,Orakei Train Station 2,20640,05:44:00; ..."
	//    
	// Each set of platform details thus looks like:
	// 
	//    "1,Britomart Train Station 2,20280,05:38:00;"
	//    
	// Thus the platform details is a colon separated list
	// 
	
	// First get the "stop_details_str" and "train_featured_img_url" 
	$trip_details = "";
 	foreach($query_results as $curr_result){
		$trip_details = $curr_result->stop_details_str ;
		
	};
	
	
	
	// Check there are trip details for this train
	if ($trip_details != ""){
		$trip_rows = explode(";", $trip_details);
		$currRowNumber = 1;
		
		// Iterate over each set of platform details
		foreach ($trip_rows as $currRow) {
			
			// Split the platfrom details into an array
			$tripRowColumns = explode(",", $currRow);
			
			// The table needs to come with one row of data, otherwise the formatting
			// gets messed up - blame WordPress
			// We add a row for each set of platform details, but since the table
			// comes with the first row already present we only insert a row if it's not the
			// first set of platorm details
			if ( $currRowNumber > 1 ){
				echo "newRow = curTrainTripTable.insertRow(curTrainTripTable.rows.length);";
				echo "newRow.insertCell(0);";
				echo "newRow.insertCell(1);";
			};
			
			// We are going to preset the depart time as being the scheduled time plus how
			// much the train is delay, or early. So the user only sees the ETA.
			// The most practical way of doing this is in the front end as the delay can go up
			// and down quite a bit during a trip, but the scheduled times don't change.
			$tripDelay = $curr_result->trip_delay;
			$estimatedDepartTime = $tripRowColumns[2] + $tripDelay;
			
			// It was challenging to work out how to format the time string, I looked at various JavaScript
			// functions, but they all had issues.
			// Then I realised it was much simpler to do it "manually" as my requirements were quite simple.
			$estimatedHour = intval($estimatedDepartTime/3600);
			$estimatedMin = round((($estimatedDepartTime - ($estimatedHour*3600))/60),0);
			if ($estimatedHour > 24){
				$estimatedHour = $estimatedHour -24;
			};
			$tripStrSuffix = "am";
			if ($estimatedHour > 12){
				$estimatedHour = $estimatedHour - 12;
				$tripStrSuffix = "pm";
			}
			$estimatedMinStr = (string)$estimatedMin;
			if ($estimatedMin < 10){
				$estimatedMinStr = "0" . (string)$estimatedMin;
			}
			
			echo "curTrainTripTable.rows[" . $currRowNumber . "].cells[0].innerHTML = \"" . $tripRowColumns[1] . "\";";
			echo "curTrainTripTable.rows[" . $currRowNumber . "].cells[1].innerHTML = \"" . $estimatedHour . ":" . $estimatedMinStr . " " . $tripStrSuffix . "\";";
			$currRowNumber+= 1;
		};
	};
?>
	//
	// Update the Featured image
	// 
	featuredImageFigureElement = document.getElementsByClassName("zoyinc_featured_image_single")[0]; 
	featuredImgElement = featuredImageFigureElement.getElementsByTagName("img")[0];
	featuredImgElement.src= trainFeaturedImgURLRelative;
	featuredImgElement.srcset= trainFeaturedImgURLRelative + " 1200w";
	delete featuredImgElement.style.removeProperty('aspect-ratio');
	


});	

</script>	 