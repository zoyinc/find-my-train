<script>
//
// Function to update the current trains details
//

// We need to wait until the entire page is loaded so use "window.addEventListener('load'..."
window.addEventListener('load', function () {
	
	// Get reference to current train status heading
	currTrainStatusElementsList = document.getElementsByClassName("zoyinc-find_my_train-curr_train_status");
	if ( currTrainStatusElementsList.length != 1){
		alert("Current train status element not found");
	};
	currTrainStatusElement = currTrainStatusElementsList[0];
	
	// Get a reference to the title heading
	currElementsList = document.getElementsByClassName("zoyinc_post_title");
	if ( currElementsList.length != 1){
		alert("Unable to locate the page title to update! Length = " + currElementsList.length);
	};
	currTrainTitleElement = currElementsList[0];
	
	// Get a reference to the current train group
	currElementsList = document.getElementsByClassName("zoyinc_curr_train_details");
	if ( currElementsList.length != 1){
		alert("Unable to locate the current train group! Length = " + currElementsList.length);
	};
	currTrainGroupElement = currElementsList[0];
	
	// Get a reference to the tables - remember they are inside "figure" and the figure has the class
	//tableFigureElement = currTrainGroupElement.getElementsByClassName("zoyinc_curr_status_table_figure")[0];
	//tableElement = tableFigureElement.getElementsByTagName("table")[0];
	
	// Disable wrapping of table headers
	//tableElement.rows[0].cells[0].width = "15%";
	//tableElement.rows[0].cells[2].width = "5%";
	//tableElement.rows[0].cells[3].width = "15%";
	//tableElement.rows[0].cells[2].style.backgroundColor = "white";
	//tableElement.rows[1].cells[2].style.backgroundColor = "white";


	
	// Get references to the current train status
	currTrainCustomName = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_custom_name")[0];
	currTrainDescription = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_description")[0];
	//alert("currTrainGroupElement = " + currTrainGroupElement.getElementsByClassName("zoyinc_curr_status_table_1").length);
	//currTrainStatusTable1 = currTrainGroupElement.getElementsByClassName("zoyinc_curr_status_table_1")[0];
	//currTrainStatusTable2 = currTrainGroupElement.getElementsByClassName("zoyinc_curr_status_table_2")[0];
	//currTrainLocationFigure = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_location")[0];
	//
	currTrainLocationTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_location")[0].getElementsByTagName('table')[0];
	currTrainLocationTable.rows[0].cells[0].width = "35%";
	currTrainLocationStr = currTrainLocationTable.rows[0].cells[1];
	
	currTrainConnectedTrainsTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_connected_trains")[0].getElementsByTagName('table')[0];
	currTrainConnectedTrainsTable.rows[0].cells[0].width = "35%";
	currTrainConnectedTrainsStr = currTrainConnectedTrainsTable.rows[0].cells[1];
	
	currTrainServiceTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_service")[0].getElementsByTagName('table')[0];
	currTrainServiceTable.rows[0].cells[0].width = "35%";
	currTrainServiceStr = currTrainServiceTable.rows[0].cells[1];
	
	currTrainUpdatedTable = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_updated_trains")[0].getElementsByTagName('table')[0];
	currTrainUpdatedTable.rows[0].cells[0].width = "35%";
	currTrainUpdatedStr = currTrainUpdatedTable.rows[0].cells[1];
	
	
	
	//currTrainConnectedTrainsStr = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_trains")[0];
	
	//currTrainServiceStr = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_service")[0];
	//currTrainUpdatedStr = currTrainGroupElement.getElementsByClassName("zoyinc_curr_train_updated")[0];

<?php	
	global $wpdb; // Use the existing WordPress DB connection
	
	// 
	// Get the details for this train
	//
	$query_results = $wpdb->get_results("
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
		
		//echo "alert(\"Train is " . $curr_result->friendly_name . "\"); \n";
		echo "currTrainTitleElement.innerHTML = currTrainTitleElement.innerHTML + \" - " . $curr_result->friendly_name . "\"; \n";
		echo "currTrainCustomName.innerText = \"" . $curr_result->custom_name . "\"; \n";
		echo "currTrainDescription.innerHTML = currTrainTitleElement.innerHTML + \" - " . $curr_result->train_description . "\"; \n";
		//echo "tableElement.rows[0].cells[1].innerHTML = \"" . $curr_result->title . "\"; \n";                               // Station
		//echo "tableElement.rows[1].cells[1].innerHTML = routeFullName; \n";                                                 // Service
		//echo "tableElement.rows[0].cells[5].innerHTML = \"" . $curr_result->train_set . "\"; \n";                           // Trains
		//echo "tableElement.rows[1].cells[5].innerHTML = \"" . strtolower($curr_result->section_id_updated_str) . "\"; \n";  // Date 
		echo "currTrainLocationStr.innerHTML = \"" . $curr_result->title . "\"; \n";                               // Station
		echo "currTrainServiceStr.innerHTML  = routeFullName; \n";                                                 // Service
		echo "currTrainConnectedTrainsStr.innerHTML = \"" . $curr_result->train_set . "\"; \n";                           // Trains
		echo "currTrainUpdatedStr.innerHTML = \"" . strtolower($curr_result->section_id_updated_str) . "\"; \n";  // Date
	};
	
	// Get the current train trip details
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
	// Step through each row of train details
	$statusMessage = "";
	foreach($query_results as $curr_result){
		$statusMessage = $curr_result->trip_delay_msg ;
	};
	if ($statusMessage != ""){
		echo "currTrainStatusElement.innerText = \"" . "Train is " . $statusMessage . ".\"; \n";
	} else {
		echo "currTrainStatusElement.innerText = \"Delay information not available for this train.\"; \n";
	};

?>

});
</script>	 