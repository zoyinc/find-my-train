<script>
//
// Special Train Status
// ====================
// 
// This shortcode takes a group with class:
//     zoyinc-find_my_train-train_statuses_group
//     
// It then hides this original group and replicates it for each "special", or wrapped train
// updating the various fields and image at the same time. 
//

// We need to run this function after the complete page has been loaded thus using "window.addEventListener('load'..."
window.addEventListener('load', function () {
	divElements = document.getElementsByTagName('div');
	statusDivIndex = -1;
	for (var i=0, im=divElements.length; im>i; i++) {
		if (divElements[i].classList.contains("zoyinc-find_my_train-train_statuses_group")){
			statusDivIndex = i;
		};
    };
	if (statusDivIndex == -1){
		alert("Train info group element not found!");
	};	
	defaultStatusGroup = divElements[statusDivIndex];
	defaultStatusGroup.style.display = 'none'; // Hide the original WordPress group	
	currStatusGroup = defaultStatusGroup;
	
<?php
	// 
	// Query the DB for all known 'special' train details
	//
	global $wpdb; // Use the existing WordPress DB connection
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
		train_number
		;");
	
	//
	// Loop through each special train
	// 
	foreach($query_results as $curr_result){
		echo "currStatusGroup.after(defaultStatusGroup.cloneNode(true)); \n";
		echo "currStatusGroup = currStatusGroup.nextSibling;  // Add After the default group \n";
		echo "currStatusGroup.style.display = ''; // Unhide the new group \n";
		
		// Update the heading
		echo "headingElementList = currStatusGroup.getElementsByClassName(\"zoyinc_status_group_heading\"); \n";
		echo "if ( headingElementList.length != 1){ \n";
		echo "	alert(\"Status info heading not found\"); \n";
		echo "}; \n";
		echo "headingElementList[0].textContent = \"" . $curr_result->custom_name . "\"; \n";
		
		// Get the table element
		// Remember the table is surrounded by a "figure" element
		echo "figureElementList = currStatusGroup.getElementsByClassName(\"zoyinc_status_group_table\"); \n";
		echo "if ( figureElementList.length != 1){ \n";
		echo "	alert(\"Status table surrounding figure element not found\"); \n";
		echo "}; \n";
		echo "tableElementList = figureElementList[0].getElementsByTagName(\"table\"); \n";
		echo "if ( tableElementList.length != 1){ \n";
		echo "	alert(\"Status table not found\"); \n";
		echo "}; \n";
		echo "statusTableElement = tableElementList[0]; \n";
		
		// Work out the service name
		if ($curr_result->heading_to_britomart == "Y") {
			$route_full_name = $curr_result->route_name_to_britomart ;
		} else {
			$route_full_name = $curr_result->route_name_from_britomart ;
		};
 		echo "routeFullName = \"" . $route_full_name . "\";";
		
		// Set left column width as it sometimes gets squashed on a mobile
		echo "statusTableElement.rows[0].cells[0].width = \"30%\"; \n"; 
		echo "statusTableElement.rows[1].cells[0].width = \"30%\"; \n";
		echo "statusTableElement.rows[2].cells[0].width = \"30%\"; \n";
		echo "statusTableElement.rows[3].cells[0].width = \"30%\"; \n";
		
		// Update status text fields
		echo "statusTableElement.rows[0].cells[1].innerHTML = \"" . $curr_result->title . "\"; \n";                               // Station
		echo "statusTableElement.rows[1].cells[1].innerHTML = routeFullName; \n";                                                 // Service
		echo "statusTableElement.rows[2].cells[1].innerHTML = \"" . $curr_result->train_set . "\"; \n";                           // Trains
		echo "statusTableElement.rows[3].cells[1].innerHTML = \"" . strtolower($curr_result->section_id_updated_str) . "\"; \n";  // Date updated
		echo "testElement = statusTableElement.rows[3].cells[1];";
		
		// Update status image
		$smallImgURL =  substr($curr_result->train_small_img_url, strpos($curr_result->train_small_img_url,"/",9)) ;
		echo "figureElementList = currStatusGroup.getElementsByClassName(\"zoyinc_status_group_img\"); \n";
		echo "if ( figureElementList.length != 1){ \n";
		echo "	alert(\"Status table surrounding figure element not found\"); \n";
		echo "}; \n";
		echo "tableImgElementList = figureElementList[0].getElementsByTagName(\"img\"); \n";
		echo "if ( tableImgElementList.length != 1){ \n";
		echo "	alert(\"Status table image not found\"); \n";
		echo "}; \n";
		echo "tableImgElement = tableImgElementList[0];";
		echo "tableImgElement.src= \"" . $smallImgURL . "\"; \n";
		echo "tableImgElement.srcset= \"" . $smallImgURL . "\" + \" 600w\"; \n";
		echo "delete tableImgElement.style.removeProperty('aspect-ratio'); \n";
	};	
?>	
//alert("Style: " + getComputedStyle(testElement, null).getPropertyValue("font"));
//alert("Style: " + getComputedStyle(testElement).cssText );
});
</script>