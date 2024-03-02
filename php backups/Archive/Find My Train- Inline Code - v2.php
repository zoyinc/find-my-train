<!-- Prior building dynamic trip table -->
<?php 
	 global $wpdb; // Use the existing WordPress DB connection
	
	 // Get train status details for the current train
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
		   DATE_FORMAT(`section_id_updated`,'%d/%e/%Y - %l:%i %p') AS `section_id_updated_str`
		FROM 
		   fmt_train_details ftd, 
		   fmt_routes fr, 
		   fmt_track_sections fts 
		WHERE 
		   train_number = " . get_query_var('train_number') . "
		   AND ftd.most_recent_route_id = fr.id 
		   AND ftd.section_id = fts.id
		LIMIT 1
		;");
	
	 // Display current train status
	 echo "</br>";
	 foreach($query_results as $curr_result){
		echo "<script> trainFeaturedImgURL = \"" . substr($curr_result->train_featured_img_url, strpos($curr_result->train_featured_img_url,"/",9))  . "\"; </script></b></br>";
		$smallImgURL =  substr($curr_result->train_small_img_url, strpos($curr_result->train_small_img_url,"/",9)) ;
        echo "<b>"  . $curr_result->custom_name . "</b></br>";
		echo "<table border=0 align='left'><tr><td valign='top'>";
		echo $curr_result->title . "</br>";
		if ($curr_result->heading_to_britomart == "Y") {
  			$route_full_name = $curr_result->route_name_to_britomart ;
		} else {
  			$route_full_name = $curr_result->route_name_from_britomart ;
		};
		 echo $route_full_name . "</br>";
		 echo $curr_result->train_set . "</br>";
		 //echo $curr_result->section_id_updated . "</br>";
		 echo strtolower($curr_result->section_id_updated_str) . "</br>";
		 echo "</td><td width=15px></td><td width=40%><img src='" .  $smallImgURL . "' width=100%></td></tr><tr height=30px><td></td></tr></table><br><br>";
	 };

	// Get trip details for the current train
	$query_results = $wpdb->get_results("
		SELECT 
			stop_details_str 
		FROM 
			fmt_train_details ftd, 
			fmt_trips ft  
		WHERE 
			train_number = \"" . get_query_var('train_number') . "\" 
			AND ftd.whole_train_trip_id = ft.trip_id
		;");
	// Step through each row of train details
	$trip_details = "";
 	foreach($query_results as $curr_result){
		$trip_details = $curr_result->stop_details_str ;
	};
	if ($trip_details != ""){
		$trip_rows = explode(";", $trip_details);
		foreach ($trip_rows as $x) {
		  echo "$x <br>";
		};
	} else {
		echo "</br><b>No trip details for this train</b></br>";
	};
		 
	 // Query the DB for all known 'special' train details
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
		   DATE_FORMAT(`section_id_updated`,'%d/%e/%Y - %l:%i %p') AS `section_id_updated_str`
		FROM 
		   fmt_train_details ftd, 
		   fmt_routes fr, 
		   fmt_track_sections fts 
		WHERE 
		   special_train
		   AND ftd.most_recent_route_id = fr.id 
		   AND ftd.section_id = fts.id
		;");
	
	 // Step through each row of train details
	 echo "</br></br></br>";
 	 foreach($query_results as $curr_result){
		$smallImgURL =  substr($curr_result->train_small_img_url, strpos($curr_result->train_small_img_url,"/",9)) ;
        echo "<b>"  . $curr_result->custom_name . "</b><br>";
		echo "<table width=100% border=0 align='left'><tr><td valign='top'>";
		echo $curr_result->title . "</br>";
		if ($curr_result->heading_to_britomart == "Y") {
  			$route_full_name = $curr_result->route_name_to_britomart ;
		} else {
  			$route_full_name = $curr_result->route_name_from_britomart ;
		};
		 echo $route_full_name . "</br>";
		 echo $curr_result->train_set . "</br>";
		 echo strtolower($curr_result->section_id_updated_str) . "</br>";
		 echo "</td><td width=15px></td><td width=40%><img src='" .  $smallImgURL . "' width=100%></td></tr><tr height=30px><td></td></tr></table>";

	};
?>

<script>
//
// Function to update the table
//
document.addEventListener('DOMContentLoaded', updateCurTrainTripTable(), false);
function updateCurTrainTripTable() {
	figureElements = document.getElementsByTagName('figure');
	featureImgIndex = -1;
    for (var i=0, im=figureElements.length; im>i; i++) {
		if (figureElements[i].classList.contains("zoyinc-find_my_train-cur_train_trips")){
			figureIndexCurTrainTrips = i;
		};
    };
	if (figureIndexCurTrainTrips == -1){
		alert("Current train trip table was not found!");
	} else {
		curTrainTripTable = figureElements[figureIndexCurTrainTrips].getElementsByTagName("table")[0];
		curTrainTripTable.rows[0].cells[0].innerHTML = "NEW Heading";
    	//var y=x[0].cells
    	//y[0].innerHTML="NEW CONTENT"

	};

	newRow = curTrainTripTable.insertRow(curTrainTripTable.rows.length);
	newRow.insertCell(0);
	newRow.cells[0].innerHTML = "Added new row";
	newRow.insertCell(1);
	newRow.cells[1].innerHTML = "Added new row VALUE";
	
	newRow = curTrainTripTable.insertRow(curTrainTripTable.rows.length);
	newRow.insertCell(0);
	newRow.cells[0].innerHTML = "Added new row";
	newRow.insertCell(1);
	newRow.cells[1].innerHTML = "Added new row VALUE";
};

//
// Function to replace the Featured Image
// 
document.addEventListener('DOMContentLoaded', replaceFeaturedImage(), false);
function replaceFeaturedImage() {
	figureElements = document.getElementsByTagName('figure');
	featureImgIndex = -1;
    for (var i=0, im=figureElements.length; im>i; i++) {
		if (figureElements[i].classList.contains("wp-block-post-featured-image")){
			featureImgIndex = i;
		};
    };
	newFeaturedImgURL= trainFeaturedImgURL;
	if (featureImgIndex == -1){
		alert("Featured image not found");
	} else {
		featuredImgElement = figureElements[featureImgIndex].getElementsByTagName("img")[0];
		featuredImgElement.src= trainFeaturedImgURL;
		featuredImgElement.srcset= trainFeaturedImgURL + " 1200w";
		delete featuredImgElement.style.removeProperty('aspect-ratio');
	};
	//alert("Test");
};

</script>