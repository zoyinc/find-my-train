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
				   section_id_updated
				FROM 
				   fmt_train_details ftd, 
				   fmt_routes fr, 
				   fmt_track_sections fts 
				WHERE 
				   train_number = " . get_query_var('train_number') . "
				   AND ftd.most_recent_route_id = fr.id 
				   AND ftd.section_id = fts.id
				;");
	
	 // Step through each row of train details
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
		 echo $curr_result->section_id_updated . "</br>";
		 echo "</td><td width=15px></td><td width=40%><img src='" .  $smallImgURL . "' width=100%></td></tr><tr height=30px><td></td></tr></table><br><br>";

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
		   section_id_updated
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
		 echo $curr_result->section_id_updated . "</br>";
		 echo "</td><td width=15px></td><td width=40%><img src='" .  $smallImgURL . "' width=100%></td></tr><tr height=30px><td></td></tr></table>";

	};
?>

<script>

document.addEventListener('DOMContentLoaded', replaceFeaturedImage(), false);

function replaceFeaturedImage() {
	
	//newFeaturedImgURLRaw = "http://www.zoyinc.com/wp-content/uploads/2022/08/CDArchivingFeature04.jpg";
	//newFeaturedImgURL = newFeaturedImgURLRaw.substring(0, newFeaturedImgURLRaw.indexOf(".jpg"));
	//alert("About to get featured image");
	figureElements = document.getElementsByTagName('figure');
	featureImgIndex = -1;
    for (var i=0, im=figureElements.length; im>i; i++) {
		if (figureElements[i].classList.contains("wp-block-post-featured-image")){
			featureImgIndex = i;
		};
       
    };
	//alert("trainFeaturedImgURL = " + trainFeaturedImgURL);
	newFeaturedImgURL= trainFeaturedImgURL;
	if (featureImgIndex == -1){
		alert("Featured image not found");
	} else {
		//myimage = document.getElementById("fmt_train_image").getElementsByTagName("img")[0];
		featuredImgElement = figureElements[featureImgIndex].getElementsByTagName("img")[0];
		featuredImgElement.src= trainFeaturedImgURL;
		featuredImgElement.srcset= trainFeaturedImgURL + " 1200w";
		
		//featuredImgElement.src= newFeaturedImgURL + ".jpg";
		//featuredImgElement.srcset= newFeaturedImgURL + ".jpg 1200w";
		delete featuredImgElement.style.removeProperty('aspect-ratio');
		//alert("Updated Featured image " + featuredImgElement.src );
	};
};

</script>