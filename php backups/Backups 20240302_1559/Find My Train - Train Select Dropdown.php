<script>
// We need to run this function after the complete page has been loaded thus using "window.addEventListener('load'..."
window.addEventListener('load', function () {	
	
	// Grab a reference to the master WordPress group
	byClassElementList = document.getElementsByClassName("zoyinc_train_select_form_master_group");
	if ( byClassElementList.length != 1){ 
		alert("Could not find the master group element. Length = " + byClassElementList.length); 
	};
	masterGroup = byClassElementList[0];

	// Create a train select form
	var trainSelectForm = document.createElement("form");
	trainSelectForm.setAttribute("method", "get");
	trainSelectForm.setAttribute("id", "train_select_form_id");
	
	// Wrap the train select form around the master group block
	masterGroup.replaceWith(trainSelectForm);
	trainSelectForm.appendChild(masterGroup);
	
	// Get the dummy submit button
	// In WordPress this is a "Button" block but in html it ends up as a div
	divElementsList = document.getElementsByClassName("zoyinc_dummy_submit_button");
	if ( divElementsList.length != 1){ 
		alert("Could not find the submit button. Length = " + byClassElementList.length); 
	};
	submitButtonDiv = divElementsList[0];
	// Get the anchor within the button
	anchorElements = submitButtonDiv.getElementsByTagName('a');
	if ( anchorElements.length != 1){ 
		alert("Could not find the anchor inside the dummy button. Length = " + byClassElementList.length); 
	};
	submitButtonAnchor = anchorElements[0];
	// Change the anchor so on click it submits the form
	submitButtonAnchor.setAttribute('href', '#');
	submitButtonAnchor.setAttribute('onclick','document.getElementById(\'train_select_form_id\').submit();');
		
	// Create a hidden "input" field
	// This will cause WordPress to return to this page
	var trainPostNoInput = document.createElement("input");
	trainPostNoInput.setAttribute("type", "hidden");
	trainPostNoInput.setAttribute("id", "p");
	trainPostNoInput.setAttribute("name", "p");
	trainPostNoInput.setAttribute("value", "<?php echo get_query_var('p'); ?>");

	
	// We are using the button with class "zoyinc_dummy_select_list_button" as the
	// template for the dynamic select box
	// The button ends up in html as an anchor, and a div. The class ends up on the div
	// so we have to find the anchor and will assume there is only one
	 
	// Get a reference to the div that surrounds the dummy select button
	byClassElementList = document.getElementsByClassName("zoyinc_dummy_select_list");
	if ( byClassElementList.length != 1){ 
		alert("Could not find \"div\" for dummy list box. Length = " + byClassElementList.length); 
	};
	dummyListDiv = byClassElementList[0];
	
	// Find the anchor inside the div
	// This is the element that contains the text for the dummy button. As such it contains the settings
	// for the font of the dummy button and so on.
	anchorElements = dummyListDiv.getElementsByTagName('a');
	if ( anchorElements.length != 1){ 
		alert("Could not find the anchor inside the dummy button. Length = " + byClassElementList.length); 
	};
	dummyButtonAnchor = anchorElements[0];

	// Create a select element for train number
	var trainSelect = document.createElement("select");
	trainSelect.id = "train_number";
	trainSelect.name = "train_number";
	trainSelect.style.font = getComputedStyle(dummyListDiv, null).getPropertyValue("font");
	trainSelect.style.backgroundColor = getComputedStyle(byClassElementList[0], null).getPropertyValue("background-color");
	trainSelect.style.width = getComputedStyle(byClassElementList[0], null).getPropertyValue("width");
	trainSelect.style.height = getComputedStyle(dummyListDiv, null).getPropertyValue("height");
	trainSelect.style.color = getComputedStyle(byClassElementList[0], null).getPropertyValue("color");	
	trainSelect.style.border  = getComputedStyle(dummyButtonAnchor, null).getPropertyValue("border");
	// Align the height and font of the submit button with the details for the select dropdown
	// Its actually quite hard to get this working for both PC and mobile browsers
	// What we have done is the best we can
	submitButtonDiv.style.height = trainSelect.style.height;
	submitButtonDiv.style.font = trainSelect.style.font;
	submitButtonAnchor.style.height = trainSelect.style.height;
	submitButtonAnchor.style.font = trainSelect.style.font;
<?php 
	//
	// Add train details
	// 
	global $wpdb; // Use the existing WordPress DB connection
	
	// Query the DB for all known train details
	$query_results = $wpdb->get_results('SELECT * FROM  fmt_train_details order by train_number');
	
	// Step through each row of train details
	foreach($query_results as $curr_result){
		$selected_state = false;
		
		echo "var option = document.createElement(\"option\"); \n";
		echo "option.value = \"" . $curr_result->train_number . "\";  \n";
		echo "option.text = \"" . $curr_result->custom_name . "\";  \n";
		if (get_query_var('train_number') == $curr_result->train_number){
			echo "option.selected = true; \n";
		};
		echo "option.style.color = getComputedStyle(dummyButtonAnchor, null).getPropertyValue(\"color\"); \n";
		echo "trainSelect.appendChild(option); \n";		
	};
?>	
	// Append the various elements to the form
	dummyListDiv.replaceWith(trainSelect);
	trainSelectForm.append(trainPostNoInput);
	
});	
</script>