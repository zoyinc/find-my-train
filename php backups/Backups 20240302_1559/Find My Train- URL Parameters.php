add_action('init','add_get_val');
function add_get_val() { 
    global $wp; 
    $wp->add_query_var('train_number');
}