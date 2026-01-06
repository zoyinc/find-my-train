<?php
/*
YARPP Template: Zoyinc Thumbnails V2
Author: Zoyinc
Description: Zoyinc custom YARPP thumbnail to display 'medium' thumnails and subjects. https://www.zoyinc.com

Styling changes
===============
The primary motivation for the custom template was so that I could customize the look and feel of the 
related posts so it aligned with the rest of the site.

The heading is not displayed via YARPP as it is really hard to get that to align with the same settings
that are used on the rest of the site. So I have instead just put a label above the related posts rather
that use YARPP

YARPP Groups
============
I have 3 dynamic posts related to train information "Find My Train", "Station Trains", and "Trains By Route".

When one of these dynamic post was viewed I wanted easy accessible links to the other related dynamic
posts. It seemed logical these should appear in related posts and specifically at the top of related posts.
However YARPP doesn't support this.

My answer was 'YARPP Group'. The way this works is:

1. Create a category whose name begins with 'YARPP Group'
   - In my case it is 'YARPP Group Find My Train'
2. For all posts that you want shown at the top of the related posts, add them to
   you new category.
   - In my case I added "Find My Train", "Station Trains", and "Trains By Route" to
     the category 'YARPP Group Find My Train'.
 
     Further the only posts in the category 'YARPP Group Find My Train' are "Find My Train", 
     "Station Trains", and "Trains By Route"
3. So if we are displaying the page "Find My Train" then the custom YARPP template will identify that
   for this page it is only in one 'YARPP Group' category, and that is 'YARPP Group Find My Train'
4. The template will then display the other posts in the same category - 'YARPP Group Find My Train'
   - So it will display at the top of the related posts "Station Trains", and "Trains By Route"
5. The template will then display the standard related posts up to the max set on the page.

This mechanism means that if any other similar scenarios come up I can just create another 'YARPP Group'
and it will just work.


*/

//
// Specify the thumbnail size
//
$yarppContentWidth = 250;
$titlePadding = "5";
$yarppContentPadding = 10;
//$yarppHeading = "Related";
//$headingHeight = "40";
//$headingUnderline = "3";
//$headingSpacer = "10";
$spacerBetweenThumbnails = "20";
$yarppFontStyle = "font-style:normal;font-weight:500";

$titleBackground = "#f6f6f6";
//$headerUnderlineColor = "black";
//$headerUnderlineHeight = "3";
//$spacerBelowHeader = "25";

$yarppTitleWidth = $yarppContentWidth - $yarppContentPadding - $yarppContentPadding;

?>
<?php
echo "<!-- YARPP Zoyinc custom template v2.3 -->\n";

// Function to display a single YARPP post (defined once, used multiple times)
function display_yarpp_post($yarppContentWidth, $yarppContentPadding, $yarppFontStyle, $yarppTitleWidth, $titleBackground, $spacerBetweenThumbnails) {
    ?>
    <div style="margin:0;">
        <a href="<?php the_permalink(); ?>" rel="bookmark norewrite" title="<?php the_title_attribute(); ?>">
            <div style="vertical-align: bottom;">
                <?php the_post_thumbnail( array($yarppContentWidth,$yarppContentWidth), array('style' => 'vertical-align: bottom;') ); ?>
            </div>
            <div style="padding:<?php print($yarppContentPadding . "px " . $yarppContentPadding . "px " . $yarppContentPadding . "px " . $yarppContentPadding . "px;" . $yarppFontStyle) ?>;margin:0px;width:<?php print($yarppTitleWidth); ?>px;background-color:<?php print($titleBackground); ?>;">
                <?php the_title(); ?>
            </div>
        </a>
        <div style="width:<?php print($yarppContentWidth); ?>px;height:<?php print($spacerBetweenThumbnails); ?>px;"></div>
    </div>
    <?php
}

// Get YARPP settings - both global and effective
$yarpp_settings = get_option('yarpp');
$global_max_posts = isset($yarpp_settings['limit']) ? (int)$yarpp_settings['limit'] : 20;

// Get the effective limit from the current query (this is what YARPP is actually using)
global $wp_query;
$effective_max_posts = isset($wp_query->query_vars['showposts']) ? (int)$wp_query->query_vars['showposts'] : $global_max_posts;

// Use the effective limit
$max_posts = $effective_max_posts;

echo "<!-- YARPP Debug: Global YARPP setting: " . $global_max_posts . " -->\n";
echo "<!-- YARPP Debug: Effective page limit (from query vars): " . $effective_max_posts . " -->\n";
echo "<!-- YARPP Debug: Using max_posts: " . $max_posts . " -->\n";

// Check if current post is in a "YARPP Group" category
$current_post_categories = get_the_category();
$yarpp_group_category = null;

echo "<!-- YARPP Debug: Checking categories for current post ID " . get_the_ID() . " -->\n";

foreach ($current_post_categories as $category) {
    echo "<!-- YARPP Debug: Found category: '" . $category->name . "' -->\n";
    if (strpos($category->name, 'YARPP Group') === 0) {
        $yarpp_group_category = $category;
        echo "<!-- YARPP Debug: FOUND YARPP Group category: '" . $category->name . "' (ID: " . $category->term_id . ") -->\n";
        break;
    }
}

if (!$yarpp_group_category) {
    echo "<!-- YARPP Debug: No YARPP Group category found for this post -->\n";
}

// If we found a YARPP Group category, get all posts from that category
$priority_posts_data = array();
if ($yarpp_group_category) {
    echo "<!-- YARPP Debug: Querying posts from category '" . $yarpp_group_category->name . "' -->\n";
    
    $priority_query = new WP_Query(array(
        'cat' => $yarpp_group_category->term_id,
        'post__not_in' => array(get_the_ID()), // Exclude current post
        'posts_per_page' => $max_posts, // Limit priority posts to max posts setting
        'orderby' => 'date',
        'order' => 'DESC'
    ));
    
    if ($priority_query->have_posts()) {
        echo "<!-- YARPP Debug: Found " . $priority_query->found_posts . " priority posts in group -->\n";
        while ($priority_query->have_posts()) {
            $priority_query->the_post();
            $priority_posts_data[] = array(
                'id' => get_the_ID(),
                'post_obj' => $post
            );
            echo "<!-- YARPP Debug: Added priority post: '" . get_the_title() . "' (ID: " . get_the_ID() . ") -->\n";
        }
    } else {
        echo "<!-- YARPP Debug: No other posts found in this YARPP Group category -->\n";
    }
    wp_reset_postdata();
}

$priority_count = count($priority_posts_data);
$remaining_slots = $max_posts - $priority_count;

echo "<!-- YARPP Debug: Showing " . $priority_count . " priority posts, " . $remaining_slots . " slots remaining for YARPP posts -->\n";

// Now display priority posts first, then YARPP posts
$shown_post_ids = array();

echo "<!-- YARPP Debug: Starting to display posts -->\n";

// Display priority group posts first
if (!empty($priority_posts_data)) {
    echo "<!-- YARPP Debug: Displaying " . count($priority_posts_data) . " priority group posts first -->\n";
    foreach ($priority_posts_data as $priority_post) {
        $post = $priority_post['post_obj'];
        setup_postdata($post);
        
        echo "<!-- YARPP Debug: Showing priority post: '" . get_the_title() . "' -->\n";
        display_yarpp_post($yarppContentWidth, $yarppContentPadding, $yarppFontStyle, $yarppTitleWidth, $titleBackground, $spacerBetweenThumbnails);
        
        $shown_post_ids[] = get_the_ID();
    }
    wp_reset_postdata();
} else {
    echo "<!-- YARPP Debug: No priority posts to display -->\n";
}

// Then display regular YARPP posts (limited to remaining slots)
echo "<!-- YARPP Debug: Now showing regular YARPP related posts -->\n";
if ($remaining_slots > 0 && have_posts()) {
    $yarpp_count = 0;
    while (have_posts() && $yarpp_count < $remaining_slots) {
        the_post();
        if (!in_array(get_the_ID(), $shown_post_ids)) {
            $yarpp_count++;
            echo "<!-- YARPP Debug: Showing YARPP post #" . $yarpp_count . ": '" . get_the_title() . "' -->\n";
            display_yarpp_post($yarppContentWidth, $yarppContentPadding, $yarppFontStyle, $yarppTitleWidth, $titleBackground, $spacerBetweenThumbnails);
        } else {
            echo "<!-- YARPP Debug: Skipping duplicate post: '" . get_the_title() . "' -->\n";
        }
    }
    echo "<!-- YARPP Debug: Displayed " . $yarpp_count . " YARPP posts -->\n";
} elseif ($remaining_slots <= 0) {
    echo "<!-- YARPP Debug: No slots remaining for YARPP posts (priority posts filled all " . $max_posts . " slots) -->\n";
} else {
    echo "<!-- YARPP Debug: No YARPP related posts found -->\n";
}

echo "<!-- YARPP Debug: Total posts displayed: " . ($priority_count + (isset($yarpp_count) ? $yarpp_count : 0)) . " (max was " . $max_posts . ") -->\n";
echo "<!-- YARPP Debug: Finished displaying all posts -->\n";
?>