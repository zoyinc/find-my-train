<?php
/*
YARPP Template: Zoyinc Thumbnails
Description: Zoyinc custom YARPP thumbnail to display 'medium' thumnails and subjects. https://www.zoyinc.com
Author: Zoyinc

The tags used in YARPP templates are the same as the template tags used in any WordPress template. In fact, 
any WordPress template tag will work in the YARPP Loop. You can use these template tags to display the excerpt, 
the post date, the comment count, or even some custom metadata. In addition, template tags from other plugins will also work.

Special template tags which only work within a YARPP Loop:

1. the_score()		// this will print the YARPP match score of that particular related post
2. get_the_score()		// or return the YARPP match score of that particular related post

Notes:
1. If you would like Pinterest not to save an image, add `data-pin-nopin="true"` to the img tag.
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

if ( have_posts() ) :
	$postsArray = array();
	while ( have_posts() ) :
		the_post();
        
        // Thumbnail, title and spacer for each post
        ?><div style="margin:0;"><a href="<?php the_permalink(); ?>" rel="bookmark norewrite" title="<?php the_title_attribute();  ?>"><div style="vertical-align: bottom;"  ><?php 
        the_post_thumbnail( array($yarppContentWidth,$yarppContentWidth), array('style' => 'vertical-align: bottom;') ); ?></div><div style="padding:<?php print($yarppContentPadding . "px " . $yarppContentPadding . "px " . $yarppContentPadding . "px " . $yarppContentPadding . "px;" . $yarppFontStyle) ?>;margin:0px;width:<?php print($yarppTitleWidth); ?>px;background-color:<?php  print($titleBackground); ?>;"><?php the_title(); ?></div></div>
        <div style="width:<?php print($yarppContentWidth); ?>px;height:<?php print($spacerBetweenThumbnails); ?>px;"></div></a><?php
	endwhile;
endif;
?>