-- zoyinc.fmt_train_details definition

CREATE TABLE `fmt_train_details` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Primary key',
  `vehicle_label` varchar(100) NOT NULL COMMENT 'AT vehicle label, eg ''AMP      578''',
  `friendly_name` varchar(100) NOT NULL COMMENT 'Friendly name, eg AMP578',
  `odometer` int(11) DEFAULT NULL COMMENT 'Odometer reading, in metres',
  `train_featured_img_url` varchar(100) DEFAULT NULL COMMENT 'URL link to image of train',
  `custom_name` varchar(100) DEFAULT NULL COMMENT 'Custom name, such as ''AMP509 - Heads Up Ears Out Trains About''',
  `train_number` varchar(100) DEFAULT NULL COMMENT 'Raw train number, eg. 578',
  `train_set` varchar(100) DEFAULT NULL COMMENT 'Most recent list of multi-trains this train was a part of, eg. ''AMP578 and AMP113''',
  `section_id` int(11) DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL,
  `geo_location` varchar(100) DEFAULT NULL,
  `train_small_img_url` varchar(100) DEFAULT NULL,
  `train_description` text DEFAULT NULL,
  `special_train` tinyint(1) DEFAULT NULL,
  `trip_id` varchar(100) DEFAULT '-1',
  `position_history` mediumtext DEFAULT NULL,
  `train_set_display` varchar(100) DEFAULT NULL,
  `heading_to_britomart` varchar(100) DEFAULT NULL,
  `last_good_heading_to_britomart` varchar(100) DEFAULT 'N' COMMENT 'The problem with the heading to britomart column is that it refects what is given us us from the realtime vehicle positions api call. This can return a valid bearing, a zero , or no bearing value at all. We need to know which was the last know direction for this train and that is what this column is for',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=172 DEFAULT CHARSET=latin1;