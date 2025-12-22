-- zoyinc.fmt_locations definition

CREATE TABLE `fmt_locations` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `train_number` int(11) NOT NULL,
  `section_id` int(11) NOT NULL,
  `first_updated` datetime DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL,
  `trip_id` varchar(100) DEFAULT NULL,
  `latest_odometer` int(11) DEFAULT NULL,
  `latest_speed` int(11) DEFAULT NULL,
  `heading_to_britomart` varchar(100) DEFAULT NULL,
  `route_id` int(11) DEFAULT NULL,
  `first_updated_posix` int(11) DEFAULT NULL,
  `last_updated_posix` int(11) DEFAULT NULL,
  `row_inserted` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12146062 DEFAULT CHARSET=latin1;