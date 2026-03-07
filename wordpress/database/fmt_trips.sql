-- zoyinc.fmt_trips definition

CREATE TABLE `fmt_trips` (
  `trip_id` varchar(100) NOT NULL,
  `stop_details_str` text DEFAULT NULL,
  `updated` datetime DEFAULT current_timestamp(),
  `trip_delay` int(11) DEFAULT 0,
  `trip_delay_msg` varchar(100) DEFAULT NULL,
  `route_id` varchar(100) DEFAULT NULL,
  `direction_id` int(11) DEFAULT 0,
  `trip_headsign` varchar(100) DEFAULT '"DB Default"',
  `trip_headsign_short` varchar(100) DEFAULT '"DB Default Shortened"',
  `trip_headsign_full` varchar(100) DEFAULT NULL,
  `headsign_hash` varchar(100) DEFAULT NULL,
  `trip_end_sec_past_midnight` int(11) DEFAULT NULL,
  PRIMARY KEY (`trip_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;