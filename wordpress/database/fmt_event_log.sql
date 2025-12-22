-- zoyinc.fmt_event_log definition

CREATE TABLE `fmt_event_log` (
  `api_timestamp_posix` int(11) NOT NULL,
  `api_timestamp_datetime` datetime DEFAULT NULL,
  `event_type` varchar(100) DEFAULT NULL,
  `raw_train_details` mediumtext DEFAULT NULL,
  `event_title` varchar(100) DEFAULT NULL,
  `event_msg` mediumtext DEFAULT NULL,
  `event_id` int(11) NOT NULL,
  `train_details` mediumtext DEFAULT NULL,
  `api_cycle_start` datetime DEFAULT NULL,
  `event_timestamp` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;