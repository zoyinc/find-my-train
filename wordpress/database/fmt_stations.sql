-- zoyinc.fmt_stations definition

CREATE TABLE `fmt_stations` (
  `section_id` int(11) DEFAULT NULL,
  `section_name` varchar(100) DEFAULT NULL,
  `featured` varchar(100) DEFAULT '"N"',
  `primary_image` varchar(100) DEFAULT NULL,
  `secondary_image` varchar(100) DEFAULT NULL,
  `description` varchar(3000) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;