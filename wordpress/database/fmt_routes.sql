-- zoyinc.fmt_routes definition

CREATE TABLE `fmt_routes` (
  `id` int(11) NOT NULL,
  `at_route_id` varchar(100) NOT NULL,
  `route_name_to_britomart` varchar(100) NOT NULL,
  `route_name_from_britomart` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;