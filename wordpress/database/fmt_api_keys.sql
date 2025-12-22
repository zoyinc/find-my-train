-- zoyinc.fmt_api_keys definition

CREATE TABLE `fmt_api_keys` (
  `api_key_name` varchar(100) NOT NULL,
  `live_after_posix` bigint(20) DEFAULT NULL,
  `key_value` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`api_key_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;