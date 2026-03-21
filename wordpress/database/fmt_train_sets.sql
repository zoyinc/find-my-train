-- zoyinc.fmt_train_sets definition

CREATE TABLE `fmt_train_sets` (
  `train_set` varchar(100) NOT NULL,
  `train_set_display` varchar(100) DEFAULT NULL,
  `front_train_history` longtext DEFAULT NULL,
  `updated` datetime DEFAULT NULL,
  `train_set_debug` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`train_set`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;