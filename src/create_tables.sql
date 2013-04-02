
CREATE TABLE `metric` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `host` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `tagk` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `tagv` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `tsdb` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `val_int` bigint(20) unsigned,
  `val_dbl` double(20,4),
  `metricid` bigint unsigned NOT NULL,
  `hostid` bigint unsigned NOT NULL,
   PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `tsdbtag` (
  `tsdbid` bigint unsigned NOT NULL,
  `tagkid` bigint unsigned NOT NULL,
  `tagvid` bigint unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
