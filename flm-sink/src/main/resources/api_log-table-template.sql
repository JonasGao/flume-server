CREATE TABLE com_api_log
(
  id            BIGINT(11) UNSIGNED                 NOT NULL AUTO_INCREMENT
  COMMENT '自增ID'
    PRIMARY KEY,
  apiMethodName VARCHAR(200)                        NOT NULL
  COMMENT 'API方法名',
  channel_id    VARCHAR(5) DEFAULT ''               NOT NULL
  COMMENT '渠道ID',
  cart_id       INT DEFAULT '0'                     NOT NULL
  COMMENT '店铺ID',
  call_time     VARCHAR(50)                         NOT NULL
  COMMENT '调用时间',
  time_zone     VARCHAR(100)                        NOT NULL
  COMMENT '时区',
  username      VARCHAR(200) DEFAULT ''             NULL
  COMMENT '用户',
  computer_name VARCHAR(200) DEFAULT ''             NOT NULL
  COMMENT '机器名',
  ip            VARCHAR(64) DEFAULT ''              NOT NULL
  COMMENT 'IP地址',
  mac           VARCHAR(46) DEFAULT ''              NOT NULL
  COMMENT 'Mac地址',
  active        TINYINT(1) DEFAULT '1'              NULL
  COMMENT '是否有效（1:有效；0：无效）',
  created       TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
  COMMENT '创建时间',
  creater       VARCHAR(50)                         NOT NULL
  COMMENT '创建者',
  modified      TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
  COMMENT '修改时间',
  modifier      VARCHAR(50)                         NOT NULL
  COMMENT '修改者'
)
  COMMENT 'API日志表';

CREATE INDEX AK_api_log_idx02
  ON com_api_log (channel_id, cart_id);

CREATE INDEX AK_api_log_idx03
  ON com_api_log (ip);

CREATE INDEX api_log_idx01
  ON com_api_log (apiMethodName, channel_id, cart_id);
