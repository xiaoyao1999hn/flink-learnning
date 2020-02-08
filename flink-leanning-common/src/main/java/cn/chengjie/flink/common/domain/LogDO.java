package cn.chengjie.flink.common.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Getter
@Setter
@ToString
public class LogDO {
	private Long id;

	private Long userId;

	private String username;

	private String operation;

	private Integer time;

	private String method;

	private String params;

	private String ip;

	private Date gmtCreate;

    /**
     * 日志类型，0：研发日志；1：产品日志；
     */
	private Integer type;


}