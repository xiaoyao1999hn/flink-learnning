package cn.chengjie.flink.leanning.cep.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/17 18:01
 **/
@Getter
@Setter
@ToString
public class LogDO  implements Serializable {
    private Long id;

    private Long userId;

    private String username;

    private String operation;

    private Integer time;

    private String method;

    private String params;

    private String ip;

    /**
     * 日志类型，0：研发日志；1：产品日志；
     */
    private Integer type;

}
