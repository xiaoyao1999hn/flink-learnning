package cn.chengjie.flink.common.domain;

import lombok.Getter;
import lombok.Setter;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/5 15:50
 **/
@Getter
@Setter
public class LogStatisticDO {

    String operation;

    Integer sumCount;

    Double avgTime;

    Integer version;

}
