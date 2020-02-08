package cn.chengjie.flink.tags.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/15 17:37
 **/
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TagInfoDO {

    String sku;

    String buCode;

    String buName;

    String ruleName;

    String code;

    String theDateCd;

    Double labelValue;

}
