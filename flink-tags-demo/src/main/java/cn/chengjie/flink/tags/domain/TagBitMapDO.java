package cn.chengjie.flink.tags.domain;

import lombok.Getter;
import lombok.Setter;
/**
 * 商品标签实体类
 * @author ChengJie
 * @desciption
 * @date 2019/8/19 14:34
 **/
@Getter
@Setter
public class TagBitMapDO  implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 标签Id
     */
    Long tagId;

    /**
     * 标签名
     */
    Long  ruleId;

    /**
     * 规则名称
     */
    String ruleName;

    /**
     * 事业部编码
     */
    String buCode;

    /**
     * 标签规则编码
     */
    String  code;

//    RoaringBitmap bitmap;

    String bitmapString;
    /**
     * 创建日期
     */
    String createDate;

    Integer order;


}
