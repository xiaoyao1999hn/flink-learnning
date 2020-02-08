package cn.chengjie.flink.tags.domain;

import lombok.Getter;
import lombok.Setter;
import java.util.List;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/7 16:46
 **/
@Getter
@Setter
public class TagRuleRationDO implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    String relationType;

    String ruleType;

    List<TagRuleRationDO> rules;

    String sql;
}
