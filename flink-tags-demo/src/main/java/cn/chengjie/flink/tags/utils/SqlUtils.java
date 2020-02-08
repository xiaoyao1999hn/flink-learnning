package cn.chengjie.flink.tags.utils;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/15 16:38
 **/
public class SqlUtils {

//    final String[] ruleNames = {"重点款","热销款","未上架","新品","滞销款","呆滞款","平销款","无动销"};

    public static final String SQL_TEMPLATE ="SELECT sku ,bu_code,bu_name,label_code,label_name,label_value FROM  dm_dss_fx_goods_label_f WHERE %s";

    public static final String SQL0 = "(label_value > 0 AND label_value <= 0.30 AND label_name = '近30天真实销售额排名百分比') OR (label_value >= 10000 AND label_name = '近30天真实销售额')";

    public static final String SQL1 = "(label_value > 0.30 AND label_value <= 0.50 AND label_name = '近30天真实销售额排名百分比') OR (label_value >= 4000 AND label_name = '近30天真实销售额')";

    public static final String SQL2 = "(label_value > 0  AND (label_name = '环球库存数量' OR label_name = '近30天真实销售额')) AND (label_value = -1  AND label_name = 'CB网站首次上架天数')";

    public static final String SQL3 = "label_value <= 90 AND label_value > -1 AND label_name = 'CB网站首次上架天数'";

    public static final String SQL4 = "(label_value > 30  AND label_name = '加权平均库龄') AND ( label_value > 0 AND label_name = '近30天真实销量') AND (label_value > 90 AND label_name = '销售周转天数') AND (label_value > 90 AND label_name = 'CB网站首次上架天数')";

    public static final String SQL5 = "(label_value > 30 AND label_name = '加权平均库龄') AND (label_value = 0 AND label_name = '近30天真实销量') AND (label_value > 90 AND label_name = 'CB网站首次上架天数')";

    public static final String SQL6 = "(label_value > 90  AND label_name = 'CB网站首次上架天数') AND (label_value > 0  AND (label_name = '近30天真实销售额' OR label_name = '环球库存数量'))";

    public static final String SQL7 = "(label_value = 0  AND label_name = '近30天真实销量' AND label_name = '环球库存数量')";


}
