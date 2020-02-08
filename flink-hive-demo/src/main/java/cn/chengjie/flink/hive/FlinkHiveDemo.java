package cn.chengjie.flink.hive;

import cn.chengjie.flink.common.enums.DbTypeEnum;
import cn.chengjie.flink.common.utils.DbUtils;
import cn.chengjie.flink.common.utils.HdfsConfigUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.sql.Connection;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/6 18:07
 **/
public class FlinkHiveDemo {

    private static final String[] tableName={"dm_dss_b2b_goods_lable_f","dm_dss_dz_goods_lable_f",
            "dm_dss_dz_goods_lable_f","dm_dss_fx_goods_lable_f",
            "dm_dss_auxdsf_goods_lable_f","dm_dss_fz_spu_lable_f"};



    public static void main(String[] args) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes = new TypeInformation[]{
                 INT_TYPE_INFO
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDBUrl("jdbc:hive2://10.68.1.65:10000/dm")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("bigdata")
                .setPassword("bigdata")
                .setQuery("select id from dm_dss_b2b_goods_lable_f")
                .setRowTypeInfo(rowTypeInfo)
                .finish();



    }
}
