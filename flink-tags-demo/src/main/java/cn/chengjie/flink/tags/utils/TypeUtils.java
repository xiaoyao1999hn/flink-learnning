package cn.chengjie.flink.tags.utils;

import cn.chengjie.flink.common.utils.DbUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.zookeeper.Op;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/7 15:46
 **/
public class TypeUtils {

    public static RowTypeInfo createRowTypeInfo(List<DbUtils.Column> cols){
        if(cols!=null&&cols.size()>0){
            List<BasicTypeInfo> list=cols.stream().map(x->{
                switch (x.getJdbcType()){
                    case TIMESTAMP:
                    case DATE: return BasicTypeInfo.STRING_TYPE_INFO;
                    case INTEGER: return BasicTypeInfo.INT_TYPE_INFO;
                    case BIGINT:return BasicTypeInfo.BIG_INT_TYPE_INFO;
                    case FLOAT:return BasicTypeInfo.FLOAT_TYPE_INFO;
                    case DOUBLE:return BasicTypeInfo.DOUBLE_TYPE_INFO;
                    case DECIMAL:return BasicTypeInfo.BIG_DEC_TYPE_INFO;
                    case NVARCHAR:
                    case LONGNVARCHAR:
                    case VARCHAR:return BasicTypeInfo.STRING_TYPE_INFO;
                    case BOOLEAN:return BasicTypeInfo.BOOLEAN_TYPE_INFO;
                    default:return BasicTypeInfo.STRING_TYPE_INFO;
                }
            }).collect(Collectors.toList());
            return new RowTypeInfo(list.toArray(new BasicTypeInfo[list.size()]));
        }else {
            return new RowTypeInfo();
        }
    }

}
