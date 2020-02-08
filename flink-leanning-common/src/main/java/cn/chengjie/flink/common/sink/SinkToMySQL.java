package cn.chengjie.flink.common.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Desc: 数据批量 sink 数据到 mysql
 * Created by yuanblog_tzs on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/2019/01/09/Flink-MySQL-sink/
 */
@Slf4j
public class SinkToMySQL extends RichSinkFunction<Tuple3<String, Integer, Double>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        connection.setAutoCommit(true);
        String sql = "insert into sys_statistic_log(`operation`, `sum_count`, `avg_time`) values(?, ?, ?) ON DUPLICATE KEY UPDATE  `sum_count`=?, `avg_time`=?, `version`=`version`+1";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Tuple3<String, Integer, Double> value, Context context) throws Exception {

        if (ps == null) {
            return;
        }
        ps.setString(1, value.f0);
        ps.setInt(2, value.f1);
        ps.setDouble(3, value.f2);
        ps.setInt(4, value.f1);
        ps.setDouble(5, value.f2);
        int count = ps.executeUpdate();
        log.info("成功了插入了 {} 行数据", count);
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://10.40.6.180:3306/global_bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
