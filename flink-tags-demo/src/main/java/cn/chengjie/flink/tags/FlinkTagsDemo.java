package cn.chengjie.flink.tags;

import lombok.extern.slf4j.Slf4j;
/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/6 18:07
 **/
@Slf4j
public class FlinkTagsDemo {

//    private static final String[] tableName = {
////            "dm_dss_b2b_goods_lable_f",
////            "dm_dss_dz_goods_lable_f",
////            "dm_dss_fx_goods_lable_f",
//            "dm.dm_dss_fz_goods_lable_f",
////            "dm_dss_auxdsf_goods_lable_f",
////            "dm_dss_fz_spu_lable_f"
//    };
//
//
//    public static void main(String[] args) throws Exception {
//
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
//
//        Connection connection = DbUtils.getConnection("jdbc:hive2://10.68.1.65:10000", "bigdata", "bigdata");
//        Boolean close = false;
//
//        for (int i = 0; i < tableName.length; i++) {
//
//            List<DbUtils.Column> cols = DbUtils.getAllColumns(connection, tableName[i], close);
//            StringBuffer colNames = new StringBuffer();
//            cols.forEach(x -> colNames.append(x.getColumnName().split("\\.")[1]).append(","));
//            colNames.deleteCharAt(colNames.lastIndexOf(","));
//
//            if (i == tableName.length - 1) {
//                close = true;
//            }
//            JDBCInputFormat formats = JDBCInputFormat.buildJDBCInputFormat()
//                    .setDBUrl("jdbc:hive2://10.68.1.65:10000")
//                    .setDrivername("org.apache.hive.jdbc.HiveDriver")
//                    .setUsername("bigdata")
//                    .setPassword("bigdata")
//                    .setQuery("select " + colNames + " from " + tableName[i] + " where the_date_cd='20191208' ")
//                    .setRowTypeInfo(TypeUtils.createRowTypeInfo(cols))
//                    .finish();
//
//            DataSet<Row> dataSet = env.createInput(formats);
//
//            Table table = tableEnv.fromDataSet(dataSet, colNames.toString());
//
//            tableEnv.registerTable(tableName[i].split("\\.")[1], table);
//        }
//
//        BasicTypeInfo[] typeInfos = {
//                BasicTypeInfo.LONG_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.INT_TYPE_INFO
//        };
//
//        JDBCInputFormat formats = JDBCInputFormat.buildJDBCInputFormat()
//                .setDBUrl("jdbc:mysql://10.40.6.180:3306/test?useUnicode=true&characterEncoding=utf-8&autoReconnect=true")
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setUsername("root")
//                .setPassword("123456")
//                .setQuery("SELECT t1.source_id,t1.`name`,t1.`code`, t2.bu_code,t1.rule_content_sql_list,t1.order FROM compute_rule t1  LEFT JOIN tags_info t2 ON t1.source_id=t2.id  WHERE source_id=27 ORDER BY t1.source_id, t1.`order` ")
//                .setRowTypeInfo(new RowTypeInfo(typeInfos))
//                .finish();
//
//        List<List<Row>> list = env.createInput(formats).groupBy(0).reduceGroup(new RichGroupReduceFunction<Row, List<Row>>() {
//            @Override
//            public void reduce(Iterable<Row> values, Collector<List<Row>> out) throws Exception {
//                out.collect(IteratorUtils.toList(values.iterator()));
//
//            }
//        }).collect();
//
//
//
//
//        list.forEach(rows -> {
//
//            List<RoaringBitmap> maps = new ArrayList<>();
//
//            rows.forEach(row -> {
//                TagBitMapDO bitMapDO = new TagBitMapDO();
//                bitMapDO.setTagId(Long.parseLong(row.getField(0).toString()));
////                bitMapDO.setRuleId(Long.parseLong(row.getField(0).toString()));
//                bitMapDO.setRuleName(row.getField(1).toString());
//                bitMapDO.setCode(row.getField(2).toString());
//                bitMapDO.setBuCode(row.getField(3).toString());
//                bitMapDO.setCreateDate(DateUtils.format(new Date()));
//                bitMapDO.setOrder(Integer.parseInt(row.getField(5).toString()));
//                TagRuleRationDO ruleRation = JSONObject.parseObject(row.getField(4).toString(), TagRuleRationDO.class);
//                RoaringBitmap roaringBitmap = null;
//                try {
//                    roaringBitmap = compute(tableEnv, ruleRation);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                for(RoaringBitmap tempMap:maps){
//                    roaringBitmap.andNot(tempMap);
//                }
//                roaringBitmap.runOptimize();
//                maps.add(roaringBitmap);
//                bitMapDO.setBitmapString(StringUtils.toHexString(BitMapUtil.serialize(roaringBitmap)));
//                System.out.println("标签【"+bitMapDO.getTagId()+"】,规则【"+bitMapDO.getRuleName()+"】数量："+roaringBitmap.getCardinality());
//            });
//
//        });
//    }
//
//
//    public static RoaringBitmap compute(BatchTableEnvironment tableEnv, TagRuleRationDO ruleRation) throws Exception {
//        RoaringBitmap bitmap = new RoaringBitmap();
//        if (ruleRation.getRules() != null && ruleRation.getRules().size() > 0) {
//            for (TagRuleRationDO temp : ruleRation.getRules()) {
//                RoaringBitmap tempBitmap = new RoaringBitmap();
//                if (temp.getRuleType().equals("RELATION")) {
//                    tempBitmap = compute(tableEnv, temp);
//                } else {
//                    tempBitmap = getData(tableEnv, temp.getSql());
//                }
//                switch (ruleRation.getRelationType()) {
//                    case "AND":
//                        bitmap.and(tempBitmap);
//                        break;
//                    case "OR":
//                        bitmap.or(tempBitmap);
//                        break;
//                }
//            }
//        }
//        return bitmap;
//    }
//
//    public static RoaringBitmap getData(BatchTableEnvironment tableEnv, String sql) throws Exception {
//        for(String name:tableName){
//            if(sql.contains(name)){
//                sql=sql.replace(name,name.split("\\.")[1]);
//            }
//        }
//        RoaringBitmap roaringBitmap = new RoaringBitmap();
//        System.out.println(sql);
//        tableEnv.toDataSet(tableEnv.sqlQuery(sql), new TypeHint<BigDecimal>() {}.getTypeInfo()).collect().forEach(x -> roaringBitmap.add(x.intValue()));
//
//        roaringBitmap.runOptimize();
//        return roaringBitmap;
//    }
//
//
//    /**
//     * 保存分层bitmap数据到es
//     *
//     * @param dataDate
//     * @param tagBitMapDO
//     * @return
//     */
//    private boolean saveTagBitMap(LocalDate dataDate, TagBitMapDO tagBitMapDO, String esIndex, String esType) throws Exception {
//
//        String esUrl = "http://10.4.4.246:9200";
//        JestClientFactory factory = new JestClientFactory();
//        factory.setHttpClientConfig(new HttpClientConfig
//                .Builder(esUrl)
//                .gson(new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create())
//                .multiThreaded(true)
//                .readTimeout(30000)
//                .build());
//        //9069,9077
//        JestClient client = factory.getObject();
//
//        tagBitMapDO.setCreateDate(dataDate.format(DateTimeFormatter.ISO_DATE));
//        Index.Builder builder = new Index.Builder(tagBitMapDO);
//        if (null != tagBitMapDO.getTagId() && !"".equals(tagBitMapDO.getTagId())) {
//            builder.id(tagBitMapDO.getTagId().toString() + "-" + tagBitMapDO.getRuleId() + "-" + tagBitMapDO.getCreateDate());
//        }
//        builder.refresh(true);
//        Index indexDoc = builder.index(esIndex).type(esType).build();
//        JestResult result;
//        try {
//            result = client.execute(indexDoc);
//            if (result != null && result.isSucceeded()) {
//                return Boolean.TRUE;
//            }
//            throw new RuntimeException(result.getErrorMessage());
//        } catch (Exception e) {
//            e.printStackTrace();
//            log.error("save to es error: {}", e.getMessage());
//            throw e;
//        }
//    }
}
