package com.zdjizhi.base.utils;//package com.zdjizhi.base.utils;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.types.Row;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.util.List;
//
//public class ClickhouseSink extends RichSinkFunction<List<Row>> {
//
//    Connection connection = null;
//    String sql;
//
//    private String[] tableColums;
//    private  String[] types;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//
//        String sql = "INSERT INTO user2 (name, address, age) VALUES (?,?,?)";
//        tableColums = new String[]{"name", "address", "age"};
//        types = new String[]{"string", "string", "int"};
//        ClickhouseUtils instance = ClickhouseUtils.getInstance();
//        connection = instance.getConnection();
//    }
//
//    @Override
//    public void close() throws Exception {
//        super.close();
//        connection.close();
//    }
//
//    @Override
//    public void invoke(List<Row> value, Context context) throws Exception {
//        PreparedStatement preparedStatement = connection.prepareStatement(sql);
//        for (Row e : value) {
//            int length = tableColums.length;
//            for (int i = 0; i < length; i++) {
//                String type = types[i];
//                if (e.getField(i) != null) {
//                    switch (type) {
//                        case "string":
//                            preparedStatement.setString(i + 1, (String) e.getField(i));
//                            break;
//                        case "int":
//                            preparedStatement.setInt(i + 1, (int) e.getField(i));
//                            break;
//                        default:
//                            break;
//                    }
//                } else {
//                    preparedStatement.setObject(i + 1, null);
//                }
//            }
//            preparedStatement.addBatch();
//        }
//        long startTime = System.currentTimeMillis();
//        int[] ints = preparedStatement.executeBatch();
//        connection.commit();
//        long endTime = System.currentTimeMillis();
//        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
//    }
//}
