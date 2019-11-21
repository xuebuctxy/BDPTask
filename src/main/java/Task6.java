import entity.Order;
import entity.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class Task6 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .master("local[*]")
                .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        String zk = "hadoopnode";
        String tableName = "user_order";
        conf.set("hbase.zookeeper.quorum", zk);
        try {
//            HTable table = new HTable(conf, tableName);
//            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
//            Job job = Job.getInstance(conf);
//            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//            job.setMapOutputValueClass(KeyValue.class);
//            HFileOutputFormat.configureIncrementalLoad(job, table);
            String userPath = "hdfs://hadoopnode:9000/warehouse/jddata.db/t_user";
            String orderPath = "hdfs://hadoopnode:9000/warehouse/jddata.db/t_order";
            JavaRDD<String> rdd1 = spark.read().textFile(userPath).javaRDD();
            JavaRDD<String> rdd2 = spark.read().option("header", true).textFile(orderPath).javaRDD();

            JavaRDD<User> userRdd = rdd1.map(line->{
                String parts[] = line.split(",");
                User user = new User();
                user.setUid(parts[0]);
                user.setAge(parts[1]);
                user.setSex(parts[2]);
                user.setActive_date(parts[3]);
                user.setLimit(parts[4]);
                return user;
            });
            Dataset<Row> userDs = spark.createDataFrame(userRdd, User.class);
            userDs.createOrReplaceTempView("t_user");

            JavaRDD<Order> orderRdd = rdd2.map(line->{
                String parts[] = line.split(",");

                Order order = new Order();
                order.setUid(parts[0]);
                order.setBuy_time(parts[1]);
                order.setPrice(parts[2]);
                order.setQty(parts[3]);
                order.setCate_id(parts[4]);
                order.setDiscount(parts[5]);
                return order;
            });

//            userDs.head(10);
            Dataset<Row> orderDs = spark.createDataFrame(orderRdd, Order.class);
            orderDs.createOrReplaceTempView("t_order");

            Dataset<Row> rs = spark.sql("select t_user.uid, t_user.age, t_user.sex, t_user.active_date, t_user.limit, " +
                    "t_order.buy_time, t_order.price, t_order.qty, t_order.cate_id, t_order.discount " +
                    "from t_user join t_order on t_user.uid=t_order.uid " +
                    "order by t_user.uid,t_order.buy_time");
            //方法一：批量写入hbase
//            rs.foreachPartition(p->{
//                Configuration hbaseconf = HBaseConfiguration.create();
//                hbaseconf.set("hbase.zookeeper.quorum",
//                        "hadoopnode");
//                hbaseconf.set("zookeeper.znode.parent", "/hbase");
//                Connection connection = ConnectionFactory.createConnection(hbaseconf);
//                //retrieve a handler to the target table
//                BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf("user_order"));
//                while(p.hasNext()){
//                    Row row = p.next();
//                    for(int i=0;i<1000;i++){
//                        String rowkey = (String) row.get(0)+"-"+ (String)row.get(5)+ i;
//                        Put put = new Put(Bytes.toBytes(rowkey));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("uid"), Bytes.toBytes((String)row.get(0)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes((String)row.get(1)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("sex"), Bytes.toBytes((String)row.get(2)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("active_date"), Bytes.toBytes((String)row.get(3)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("limit"), Bytes.toBytes((String)row.get(4)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("buy_time"), Bytes.toBytes((String)row.get(5)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("price"), Bytes.toBytes((String)row.get(6)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("qty"), Bytes.toBytes((String)row.get(7)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cate_id"), Bytes.toBytes((String)row.get(8)));
//                        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("discount"), Bytes.toBytes((String)row.get(9)));
//
//                        bufferedMutator.mutate(put);
//                    }
//                }
//                bufferedMutator.close();
//                connection.close();
//            });

            //方法二：利用bulk load hfile
            JavaPairRDD<ImmutableBytesWritable,KeyValue> hfileRdd = rs.toJavaRDD().zipWithIndex().flatMapToPair(new PairFlatMapFunction<Tuple2<Row, Long >, ImmutableBytesWritable, KeyValue>() {
                @Override
                public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Tuple2<Row, Long > tuple2) throws Exception {
                    Row row = tuple2._1;
                    String rowkey = (String) row.get(0)+"-"+(String) row.get(5)+"-"+String.format("%09d", tuple2._2);
                    Tuple2<ImmutableBytesWritable, KeyValue>[] cells = new Tuple2[10];
                    KeyValue keyValue3 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("active_date"), Bytes.toBytes((String)row.get(3)));
                    cells[0] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue3);
                    KeyValue keyValue1 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes((String)row.get(1)));
                    cells[1] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue1);
                    KeyValue keyValue5 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("buy_time"), Bytes.toBytes((String)row.get(5)));
                    cells[2] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue5);
                    KeyValue keyValue8 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("cate_id"), Bytes.toBytes((String)row.get(8)));
                    cells[3] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue8);
                    KeyValue keyValue9 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("discount"), Bytes.toBytes((String)row.get(9)));
                    cells[4] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue9);
                    KeyValue keyValue4 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("limit"), Bytes.toBytes((String)row.get(4)));
                    cells[5] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue4);
                    KeyValue keyValue6 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("price"), Bytes.toBytes((String)row.get(6)));
                    cells[6] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue6);
                    KeyValue keyValue7 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("qty"), Bytes.toBytes((String)row.get(7)));
                    cells[7] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue7);
                    KeyValue keyValue2 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("sex"), Bytes.toBytes((String)row.get(2)));
                    cells[8] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue2);
                    KeyValue keyValue0 = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("cf1"), Bytes.toBytes("uid"), Bytes.toBytes((String)row.get(0)));
                    cells[9] = new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue0);

                    return Arrays.asList(cells).iterator();
                }
            });

            String hfilePath = "hdfs://hadoopnode:9000/user_order.hfile";
            hfileRdd.saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, conf);

//            //利用bulk load hfile
            Configuration hbaseconf = HBaseConfiguration.create();
            hbaseconf.set("hbase.zookeeper.quorum",
                        "hadoopnode");
            hbaseconf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);
            hbaseconf.set("zookeeper.znode.parent", "/hbase");
            Connection connection = ConnectionFactory.createConnection(hbaseconf);
            HTable table = new HTable(hbaseconf, "user_order");
            LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(hbaseconf);
            bulkLoader.doBulkLoad(new Path(hfilePath), table);
            spark.stop();
        }catch (Exception e){
            System.out.println(e);
        }
    }
}
