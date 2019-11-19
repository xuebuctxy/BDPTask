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
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

public class Task6 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .master("local")
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

            Dataset<Row> rs = spark.sql("select concat(t_user.uid,'-',buy_time,'-',row_number() over(partition by t_order.uid order by buy_time)) as rowkey," +
                    "t_user.uid, t_user.age, t_user.sex, t_user.active_date, t_user.limit, " +
                    "t_order.buy_time, t_order.price, t_order.qty, t_order.cate_id, t_order.discount " +
                    "from t_user join t_order on t_user.uid=t_order.uid " +
                    "order by t_user.uid,t_order.buy_time limit 10");

            rs.foreachPartition(p->{
                Configuration hbaseconf = HBaseConfiguration.create();
                hbaseconf.set("hbase.zookeeper.quorum",
                        "hadoopnode");
                hbaseconf.set("zookeeper.znode.parent", "/hbase");
                Connection connection = ConnectionFactory.createConnection(hbaseconf);
                //retrieve a handler to the target table
                Table table = connection.getTable(TableName.valueOf("user_order"));
                while(p.hasNext()){
                    Row row = p.next();
                    Put put = new Put(Bytes.toBytes((String) row.get(0)));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("uid"), Bytes.toBytes((String)row.get(1)));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes((String)row.get(2)));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("sex"), Bytes.toBytes((String)row.get(3)));
//                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_1), Bytes.toBytes(0));
//                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_2), Bytes.toBytes(0));
                    //send the data
                    table.put(put);
                }
                connection.close();
            });
            spark.stop();
//            JavaPairRDD<ImmutableBytesWritable,KeyValue> hfileRdd = lines.mapToPair(new PairFunction<String, ImmutableBytesWritable, KeyValue>() {
//                public Tuple2<ImmutableBytesWritable, KeyValue> call(String v1) throws Exception {
//                    String[] tokens = v1.split(" ");
//                    String rowkey = tokens[0];
//                    String content = tokens[1];
//                    KeyValue keyValue = new KeyValue(Bytes.toBytes(rowkey), Bytes.toBytes("article"), Bytes.toBytes("value"), Bytes.toBytes(content));
//                    return new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), keyValue);
//                }
//            });
//            String hfilePath = "hdfs://hadoopnode:9000/user_order.hfile";
//            hfileRdd.saveAsNewAPIHadoopFile(hfilePath, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat.class, conf);
//
//            //利用bulk load hfile
//            LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(conf);
//            bulkLoader.doBulkLoad(new Path(hfilePath), table);

        }catch (Exception e){
            System.out.println(e);
        }
    }
}
