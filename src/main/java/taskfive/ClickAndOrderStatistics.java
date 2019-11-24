package taskfive;

import common.ConfigUtil;
import common.JdbcUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Administrator on 2019/11/9.
 */
public class ClickAndOrderStatistics {


    public static void main(String[] args) throws Exception{
        SparkConf conf=new SparkConf().setAppName("ClickAndOrderStatistics");
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        if(args.length>0){
            conf.setMaster(args[0]);
        }else{
            conf.setMaster("local[2]");
        }

        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(10));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", ConfigUtil.getProperties("kafka.server"));
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");
        //kafkaParams.put("auto.offset.reset", "largest"); //smallest
        kafkaParams.put("group.id", ConfigUtil.getProperties("streaming.cousumer.group"));

        Set<String> clickTopicSet=new HashSet<String>();
        clickTopicSet.add(ConfigUtil.getProperties("kafka.click.topic"));
        Set<String> orderTopicSet=new HashSet<String>();
        orderTopicSet.add(ConfigUtil.getProperties("kafka.order.topic"));
        //统计用户点击次数
        clickStatistic(jsc,kafkaParams,clickTopicSet);
        //统计不同年龄段消费总额
        orderStatistic(jsc,kafkaParams,orderTopicSet);

        jsc.start();
        jsc.awaitTermination();

    }

    /**
     * 统计每个页面的累计点击次数
     * @param jsc
     * @param kafkaParams
     * @param topicSet
     */
    public static void clickStatistic(JavaStreamingContext jsc,Map<String,String> kafkaParams,Set<String> topicSet){
        //接收kafka点击消息
        JavaPairInputDStream<String,String> clickInputStream=KafkaUtils.createDirectStream(jsc,String.class,String.class, StringDecoder.class,StringDecoder.class,kafkaParams,topicSet);
        //统计每个页面的点击次数
        JavaPairDStream<String,Integer> clickDStream = clickInputStream.mapToPair(x->{
            String[] values=x._2().split(",");
            return new Tuple2<String,Integer>(values[1],1);
        }).reduceByKey((v1,v2)->v1+v2);
        //更新每个页面的点击次数
        clickDStream.foreachRDD(rdd->{
            rdd.foreachPartition(p->{
                Connection connection= JdbcUtil.getConnection();
                connection.setAutoCommit(false);
                while(p.hasNext()){
                    Tuple2<String,Integer> clickRecord=p.next();
                    String sql="select result_value from streaming_result where result_key='click+"+clickRecord._1+"' for update";
                    ResultSet clickCount=JdbcUtil.executeQuery(connection,sql);
                    if(clickCount.next()){
                        int count=clickCount.getInt(1)+clickRecord._2;
                        sql="update streaming_result set result_value='"+count+"' where result_key='click+"+clickRecord._1+"'";
                    }else{
                        sql="insert into streaming_result(result_key,result_value) values('click+"+clickRecord._1+"','"+clickRecord._2+"')";
                    }
                    JdbcUtil.executeUpdate(connection,sql);
                    connection.commit();
                }
                JdbcUtil.closeConnection(connection);
            });
        });
    }

    /**
     * 统计每个年龄段的消费总金额
     * @param jsc
     * @param kafkaParams
     * @param topicSet
     */
    public static void orderStatistic(JavaStreamingContext jsc,Map<String,String> kafkaParams,Set<String> topicSet){
        //接收kafka订单消息
        JavaPairInputDStream<String,String> clickInputStream=KafkaUtils.createDirectStream(jsc,String.class,String.class, StringDecoder.class,StringDecoder.class,kafkaParams,topicSet);
        //统计每个uid的消费金额
        JavaPairDStream<String,BigDecimal> clickDStream = clickInputStream.mapToPair(x->{
            String[] values=x._2().split(",");
            return new Tuple2<String,BigDecimal>(x._1,new BigDecimal(values[1]).subtract(new BigDecimal(values[2])));
        }).reduceByKey((v1,v2)->v1.add(v2));
        //对每个年龄段的消费金额进行更新
        clickDStream.foreachRDD(rdd->{
           /* //查询用户信息
            Map<String,Integer> userMap=new HashMap<String,Integer>();
            Connection connection1= JdbcUtil.getConnection();
            ResultSet rs=JdbcUtil.executeQuery(connection1,"select * from t_user ");
            while(rs.next()){
                userMap.put(rs.getString(1),rs.getInt(2));
            }
            JdbcUtil.closeConnection(connection1);
            //广播用户信息到各个执行节点
            Broadcast<Map<String,Integer>> userList=jsc.sparkContext().broadcast(userMap);*/

            rdd.foreachPartition(p->{
                Connection connection= JdbcUtil.getConnection();
                connection.setAutoCommit(false);
                //累计每个年龄段的消费金额
                Map<Integer,BigDecimal> ageSumMap=new HashMap<Integer,BigDecimal>();
                while(p.hasNext()){
                    Tuple2<String,BigDecimal> orderRecord=p.next();
                    //查询用户年龄信息
                    ResultSet rs=JdbcUtil.executeQuery(connection,"select age from t_user where uid='"+orderRecord._1+"'");
                    int age=0;
                    if(rs.next()){
                        age=rs.getInt(1);
                    }
                    if(ageSumMap.containsKey(age)){
                        ageSumMap.put(age,ageSumMap.get(age).add(orderRecord._2));
                    }else {
                        ageSumMap.put(age,orderRecord._2);
                    }
                }
                //更新每个年龄段的消费总金额
                for(int age:ageSumMap.keySet()){
                    String sql="select result_value from streaming_result where result_key='buy+"+age+"' for update";
                    ResultSet clickCount=JdbcUtil.executeQuery(connection,sql);
                    if(clickCount.next()){
                        BigDecimal amount=clickCount.getBigDecimal(1).add(ageSumMap.get(age));
                        sql="update streaming_result set result_value='"+amount+"' where result_key='buy+"+age+"'";
                    }else{
                        sql="insert into streaming_result(result_key,result_value) values('buy+"+age+"','"+ageSumMap.get(age)+"')";                     }
                    JdbcUtil.executeUpdate(connection,sql);
                    connection.commit();
                }

                JdbcUtil.closeConnection(connection);
            });
        });
    }


}
