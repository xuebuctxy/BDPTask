package taskfive;


import common.ConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class ClickAndOrderProducer {
    public static void main(String[] args) {
        //读取kafka及topic配置
        String brokers = ConfigUtil.getProperties("kafka.server");
        String clickTopic = ConfigUtil.getProperties("kafka.click.topic");
        String orderTopic = ConfigUtil.getProperties("kafka.order.topic");

        //kafka生产者配置
        Map<String, String> props = new HashMap<String, String>();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer(props);
        //hdfs配置
        Configuration conf = new Configuration();
        conf.set("fs.default.name", ConfigUtil.getProperties("hdfs.server"));

        try {

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //发送点击数据
                        sendClickData(producer, conf, ConfigUtil.getProperties("click.data.path"), clickTopic);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        //发送订单数据
                        sendOrderData(producer, conf, ConfigUtil.getProperties("order.data.path"), orderTopic);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendClickData(KafkaProducer producer, Configuration conf, String filePath, String topic) throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream data = fileSystem.open(new Path(filePath));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(data));
        String line = null;
        int i = 0;
        while ((line = bufferedReader.readLine()) != null) {
            if (i == 0) {
                i++;
                continue;
            }
            String[] fields = line.split(",");
            // produce event message
            producer.send(new ProducerRecord(topic, fields[0], fields[1] + "," + fields[2]));
            System.out.println(fields[0] + ":" + fields[1] + "," + fields[2]);
            //每10秒发送一条
            Thread.sleep(10000);
        }
        bufferedReader.close();
        data.close();
        fileSystem.close();
    }

    public static void sendOrderData(KafkaProducer producer, Configuration conf, String filePath, String topic) throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream data = fileSystem.open(new Path(filePath));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(data));
        String line = null;
        int i = 0;
        while ((line = bufferedReader.readLine()) != null) {
            if (i == 0) {
                i++;
                continue;
            }
            String[] fields = line.split(",");
            // produce event message
            producer.send(new ProducerRecord(topic, fields[0], fields[0] + "," + fields[2]+ "," + fields[5]));
            System.out.println(fields[0] + ":" + fields[0] + "," + fields[2]+ "," + fields[5]);
            //每10秒发送一条
            Thread.sleep(10000);
        }
        bufferedReader.close();
        data.close();
        fileSystem.close();
    }

}
