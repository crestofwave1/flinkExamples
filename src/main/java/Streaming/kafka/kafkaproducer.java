package Streaming.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class kafkaproducer {
    public static void main(String [] args) throws JsonProcessingException {
        final ObjectMapper MAPPER = new ObjectMapper();
        Properties props = new Properties();
        props.put("bootstrap.servers", "meitudeMacBook-Pro-4.local:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
        java.util.Random level=new java.util.Random(1);

        java.util.Random points =new java.util.Random(2);

        java.util.Random index =new java.util.Random(2);

        List<String> nameList = new ArrayList<String>();
        nameList.add("bob");
        nameList.add("mike");
        nameList.add("jake");
        nameList.add("cat");
        nameList.add("dog");
        nameList.add("hive");
        nameList.add("dig");



        for(int i = 0; i < 10000; i++){


            JSONObject jsonObject = new JSONObject();
//
            if(i % 2 == 0){
//                String value = "bob"+","+points.nextInt(100)+","+System.currentTimeMillis();
//            jsonObject.put("word",nameList.get(index.nextInt(6)));
//
            jsonObject.put("word","bob");
            jsonObject.put("frequency",points.nextInt(100) );
            jsonObject.put("timestamp",System.currentTimeMillis());
            producer.send(new ProducerRecord<>("foo", jsonObject.toString().getBytes()));
            }else{
//                String value = "kaka"+","+points.nextInt(100)+","+System.currentTimeMillis();
            jsonObject.put("word","kaka");
            jsonObject.put("frequency",points.nextInt(100) );
            jsonObject.put("timestamp",System.currentTimeMillis());
                producer.send(new ProducerRecord<>("foo", jsonObject.toString().getBytes()));
            }

            try {
                sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        producer.close();
    }
}