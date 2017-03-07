/**
 * Created by akmal.muqeeth on 2/22/17.
 */

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.avro.Schema;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by akmal.muqeeth on 2/22/17.
 */
public class ConfluentProducerApp {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("bootstrap.servers", "localhost:9092");


        KafkaProducer myProducer = new KafkaProducer(props);

        final String topic = "test";

        String key = "key1";
        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[" +
                "{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value2");

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(topic, key, avroRecord);
        try {
            myProducer.send(record);
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

    }


}
