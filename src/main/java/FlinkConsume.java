import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class FlinkConsume{


    public static Properties props;

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        // setProperties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "zookeeper");
        String topic = "poe2";
        String topicOut = "poe3";



        // source
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer09<String>(topic, new SimpleStringSchema(), props));


        // map
        stream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;


            public String map(String value) throws Exception {
                return "Stream Value: " + value;
            }
        });
        // sink
        stream.addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), props));

        env.execute();
    }

}
