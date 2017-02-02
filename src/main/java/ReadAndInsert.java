import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlOpFailedError;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;


// read data from kafka topic
// insert into rethink db
public class ReadAndInsert {



    public static final RethinkDB r = RethinkDB.r;
    private static KafkaConsumer<String, String> consumer;
    public static void main(String[] args){

        Properties props = new Properties();
        // THIS WORKS NOW
        String topic = "poe3";
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "zookeeper"); // need to test if zookeeper is required group, it works but do other groups work?
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("fetch.message.max.bytes","100000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);

        Connection conn = r.connection().hostname("35.166.62.31").port(28015).connect();

        try {
            r.db("test").tableCreate("poeapi").run(conn);
        }
        catch (ReqlOpFailedError oops){
            // don't die if table no exist
            System.out.println("table already exists");

            consumeLoop(conn);

//            Cursor cursor = r.table("tv_shows").run(conn);
//            for (Object doc : cursor) {
//                System.out.println(doc);
//            }
        }
        consumeLoop(conn);
        conn.close();
    }

    // loop to consume poe3 topic and insert to rethinkdb
    private static void consumeLoop(Connection conn){

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                r.table("poeapi").insert(r.hashMap(record.key(), record.value())).run(conn);
            }
        }
    }



}