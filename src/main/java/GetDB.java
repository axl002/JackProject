import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// read and print everything from poeapi.itemCount
public class GetDB {


    public static final RethinkDB r = RethinkDB.r;
    public static void main(String[] args){
        String tableName = "lookUp";
        Connection conn = r.connection().hostname("35.166.62.31").port(28015).connect();
        conn.use("poeapi");
        //Cursor foo = r.table(tableName).filter(doc -> doc.g("id").match("Bow$")).run(conn);
        Cursor cursor = r.table(tableName).run(conn);
        //System.out.println(foo.toString());
        System.out.println("iterator created");

        for( Object item : cursor){
            System.out.println(item);
        }
        //for (Object doc : foo) {

            //String str = "ZZZZL <%= dsn %> AFFF <%= AFG %>";
            //System.out.println(str);
//            String test1 = "{\"name\": " + "\""+ "stuff"+ "\""
//        + ", \"avgPrice\": "+ 2D
//                + ", \"STD\": " + 3D
//                + ", \"threshold\": " + 4D
//                +"}";
//        ObjectMapper om = new ObjectMapper();
//        JsonNode jn;
//        try{
//            jn = om.readTree(test1);
//            System.out.println(jn.toString());
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//            String test2 = "~b/o 19.17 chaos";
//            Pattern pattern = Pattern.compile("[0-9]+.*,*[0-9]*\\s");

//            String[] foober = doc.toString().split("<<set:.+>><<set:.+>><<set:.+>>(.*?)");
//
//            if(foober.length >1){
//                System.out.println(foober[1].split("}")[0]);
//            }

//
//            Matcher matcher = pattern.matcher(test1);
////            //while (matcher.find()) {
//            matcher.find();
////                System.out.println(matcher.group(1));
//                System.out.println(matcher.group(0));
//        matcher = pattern.matcher(test2);
//
//        matcher.find();
//        System.out.println(matcher.group(0));

            //System.out.println(doc.toString());
        //}
        //cursor.close();
        //conn.close();


    }
}
