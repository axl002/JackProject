import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// read and print everything from poeapi.itemCount
public class GetDB {


    public static final RethinkDB r = RethinkDB.r;
    public static void main(String[] args){

        Connection conn = r.connection().hostname("35.166.62.31").port(28015).connect();
        conn.use("poeapi");
        Cursor foo = r.table("itemCount").filter(doc -> doc.g("id").match("Bow$")).run(conn);
        Cursor cursor = r.table("itemCount").run(conn);
        System.out.println(foo.toString());
        System.out.println("iterator created");

        for (Object doc : foo) {

            //String str = "ZZZZL <%= dsn %> AFFF <%= AFG %>";
            //System.out.println(str);
            //Pattern pattern = Pattern.compile("<<set:.+>><<set:.+>><<set:.+>>(.*?)");
//            String[] foober = doc.toString().split("<<set:.+>><<set:.+>><<set:.+>>(.*?)");
//
//            if(foober.length >1){
//                System.out.println(foober[1].split("}")[0]);
//            }


//            Matcher matcher = pattern.matcher(doc.toString());
//            //while (matcher.find()) {
//            matcher.find();
//                System.out.println(matcher.group(1));
//                System.out.println(matcher.group(0));


            System.out.println(doc.toString());
        }
        //cursor.close();
        conn.close();


    }
}
