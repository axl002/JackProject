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
        Cursor cursor = r.table("itemCount").run(conn);
        for (Object doc : cursor) {

            //String str = "ZZZZL <%= dsn %> AFFF <%= AFG %>";
            Pattern pattern = Pattern.compile("##@(.*?)@##");
            Matcher matcher = pattern.matcher(doc.toString());
            while (matcher.find()) {
                System.out.println(matcher.group(1));
            }
            System.out.println(doc.toString());
        }
        cursor.close();
        conn.close();


    }
}
