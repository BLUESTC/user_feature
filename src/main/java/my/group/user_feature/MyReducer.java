package my.group.user_feature;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;


/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        long zhuan = 0,ping=0,zan=0;
        while (values.hasNext()) {
            Record val = values.next();
            if(val.getBigint(0)==0)
            zhuan += 1;
            if(val.getBigint(0)==1)
            ping += 1;
            if(val.getBigint(0)==2)
            zan += 1;
        }
//        HashMap<String,Long> map=new HashMap<String, Long>();
//        map.put("repost", zhuan);
//        map.put("comment", ping);
//        map.put("like", zan);
//        result.set(0, "{repost:"+zhuan+",comment:"+ping+",like:"+zan+"}");
//        result.setBigint(0, zhuan);
        context.write(key, "{repost:"+zhuan+",comment:"+ping+",like:"+zan+"}");
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
