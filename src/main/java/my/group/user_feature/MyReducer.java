package my.group.user_feature;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

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
            zhuan += val.getBigint(0);
            ping += val.getBigint(1);
            zan += val.getBigint(2);
            
        }
        result.set(0, zhuan);
        result.set(1, ping);
        result.set(2, zan);
        context.write(key, result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
