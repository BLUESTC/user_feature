package my.group.user_feature;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record word;
    private Record one;

    public void setup(TaskContext context) throws IOException {
        word = context.createMapOutputKeyRecord();
        one = context.createMapOutputValueRecord();
        one.setBigint(0, 1L);
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String uid = record.getString(2);
        String mid=record.getString(3);
        String action=record.getString(5);
        
//        one.setBigint(0, 0L);
//        one.setBigint(1, 0L);
//        one.setBigint(2, 0L);
        
        one.setBigint(0,(long) Integer.parseInt(action));
        if(action.equals("1")){
        	one.setBigint(0, 1L);
        }
        else if(action.equals("2")){
        	one.setBigint(0, 2L);
        }
       else if(action.equals("3")){
        	one.setBigint(0, 3L);
       }
       
        
        
        word.setString(0, mid);

        context.write(word, one);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}
