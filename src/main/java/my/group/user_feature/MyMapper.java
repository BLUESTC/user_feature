package my.group.user_feature;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key ,value;
    Pattern sP=Pattern.compile("(?)|(!)|(...)|(？)|(！)|(。。。)");
    
    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputValueRecord();
        value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String uid = record.getString(0);
        String mid=record.getString(1);
        String status=record.getString(3);
        
        JsonObject json=new JsonObject();
        json.addProperty("uid", uid);
//        json.addProperty("blog", status);
        json.addProperty("hasTopic", status.contains("#")?1:0);
        json.addProperty("hasUrl", status.matches(".*(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?.*")?1:0);
        json.addProperty("hasAt", status.contains("@")?1:0);
        json.addProperty("hasUrl", status.matches(".*\\[\\da-z+\\].*")?1:0);
        

//        Matcher m=sP.matcher(status);
//        int ssSum=0;
//        while(m.find()){
//        	ssSum++;
//        }
//        json.addProperty("specialSigSum", ssSum);

        
        key.setString(0, mid);
//        one.setBigint(0, 0L);
//        one.setBigint(1, 0L);
//        one.setBigint(2, 0L);
        
//        one.setBigint(0,(long) Integer.parseInt(action));
//        if(action.equals("1")){
//        	one.setBigint(0, 1L);
//        }
//        else if(action.equals("2")){
//        	one.setBigint(0, 2L);
//        }
//       else if(action.equals("3")){
//        	one.setBigint(0, 3L);
//       }
//       
//        
//        
//        word.setString(0, mid);
        value.setString(0, json.toString());
        context.write(key, value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}
