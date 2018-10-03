import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerBought {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text wordKey = new Text();
    private Text wordValue = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] tmp = value.toString().split(",");
      for(String a : tmp){
        wordKey.set(a);
        for(String b: tmp){
          if(!b.equals(a)){
            wordValue.set(b);
            context.write(wordKey,wordValue);
          }
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int nbMax = 0;
      String booksMax = "";
      Text val = new Text();
      Map<String, Integer> count = new HashMap<String, Integer>();
      
      for(Text vals : values){
        if(!count.containsKey(vals.toString())){
          count.put(vals.toString(), 1);
          if(nbMax < 1){
            nbMax = 1;
          }
        }
        else{
          int tmp = count.get(vals.toString());
          count.put(vals.toString(), tmp+1);
          if(tmp + 1 > nbMax){
            nbMax = tmp + 1;  
          }
        }
      }
     for(Map.Entry<String, Integer> m : count.entrySet()){
      if(m.getValue() == nbMax){
        booksMax += m.getKey();
      }
    }
   
      val.set(booksMax);
      context.write(key, val);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(CustomerBought.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}