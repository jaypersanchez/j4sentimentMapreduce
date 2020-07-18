// Learning MapReduce by Nitesh Jain
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: WordCount <input path> <output path> <keywords>^<keywords>^<keywords>");
      System.exit(-1);
    }
    //creaet singleton to load common resources
    CoreSystemEngine.getInstance();
    
    //get stopwords and AFINN words
    Configuration conf = new Configuration();
    conf.set("param", args[2]);
    Job job = new Job(conf);
    job.setJarByClass(WordCount.class);
    job.setJobName("Word Count");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
