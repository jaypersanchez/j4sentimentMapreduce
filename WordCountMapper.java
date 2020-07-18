// Learning MapReduce by Nitesh J.
// Word Count Mapper. 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper 
  extends Mapper<LongWritable, Text, Text, IntWritable>  {

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private ArrayList keywordsList = new ArrayList();
  private BufferedReader br = null;
  private FileReader fr = null;
  private HashMap scoreWords = new HashMap();
  private Text positiveTotal = new Text("positiveTotal");
  private Text negativeTotal = new Text("negativeTotal");
  IntWritable totalSentenceScore = new IntWritable(0);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
    Configuration conf = context.getConfiguration();
    String param = conf.get("param");
    buildKeywordsList(param);
    loadSentimentWords();
    
    while (itr.hasMoreTokens()) {
      
      //just added the below line to convert everything to lower case 
      word.set(itr.nextToken().toLowerCase());
      /** Pattern searching algorithm **/
      //the following check is that the word starts with an alphabet.
      keywordInFilterWord();
      if(Character.isAlphabetic((word.toString().charAt(0)))){
          int score = getWordScore(word.toString());
          totalSentenceScore.set(score);
    	  context.write(word, one);
          
      }//if
    }//while
    if(totalSentenceScore.get() > 0 ) {
        //positiveTotal += totalSentenceScore;
        context.write(positiveTotal, totalSentenceScore);
    }
    else {
        context.write(negativeTotal, totalSentenceScore);
    }
  }//map

  public int getWordScore(String word) {
        int i = 0;
        if( scoreWords.containsKey(word.trim()) ) {
            //System.out.println( word + "^" + scoreWords.get(word) );
            i = Integer.parseInt( (String)scoreWords.get(word) );
        }
        return i;
  }
  
  public void loadSentimentWords() {
        BufferedReader br = null;
	FileReader fr = null;
        
        try {
           fr = new FileReader("AFINNcsv.txt");
           br = new BufferedReader(fr); 
           String sCurrentLine;
           br = new BufferedReader(fr);
           while ((sCurrentLine = br.readLine()) != null) {
               int idx = sCurrentLine.trim().indexOf(',');
               if(idx > 0) {
                   //System.out.println(sCurrentLine + "^" + idx);
                   scoreWords.put(sCurrentLine.substring(0, idx), 
                              sCurrentLine.substring(idx+1,sCurrentLine.length()));
               }
           }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
  }
  
  void keywordInFilterWord() {
      String keyword = null;
      String filterword = word.toString();
      for (int i = 0; i < this.keywordsList.size(); i++) {
            if(filterword.contains( (String)keywordsList.get(i) ) ) {
                keyword = (String)keywordsList.get(i);
                word.set(keyword);
            }
      }
  }
  
  void buildKeywordsList(String keywords) {
      StringTokenizer st = new StringTokenizer(keywords,"^");
      while (st.hasMoreElements()) {
        String k = (String)st.nextElement();
        this.keywordsList.add(k);
      }
  }
  
}

    
