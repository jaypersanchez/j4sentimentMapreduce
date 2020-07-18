
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Jayper
 */
public class CoreSystemEngine {
    private static ArrayList keywordsList = new ArrayList();
    private static BufferedReader br = null;
    private static FileReader fr = null;
    private static HashMap scoreWords = new HashMap();
    private Text positiveTotal = new Text("positiveTotal");
    private Text negativeTotal = new Text("negativeTotal");
    private static CoreSystemEngine instance = null;
    
    public static int getWordScore(String word) {
        int i = 0;
        if( scoreWords.containsKey(word.trim()) ) {
            //System.out.println( word + "^" + scoreWords.get(word) );
            i = Integer.parseInt( (String)scoreWords.get(word) );
        }
        return i;
    }
    
    public static void loadSentimentWords() {
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
           fr.close();
        }//try
        catch(Exception e) {
            e.printStackTrace();
        }//catch
    }
    
    public static void buildKeywordsList(String keywords) {
      StringTokenizer st = new StringTokenizer(keywords,"^");
      while (st.hasMoreElements()) {
        String k = (String)st.nextElement();
        keywordsList.add(k);
      }
    }
    
    protected CoreSystemEngine() {
      // Exists only to defeat instantiation.
      CoreSystemEngine.loadSentimentWords();
    }
    public static CoreSystemEngine getInstance() {
      if(instance == null) {
         instance = new CoreSystemEngine();
      }
      return instance;
    }
}
