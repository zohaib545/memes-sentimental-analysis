import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class MemesSentimentalAnalysisReducer extends Reducer<NullWritable, Text, NullWritable, Text>
{

    public static final String negativeFile = "/home/wasim/Desktop/negativewords.txt";
    public static final String positiveFile = "/home/wasim/Desktop/positivewords.txt";

    private Text sentiments = new Text();

    ArrayList<String> negativeArray = new ArrayList<String>();
    ArrayList<String> positiveArray = new ArrayList<String>();

    /**
     * Setup for the reducer
     * Fetch negative and positive words for sentimental analysis
     * @param context
     * @throws IOException File reading
     * @throws InterruptedException unplanned code interrupt
     */
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        try {
            Scanner sNegative = new Scanner(new File(negativeFile));
            Scanner sPositive = new Scanner(new File(positiveFile));

            while (sNegative.hasNextLine()){
                negativeArray.add(sNegative.nextLine().toLowerCase());
            }
            while(sPositive.hasNextLine()) {
                positiveArray.add(sPositive.nextLine().toLowerCase());
            }
            sNegative.close();
            sPositive.close();
        }catch(FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * Reduce method to do sentimental analysis based on the text provided in previous step
     * @param key NullWritable
     * @param values Iterable<Text>
     * @param context context of mapred job
     * @throws IOException In case if output cannot be made
     * @throws InterruptedException In case of unplanned code interrupt
     */
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int positive = 0;
        int negative = 0;
        double result = 0;

        for(Text value : values){
            String[] memeText = value.toString().split(" ");
            for(int i = 0; i < memeText.length; i++) {
                if(negativeArray.contains(memeText[i].toLowerCase().trim())) {
                    negative++;
                }else if(positiveArray.contains(memeText[i].toLowerCase().trim())) {
                    positive++;
                }
            }

            try {
                result = (positive + negative)/(positive - negative);
            }catch(ArithmeticException e) {
               result = 0 ;
            }
        }
        sentiments.set(String.valueOf(result));
        context.write(key,sentiments);
    }
}