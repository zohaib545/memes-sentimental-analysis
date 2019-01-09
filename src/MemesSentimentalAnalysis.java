import core.formats.BufferedImage.BufferedImageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MemesSentimentalAnalysis extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res  = ToolRunner.run(new MemesSentimentalAnalysis(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        Configuration conf = new Configuration();
        System.load("/home/wasim/Desktop/opencv/opencv-2.4.13.6/build/lib/libopencv_java2413.so");
        Path libraryPath = new Path("/home/wasim/Desktop/opencv/opencv-2.4.13.6/build/lib/libopencv_java2413.so");
        DistributedCache.addCacheFile(libraryPath.toUri(), conf);
        Job job  = Job.getInstance(conf, "MemesSentimentalAnalysis");
        job.setJarByClass(MemesSentimentalAnalysis.class);
        job.setMapperClass(MemesSentimentalAnalysisMapper.class);
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(BufferedImageInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MemesSentimentalAnalysisMapper.class);
        job.setReducerClass(MemesSentimentalAnalysisReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
