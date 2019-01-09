import core.formats.BufferedImage.BufferedImageInputFormat;
import core.formats.BufferedImage.BufferedImageOutputFormat;
import core.writables.BufferedImageWritable;
import opencv.MatImageInputFormat;
import opencv.MatImageOutputFormat;
import opencv.MatImageWritable;
import opencv.OpenCVMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class MemesSentimentalAnalysis extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res  = ToolRunner.run(new MemesSentimentalAnalysis(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        Configuration conf = new Configuration();
        Path libraryPath = new Path("/home/wasim/Desktop/opencv/opencv-2.4.13.6/build/lib/libopencv_java2413.so");
        DistributedCache.addCacheFile(libraryPath.toUri(), conf);
        Job job  = Job.getInstance(conf, "MemesSentimentalAnalysis");
        job.setJarByClass(MemesSentimentalAnalysis.class);
        job.setMapperClass(MemesSentimentalAnalysisMapper.class);
        Path outputPath = new Path("/home/wasim/Desktop/output");
        FileInputFormat.setInputPaths(job, "/home/wasim/Desktop/memes/I will find you");
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setNumReduceTasks(0); // In our case we don't need Reduce phase
        job.setInputFormatClass(BufferedImageInputFormat.class);
        job.setOutputFormatClass(BufferedImageOutputFormat.class);
        job.setMapperClass(MemesSentimentalAnalysisMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BufferedImageWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MemesSentimentalAnalysisMapper extends Mapper<NullWritable, BufferedImageWritable, NullWritable, BufferedImageWritable> {

        private Graphics g;
        private BufferedImage grayImage;

        @Override
        protected void map(NullWritable key, BufferedImageWritable value, Context context) throws IOException, InterruptedException {
            BufferedImage colorImage = value.getImage();

            if (colorImage != null) {
                int WIDTH = colorImage.getWidth();
                int HEIGHT = colorImage.getHeight();
                BufferedImage image = new BufferedImage(WIDTH,HEIGHT,BufferedImage.TYPE_BYTE_GRAY);
                int pixels[]=new int[WIDTH*HEIGHT];
                colorImage.getRGB(0, 0, WIDTH, HEIGHT, pixels, 0, WIDTH);
                for(int i=0; i<pixels.length;i++)
                {
                    if (pixels[i] < 0xFFf0f0f0)
                    {
                        pixels[i] = 0xFF000000;
                    }
                }
                g = image.getGraphics();
                g.drawImage(colorImage, 0, 0, null);
                g.dispose();
                image.setRGB(0, 0, WIDTH, HEIGHT, pixels, 0, WIDTH);
                BufferedImageWritable biw = new BufferedImageWritable(image, value.getFileName(), value.getFormat());
                context.write(NullWritable.get(), biw);
            }
        }
    }
}
