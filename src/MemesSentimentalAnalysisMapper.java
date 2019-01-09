import core.writables.BufferedImageWritable;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class MemesSentimentalAnalysisMapper extends Mapper<NullWritable, BufferedImageWritable, NullWritable, Text> {

    private BufferedImage grayImage;
    public final static Text memeText = new Text();

    /**
     * Map method accepts an image and finds out the imprinted text over it.
     * @param key NullWritable
     * @param value BufferedImageWritable
     * @param context mapred job context
     * @throws IOException In case the image is not read or output
     * @throws InterruptedException In case of unplanned interrupt
     */
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
            image.setRGB(0, 0, WIDTH, HEIGHT, pixels, 0, WIDTH);
            String imageFile = String.format("%s.%s", RandomStringUtils.randomAlphanumeric(8), value.getFormat());
            String tesseractFile = "/home/wasim/Desktop/txt/words";
            File outputfile = new File("/home/wasim/Desktop/tmp/"+imageFile);
            ImageIO.write(image, value.getFormat(), outputfile);
            Runtime rt = Runtime.getRuntime();
            Process pr = rt.exec("env TESSDATA_PREFIX=/home/wasim/Desktop/tessdata tesseract -l joh /home/wasim/Desktop/tmp/"+imageFile+" "+
                    tesseractFile);
            pr.waitFor();
            File file = new File(tesseractFile+".txt");
            Scanner sc = new Scanner(file);
            String mText ="";
            while (sc.hasNextLine()){
                mText= mText+ sc.nextLine();
            }
            memeText.set(mText);
            context.write(NullWritable.get(), memeText);
        }
    }
}
