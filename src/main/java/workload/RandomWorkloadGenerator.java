package workload;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate random read/write workload.
 * Created by nelsonchu on 3/18/16.
 */
public class RandomWorkloadGenerator {
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        //System.out.println("Working Directory = " + System.getProperty("user.dir"));
        PrintWriter writer = new PrintWriter(System.getProperty("user.dir") + "/task.txt", "big5");

        final int fileNumber = 40;
        final int fileSizeMin = 70; //MB
        final int fileSizeMax = 150; //MB
        final int totalReadOperation = 1000;

        // generate #fileNumber files with size ranging uniformly from fileSizeMin to fileSizeMax
        for(int i = 0; i < fileNumber; i++) {
            writer.println("create /tmp" + i + ".txt " +
                    ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1));
        }

        //for(int j=0; j<2; j++) {
            // generate randomly read workload for all files
            for (int i = 0; i < totalReadOperation / 2; i++) {
                writer.println("read /tmp" +
                        ThreadLocalRandom.current().nextInt(0, fileNumber) + ".txt");
            }

            for (int i = 0; i < totalReadOperation / 2; i++) {
                writer.println("read /tmp" +
                        ThreadLocalRandom.current().nextInt(fileNumber/2+1, fileNumber) + ".txt");
            }
        //}
        writer.close();
    }
}
