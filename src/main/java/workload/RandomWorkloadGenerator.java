package workload;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate random read/write workload.
 * Created by nelsonchu on 3/18/16.
 */
public class RandomWorkloadGenerator {
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        //System.out.println("Working Directory = " + System.getProperty("user.dir"));
        PrintWriter writer = new PrintWriter(System.getProperty("user.dir") + "/task.txt", "big5");

        List<Integer> files = new ArrayList<Integer>();
        final int persistFileNumber = 10;
        final int recomputeSourceNumberMax = persistFileNumber/2;

        List<Integer> tmps = new ArrayList<Integer>();
        final int fileNumber = 40;
        final double persistRatio = 0.5;
        final int fileSizeMin = 70; //MB
        final int fileSizeMax = 150; //MB
        final int totalReadOperation = 500;

        //create persist /file0.txt [size]
        // generate persisted files which will be used for re-computation
        for (int i=0; i<10; i++) {
            writer.println("create /file" + i + ".txt " +
                    ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1) +
                    " persist");
            files.add(i);
        }

        //create persist /tmp0.txt [size]
        for (int i=0; i<fileNumber * persistRatio; i++) {
            writer.println("create /tmp" + i + ".txt " +
                    ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1) +
                    " persist");
            tmps.add(i);
        }

        //create nonpersist /tmp1.txt [size] [files....]
        for (int i=(int)(fileNumber * persistRatio); i<fileNumber; i++) {
            writer.print("create /tmp" + i + ".txt " +
                    ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1) +
                    " nonpersist");

            int num = ThreadLocalRandom.current().nextInt(1, recomputeSourceNumberMax);
            long seed = System.nanoTime();
            Collections.shuffle(files, new Random(seed));
            for (int j=0; j<num; j++) {
                writer.print(" /file" + files.get(j) + ".txt");
            }
            writer.print("\n");
            tmps.add(i);
        }

        //long seed = System.nanoTime();
        //Collections.shuffle(tmps, new Random(seed));

        // read file
        // generate randomly read workload for all files
        for (int i = 0; i < totalReadOperation / 2; i++) {
            writer.println("read /tmp" +
                    tmps.get(ThreadLocalRandom.current().nextInt(0, fileNumber)) + ".txt");
        }

        for (int i = 0; i < totalReadOperation / 2; i++) {
            writer.println("read /tmp" +
                    tmps.get(ThreadLocalRandom.current().nextInt(fileNumber/2+1, fileNumber)) + ".txt");
        }
        writer.close();
    }

}
