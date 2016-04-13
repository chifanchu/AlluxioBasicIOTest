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
        PrintWriter writer = new PrintWriter(System.getProperty("user.dir") + "/prepare.txt", "big5");

        String writeOption = "";

        // scenario 1: total file size smaller than available memory space
        /*
        final int fileNameBase = 0;
        final int totalFileNumber = 30;
        final int fileSizeMin = 150; //MB
        final int fileSizeMax = 220; //MB
        final int totalReadOperation = 1000;
        final int manyReadOperation = 1;
        final int fewReadOperation = 1;
        final int rareReadOperation = 1;
        */

        // scenario 2: total file size exceed available memory space
        // 2G inactive        --> first 10 files
        // 2G not that active --> first 10 files
        // 4G active          --> last  20 files
        /*
        final int fileNameBase = 0;
        final int totalFileNumber = 40;
        final int fileSizeMin = 150; //MB
        final int fileSizeMax = 250; //MB
        final int totalReadOperation = 1000;
        final int manyReadOperation = 40;
        final int fewReadOperation = 14;
        final int rareReadOperation = 6;
        */

        // scenario 3: three clients, every thing can fit into memory
        // client1: 4G 30 files(100~160MB) 1000 operations
        // client2: 1G 20 files(40~55MB) 6000 operations
        // client3: 1G 10 files(80~120MB) 2500 operations
        /*
        final int fileNameBase = 0;
        final int totalFileNumber = 30;
        final int fileSizeMin = 100; //MB
        final int fileSizeMax = 160; //MB
        final int totalReadOperation = 1000;
        final int manyReadOperation = 1;
        final int fewReadOperation = 1;
        final int rareReadOperation = 1;
        writeOption = " cache";
        */

        // scenario 4: for NDR calibration
        // 2G 10 files(150~250MB)
        final int fileNameBase = 0;
        final int totalFileNumber = 10;
        final int fileSizeMin = 150; //MB
        final int fileSizeMax = 250; //MB
        final int totalReadOperation = 100;
        final int manyReadOperation = 1;
        final int fewReadOperation = 1;
        final int rareReadOperation = 1;
        writeOption = " cache";


        int totalFileSize = 0;
        List<Integer> tmps = new ArrayList<Integer>();
        // create persist file
        for (int i=fileNameBase; i<totalFileNumber/4 + fileNameBase; i++ ) {
            int size = ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1);
            writer.println("create /tmp" + i + ".txt " + size + writeOption);
            writer.println("create /file" + i + ".txt " + size);

            totalFileSize += size;

            for (int j=0; j<rareReadOperation; j++) {
                tmps.add(i);
            }
        }

        // create persist file
        for (int i=totalFileNumber/4 + fileNameBase; i<totalFileNumber/2 + fileNameBase; i++ ) {
            int size = ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1);
            writer.println("create /tmp" + i + ".txt " + size + writeOption);
            writer.println("create /file" + i + ".txt " + size);

            totalFileSize += size;

            for (int j=0; j<fewReadOperation; j++) {
                tmps.add(i);
            }
        }

        // create persist file
        for (int i=totalFileNumber/2 + fileNameBase; i<totalFileNumber + fileNameBase; i++ ) {
            int size = ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1);
            writer.println("create /tmp" + i + ".txt " + size + writeOption);
            writer.println("create /file" + i + ".txt " + size);

            totalFileSize += size;

            for (int j=0; j<manyReadOperation; j++) {
                tmps.add(i);
            }
        }
        writer.println("Total file size: " + totalFileSize);
        writer.close();

        writer = new PrintWriter(System.getProperty("user.dir") + "/task.txt", "big5");

        long seed = System.nanoTime();
        Collections.shuffle(tmps, new Random(seed));

        // create task
        for (int i = 0; i < totalReadOperation; i++) {
            int index = tmps.get(ThreadLocalRandom.current().nextInt(0, tmps.size()));

            writer.println("read " + index + " /tmp" + index + ".txt");
        }

        writer.close();
    }

}
