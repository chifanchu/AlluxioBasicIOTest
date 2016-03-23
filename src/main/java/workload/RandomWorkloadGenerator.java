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


        final int reduceSpeedMultiplier = 3;
        final int totalFileNumber = 40;
        final int fileSizeMin = 75; //MB
        final int fileSizeMax = 150; //MB

        final int totalReadOperation = 250;


        // output reduced speed multiplier
        writer.println(reduceSpeedMultiplier);

        List<Integer> tmps = new ArrayList<Integer>();
        // create persist file
        for (int i=0; i<totalFileNumber; i++ ) {
            int size = ThreadLocalRandom.current().nextInt(fileSizeMin, fileSizeMax + 1);
            writer.println("create /tmp" + i + ".txt " + size);
            writer.println("create /file" + i + ".txt " + size);

            tmps.add(i);
        }

        writer.close();
        writer = new PrintWriter(System.getProperty("user.dir") + "/task.txt", "big5");

        long seed = System.nanoTime();
        Collections.shuffle(tmps, new Random(seed));

        // create task
        for (int i = 0; i < totalReadOperation / 2; i++) {
            int index = tmps.get(ThreadLocalRandom.current().nextInt(0, totalFileNumber));

            writer.println("read " + index + " /tmp" + index + ".txt");
        }

        for (int i = 0; i < totalReadOperation / 2; i++) {
            int index = tmps.get(ThreadLocalRandom.current().nextInt(totalFileNumber / 2 + 1, totalFileNumber));

            writer.println("read " + index + " /tmp" + index + ".txt");
        }

        writer.close();
    }

}
