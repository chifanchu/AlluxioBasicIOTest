package result;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * Parse the result log files created by runAlluxioTests.py script
 * Created by nelsonchu on 4/4/16.
 */
public class ResultParser {
    public static final String LOG_DIR = System.getProperty("user.dir") + "/../log";
    public static final String OUTPUT_NAME = "results.txt";

    public static class SingleResult {
        public int mReduceSpeed;
        public boolean mBackground;
        public boolean mGlobalLRU;
        public long mTimeThreshold;
        public double mRunTime = -1;
    }

    public static class PriorityQueue {
        private final List<SingleResult> mInnerList = new LinkedList();

        public void add(SingleResult singleResult) {
            for (int i=0; i<mInnerList.size(); i++) {
                SingleResult elem = mInnerList.get(i);
                if (compare(singleResult, elem)) {
                    mInnerList.add(i, singleResult);
                    return;
                }
            }

            mInnerList.add(singleResult);
        }

        public List<SingleResult> getList() {
            return mInnerList;
        }

        // return true if first should be placed before second
        private boolean compare(SingleResult first, SingleResult second) {
            if (first.mReduceSpeed < second.mReduceSpeed)
                return true;

            if (first.mBackground != second.mBackground
                    && !first.mBackground)
                return true;

            if (first.mGlobalLRU != second.mGlobalLRU
                    && !first.mGlobalLRU)
                return true;

            if (first.mTimeThreshold < second.mTimeThreshold)
                return true;

            return false;
        }
    }

    public static void main(String[] args)
            throws IOException {
        PriorityQueue allResult = new PriorityQueue();

        File logDir = new File(LOG_DIR);
        for (final File fileEntry : logDir.listFiles()) {
            String[] comp = fileEntry.getName().split("_");
            if (comp.length != 12) {
                continue;
            }

            SingleResult singleResult = new SingleResult();
            singleResult.mBackground = Boolean.parseBoolean(comp[1]);
            singleResult.mGlobalLRU = Boolean.parseBoolean(comp[4]);
            singleResult.mTimeThreshold = Long.parseLong(comp[7]);
            singleResult.mReduceSpeed = Integer.parseInt(comp[11].split("\\.")[0]);

            BufferedReader reader =
                    new BufferedReader(new FileReader(fileEntry.getPath()));
            String line;
            while((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("--- Task elapsed time =")) {
                    singleResult.mRunTime = Double.parseDouble(line.split(" ")[5]);
                    break;
                }
            }
            reader.close();

            allResult.add(singleResult);
        }

        PrintWriter writer =
                new PrintWriter(System.getProperty("user.dir") + "/" + OUTPUT_NAME, "big5");

        int reduceSpeed = -1;
        for (SingleResult singleResult : allResult.getList()) {
            if (singleResult.mRunTime == -1) {
                continue;
            }

            int tmp = singleResult.mReduceSpeed;
            if (reduceSpeed != tmp) {
                reduceSpeed = tmp;
                writer.print("----- Reduced speed " + reduceSpeed + " ----------------------------------------------------\n");
            }
            writer.print("Background = " + Boolean.toString(singleResult.mBackground) + ", " +
                         "Global LRU = " + Boolean.toString(singleResult.mGlobalLRU) + ", " +
                         "Time threshold = " + Long.toString(singleResult.mTimeThreshold) + "\n");
            writer.print("Run time = " + Double.toString(singleResult.mRunTime) + "\n");
            writer.print("\n");
        }
        writer.close();
    }
}
