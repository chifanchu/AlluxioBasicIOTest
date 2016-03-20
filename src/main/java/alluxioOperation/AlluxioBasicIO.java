package alluxioOperation;

import common.Utils;
import common.Utils.TimeMeasure;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;



/**
 *
 * Created by nelsonchu on 2/13/16.
 */
public class AlluxioBasicIO {
    static public final String WORD    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static public final String MASTER  = "AlluxioMaster";
    static public final String WORKER1 = "AlluxioWorker1";
    static public final String NON_SPECIFIED_WORKER = "AnyWorker";

    private final FileSystem mFileSystem;


    // use all default and alluxio-site configurations
    private AlluxioBasicIO() {
        Utils.log("Connecting to Master..." + ClientContext.getMasterAddress());
        mFileSystem = FileSystem.Factory.get();
    }

    // use this constructor to specify runtime master hostname
    private AlluxioBasicIO(AlluxioURI masterLocation) {
        ClientContext.getConf().set(Constants.MASTER_HOSTNAME, masterLocation.getHost());
        //ClientContext.getConf().set(Constants.MASTER_RPC_PORT,
        //        Integer.toString(masterLocation.getPort()));
        ClientContext.init();
        Utils.log("Connecting to Master..." + ClientContext.getMasterAddress());
        mFileSystem = FileSystem.Factory.get();
    }

    private void writeFile(AlluxioURI uri, String msg, WriteType type, String targetWorker)
            throws IOException, AlluxioException {
        Utils.log("Writing data \"" + msg + "\" to " + uri.getPath()
                + " at " + targetWorker
                + (type.isCache() ? " memory" : "") + (type.isThrough() ? " disk" : ""));

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        ByteBuffer buf = ByteBuffer.wrap(msg.getBytes(Charset.forName("UTF-8")));

        FileOutStream os = getFileOutStream(uri, type, targetWorker);
        try {
            os.write(buf.array());
            Utils.log("     Done!");
        } catch (Exception e) {
            e.printStackTrace();
        }  finally {
            os.close();
        }

        tm.pause();
        Utils.log("    ElapsedTime = " + tm.getElapsedTime() + " ms");
    }

    private FileOutStream getFileOutStream(AlluxioURI uri, WriteType type, String targetWorker)
            throws AlluxioException, IOException {
        CreateFileOptions writeOptions = CreateFileOptions.defaults().setWriteType(type);
        if(!targetWorker.equals(NON_SPECIFIED_WORKER)) {
            writeOptions.setLocationPolicy(new SpecificHostPolicy(targetWorker));
        }
        return mFileSystem.createFile(uri, writeOptions);
    }

    private void writeLargeFile(AlluxioURI uri, String msg, double sizeMB, WriteType type, String targetWorker)
            throws IOException, AlluxioException {
        Utils.log("Writing " + sizeMB + "MB data to " + uri.getPath()
                + " at " + targetWorker
                + (type.isCache() ? " memory" : "") + (type.isThrough() ? " disk" : ""));

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        int remainSizeByte = (int)sizeMB*1024*1024;
        int len = Utils.utf8LenCounter(msg);

        if(remainSizeByte==0 || len==0) {
            Utils.log("Error: Zero length input!!");
            return;
        }

        ByteBuffer buf = ByteBuffer.wrap(msg.getBytes(Charset.forName("UTF-8")));
        FileOutStream os = getFileOutStream(uri, type, targetWorker);
        try {
            while((remainSizeByte-=len)>0) {
                os.write(buf.array());
            }
            Utils.log("     Done!");
        } catch (Exception e) {
            e.printStackTrace();
        }  finally {
            os.close();
        }

        tm.pause();
        Utils.log("     ElapsedTime = " + tm.getElapsedTime() + " ms");
    }

    private void readFile(AlluxioURI uri, ReadType type, boolean toPrint)
            throws IOException, AlluxioException {
        readFile(uri, type, toPrint, NON_SPECIFIED_WORKER);
    }

    private void readFile(AlluxioURI uri, ReadType type, boolean toPrint, String cacheLocation)
            throws AlluxioException, IOException {
        Utils.log("Reading data from " + uri.getPath() + "...");

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        OpenFileOptions readOptions = OpenFileOptions.defaults().setReadType(type);
        if (!cacheLocation.equals(NON_SPECIFIED_WORKER)) {
            readOptions.setLocationPolicy(new SpecificHostPolicy(cacheLocation));
        }
        FileInStream is = mFileSystem.openFile(uri, readOptions);

        //ByteBuffer buf = ByteBuffer.allocate((int) is.remaining());
        //noinspection ResultOfMethodCallIgnored
        try {
            byte[] bytes = new byte[100000];
            int bytesRead = 0;
            while ((bytesRead = is.read(bytes)) != -1) {
                String msg = new String(bytes, 0, bytesRead, Charset.forName("UTF-8"));
                if(toPrint) {
                    Utils.log(msg);
                }
            }

            Utils.log("     Done!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            is.close();
        }

        tm.pause();
        Utils.log("     ElapsedTime = " + tm.getElapsedTime() + " ms");
    }

    private void removeFile(AlluxioURI uri)
            throws IOException, AlluxioException {
        Utils.log("Deleting file: " + uri.getPath());

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        mFileSystem.delete(uri);

        tm.pause();
        Utils.log("     ElapsedTime = " + tm.getElapsedTime() + " ms");
    }

    private void freeFile(AlluxioURI uri)
            throws IOException, AlluxioException {
        Utils.log("Freeing file: " + uri.getPath());

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        mFileSystem.free(uri);

        tm.pause();
        Utils.log("ElapsedTime = " + tm.getElapsedTime() + " ms");
    }

    public void doTask() throws IOException, AlluxioException {
        Utils.log("Starting task...");

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        BufferedReader reader = new BufferedReader(new FileReader("task.txt"));
        String line;
        int counter = 0;
        while((line = reader.readLine()) != null) {
            String[] components = line.split(" ");
            String fileName = components[1];

            if (components[0].equals("create")) {
                int    fileSize = Integer.parseInt(components[2]);
                //writeLargeFile(new AlluxioURI(fileName), fileName+WORD, fileSize, WriteType.CACHE_THROUGH, MASTER);
                //writeLargeFile(new AlluxioURI(fileName), fileName+WORD, fileSize, WriteType.CACHE_THROUGH, "130.127.133.22");
                writeLargeFile(new AlluxioURI(fileName), fileName+WORD, fileSize, WriteType.CACHE_THROUGH, NON_SPECIFIED_WORKER);
            } else if (components[0].equals("read")) {
                Utils.log(Integer.toString(counter++));
                //readFile(new AlluxioURI(fileName), ReadType.CACHE_PROMOTE, false, MASTER);
                //readFile(new AlluxioURI(fileName), ReadType.CACHE_PROMOTE, false, "130.127.133.22");
                readFile(new AlluxioURI(fileName), ReadType.CACHE_PROMOTE, false, NON_SPECIFIED_WORKER);
            }
        }

        tm.pause();
        Utils.log("Task elapsed time = " + (double) tm.getElapsedTime() / 1000 + " sec");
    }

    public static void main(String[] args)
            throws AlluxioException, IOException {
        System.out.print("\n");


        //AlluxioBasicIO alluIO = new AlluxioBasicIO(new AlluxioURI("http://localhost"));
        AlluxioBasicIO alluIO = new AlluxioBasicIO();

        String path = "/tmp85.txt";

        // writeType = [CACHE_THROUGH|MUST_CACHE|THROUGH|ASYNC_THROUGH]
        //alluIO.writeFile(new AlluxioURI(path), "ABCDEFG", WriteType.CACHE_THROUGH, WORKER1);

        // write larger file, writerType = [CACHE_THROUGH|MUST_CACHE|THROUGH|ASYNC_THROUGH]
        //alluIO.writeLargeFile(new AlluxioURI(path), WORD, 199, WriteType.MUST_CACHE, MASTER);

        //alluIO.writeFile     (new AlluxioURI("/tmp80.txt"), "ABCDEFG", WriteType.MUST_CACHE, MASTER);
        //alluIO.writeLargeFile(new AlluxioURI("/tmp81.txt"), WORD, 50, WriteType.CACHE_THROUGH, MASTER);
        //alluIO.writeLargeFile(new AlluxioURI("/tmp82.txt"), WORD, 50, WriteType.CACHE_THROUGH, MASTER);
        //alluIO.writeLargeFile(new AlluxioURI("/tmp83.txt"), WORD, 50, WriteType.CACHE_THROUGH, MASTER);
        //alluIO.writeLargeFile(new AlluxioURI("/tmp84.txt"), WORD, 49, WriteType.CACHE_THROUGH, MASTER);

        //alluIO.writeLargeFile(new AlluxioURI("/tmp90.txt"), WORD, 145, WriteType.MUST_CACHE, WORKER1);

        // readType = [CACHE_PROMOTE|CACHE|NO_CACHE]
        //alluIO.readFile(new AlluxioURI(path), ReadType.CACHE_PROMOTE, false);

        //alluIO.removeFile(new AlluxioURI(path));

        //alluIO.removeFile(new AlluxioURI("/tmp80.txt"));
        //alluIO.removeFile(new AlluxioURI("/tmp81.txt"));
        //alluIO.removeFile(new AlluxioURI("/tmp82.txt"));
        //alluIO.removeFile(new AlluxioURI("/tmp83.txt"));
        //alluIO.removeFile(new AlluxioURI("/tmp84.txt"));
        //alluIO.removeFile(new AlluxioURI("/tmp85.txt"));
        //alluIO.removeFile(new AlluxioURI("/tmp86.txt"));

        //alluIO.removeFile(new AlluxioURI("/tmp90.txt"));

        //alluIO.freeFile(new AlluxioURI(path));

        alluIO.doTask();
    }
}
