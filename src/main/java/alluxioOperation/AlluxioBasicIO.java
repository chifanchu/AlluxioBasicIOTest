package alluxioOperation;

import alluxio.client.file.URIStatus;
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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by nelsonchu on 2/13/16.
 */
public class AlluxioBasicIO {
    static public final String WORD    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static public final String MASTER  = "AlluxioMaster";
    static public final String WORKER1 = "AlluxioWorker1";
    static public final String NON_SPECIFIED_WORKER = "AnyWorker";
    private static String sLongMsg;

    private final FileSystem mFileSystem;

    private int mReducedSpeedMultiplier = 1;
    private Map<String, Integer> mFileSizeMap = new HashMap<String, Integer>();

    // use all default and alluxio-site configurations
    private AlluxioBasicIO() {
        Utils.log("Connecting to Master..." + ClientContext.getMasterAddress());
        mFileSystem = FileSystem.Factory.get();
    }

    // use this constructor to specify runtime master hostname
    private AlluxioBasicIO(AlluxioURI masterLocation) {
        ClientContext.getConf().set(Constants.MASTER_HOSTNAME, masterLocation.getHost());
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
            throw new IOException(e);
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
            throw new IOException(e);
        }  finally {
            os.close();
        }

        tm.pause();
        Utils.log("     ElapsedTime = " + tm.getElapsedTime() + " ms");
    }

    private void writeLargeFileLocal(String filepath, String msg, double sizeMB)
            throws IOException, AlluxioException {
        Utils.log("Writing " + sizeMB + "MB data to " + filepath
                + " to local disk");

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        int remainSizeByte = (int)sizeMB*1024*1024;
        int len = Utils.utf8LenCounter(msg);

        if(remainSizeByte==0 || len==0) {
            Utils.log("Error: Zero length input!!");
            return;
        }

        ByteBuffer buf = ByteBuffer.wrap(msg.getBytes(Charset.forName("UTF-8")));
        FileOutputStream os = new FileOutputStream(filepath);
        try {
            while((remainSizeByte-=len)>0) {
                os.write(buf.array());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }  finally {
            os.flush();
            os.getFD().sync();
            os.close();
        }

        tm.pause();
        Utils.log("     Done!");
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
            byte[] bytes = new byte[10000000];
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
            throw new IOException(e);
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

    public boolean inMemory(String fileName) throws AlluxioException, IOException {
        URIStatus status = mFileSystem.getStatus(new AlluxioURI(fileName));
        return status.getInMemoryPercentage() == 100;
    }

    public void preTask(String workerHostName) throws IOException, AlluxioException {
        Utils.log("Prepare task...");

        BufferedReader reader = new BufferedReader(new FileReader("prepare.txt"));
        String line;

        //line = reader.readLine();
        //mReducedSpeedMultiplier = Integer.parseInt(line);

        String dummy = "";
        while((line = reader.readLine()) != null) {
            String[] components = line.split(" ");
            String fileName = components[1];

            if (components[0].equals("create")) {
                int fileSize = Integer.parseInt(components[2]);
                if (fileName.startsWith("/tmp")) {
                    if (components.length >= 4 && components[3].equals("cache")) {
                        writeLargeFile(new AlluxioURI(fileName), sLongMsg, fileSize, WriteType.CACHE_THROUGH, workerHostName);
                    } else {
                        writeLargeFile(new AlluxioURI(fileName), sLongMsg, fileSize, WriteType.THROUGH, workerHostName);
                    }
                } else  {
                    dummy = fileName;
                    mFileSizeMap.put(fileName, fileSize);
                }
            }

        }
        Utils.log("Getting disk write resources...");
        Utils.disableOutput();

        String path = System.getProperty("user.dir") + dummy;
        Path ppath = FileSystems.getDefault().getPath(path);
        writeLargeFileLocal(path, sLongMsg, mFileSizeMap.get(dummy));
        Files.delete(ppath);

        Utils.enableOutput();
        Utils.log("Done");
    }

    public void doTask(String workerHostName) throws IOException, AlluxioException {
        Utils.log("Starting task...");

        TimeMeasure tm = new TimeMeasure();
        tm.start();

        BufferedReader reader = new BufferedReader(new FileReader("task.txt"));
        String line;
        int counter = 0;
        while((line = reader.readLine()) != null) {
            String[] components = line.split(" ");
            int index = Integer.parseInt(components[1]);
            String fileName = components[2];

            if (components[0].equals("read")) {
                Utils.log(Integer.toString(counter++));
                if (!inMemory(fileName)) {
                    String overheadFileName = "/file" + index + ".txt";
                    int fileSize = mFileSizeMap.get(overheadFileName);
                    String path = System.getProperty("user.dir") + overheadFileName;
                    Path ppath = FileSystems.getDefault().getPath(path);
                    for(int i=0; i<mReducedSpeedMultiplier; i++) {
                        Utils.log("Reducing speed!!! " + Integer.toString(i + 1));
                        writeLargeFileLocal(path, sLongMsg, fileSize);
                        Utils.log("     delete file...");
                        Files.delete(ppath);
                    }
                }
                readFile(new AlluxioURI(fileName), ReadType.CACHE, false, workerHostName);
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

        // readType = [CACHE_PROMOTE|CACHE|NO_CACHE]
        //alluIO.readFile(new AlluxioURI(path), ReadType.CACHE_PROMOTE, false);

        //alluIO.removeFile(new AlluxioURI(path));

        //alluIO.freeFile(new AlluxioURI(path));

        StringBuilder builder = new StringBuilder();
        int count = 0;
        while (count < 10000000) {
            builder.append(WORD);
            count += WORD.length();
        }
        sLongMsg = builder.toString();

        String workerHostName = NON_SPECIFIED_WORKER;
        //String workerHostName = "cp-1-mgmt-lan";
        //String workerHostName = MASTER;

        if (args.length>0) {
            alluIO.mReducedSpeedMultiplier = Integer.parseInt(args[0]);
        }
        Utils.log("Reduced disk speed by: " + alluIO.mReducedSpeedMultiplier + "X");
        alluIO.preTask(workerHostName);
        alluIO.doTask(workerHostName);
    }
}
