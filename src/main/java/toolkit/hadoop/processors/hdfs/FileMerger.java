package toolkit.hadoop.processors.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class FileMerger {
    private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    private static org.apache.hadoop.fs.FileSystem fs;

    private static String hdfsConnect="hdfs://localhost:9000";
    private static String hdfsPath="/";
    private static String sizeLimit="10";
    private static String fileLimit="5";
    private static Integer fileLimitCtl = 0;
    private static Boolean compress=false;

    private static String mergeInto = "";

    public static void main(String[] args)
    {
        Options options = new Options();
        options.addOption("hdfsConnect", true, "connection string for hdfs. ex: hdfs://<hostname>:<port>");
        options.addOption("hdfsPath", true, "path to search for files to merge. ex: /<folder_name>");
        options.addOption("sizeLimit", true, "limit of the maximum file size in MBs. ex: 10");
        options.addOption("fileLimit", true, "limit of the maximum file count. ex: 10");
        options.addOption("compress", true, "if the merged file will be compressed with gzip");

        try
        {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("hdfsConnect"))
            {
                hdfsConnect = cmd.getOptionValue("hdfsConnect");
            }

            if (cmd.hasOption("hdfsPath"))
            {
                hdfsPath = cmd.getOptionValue("hdfsPath");
            }

            if (cmd.hasOption("sizeLimit"))
            {
                sizeLimit = cmd.getOptionValue("sizeLimit");
            }

            if (cmd.hasOption("fileLimit"))
            {
                fileLimit = cmd.getOptionValue("fileLimit");
            }

            if (cmd.hasOption("compress"))
            {
                if (cmd.getOptionValue("compress").trim().equalsIgnoreCase("true"))
                {
                    compress=true;
                }
            }
        }
        catch(UnrecognizedOptionException ex)
        {
            System.err.println("Unrecognized option");
            System.err.println(ex.getMessage());
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            System.out.println("Options List for exec: ");
            System.out.println("-----------------------");
            System.out.println("hdfsConnect: " + hdfsConnect + " --> Description: " + options.getOption("hdfsConnect").getDescription());
            System.out.println("hdfsPath: " + hdfsPath + " --> Description: " + options.getOption("hdfsPath").getDescription());
            System.out.println("sizeLimit: " + sizeLimit + " --> Description: " + options.getOption("sizeLimit").getDescription());
            System.out.println("sizeLimit: " + fileLimit + " --> Description: " + options.getOption("fileLimit").getDescription());
            System.out.println("compress: " + compress.toString() + " --> Description: " + options.getOption("compress").getDescription());
            System.out.println("-----------------------");
        }

        connectHadoop();

        Long orgFileModificationTime;
        Long orgFileAccessTime;

        List<String> arrMergeFile = new ArrayList();
        try
        {
            //there is a path with the supplied parameter
            if (fs.exists(new Path(hdfsPath)))
            {
                //and this path is a directory
                if (fs.isDirectory(new Path(hdfsPath)))
                {
                    //get the statuses of the files within the path
                    org.apache.hadoop.fs.FileStatus[] fileStats = fs.listStatus(new Path(hdfsPath));
                    DistributedFileSystem dfs = new DistributedFileSystem();
                    dfs.initialize(fs.getUri(), conf);

                    //
                    Boolean mergeFileFull = true;
                    FSDataOutputStream fout = null;
                    BufferedWriter bw = null;

                    //for all the files in the path
                    for (FileStatus fileStat : fileStats)
                    {
                        //if this path is a file
                        if (fileStat.isFile())
                        {
                            orgFileModificationTime = fileStat.getModificationTime();
                            orgFileAccessTime = fileStat.getAccessTime();

                            //get the size of the file
                            ContentSummary contentSum = fs.getContentSummary(fileStat.getPath());
                            Long fileSize = contentSum.getLength();

                            if (isOpenForWrite(fileStat.getPath().toString()))
                            {
                                continue;
                            }

                            if (isCompressed(fileStat.getPath().toString()))
                            {
                                continue;
                            }

                            //if the size of the file is lower than the sizeLimit
                            if (fileSize < Long.parseLong(sizeLimit)*1024*1024)
                            {
                                //log the name of the file we are working on right now
                                System.out.println("working on file: " + fileStat.getPath().toString() + " - " + fileSize/1024/1024);

                                if (mergeFileFull)
                                {
                                    //create a temporary file the .merge suffix to start merging into
                                    mergeInto = fileStat.getPath() + ".merge" ;

                                    //open the output stream for the merge file
                                    fout = fs.create(new Path(mergeInto));
                                    bw = new BufferedWriter(new OutputStreamWriter(fout));
                                    System.out.println("new merge file created: " + mergeInto);

                                    mergeFileFull = false;
                                }

                                //open the input stream for the working file
                                FSDataInputStream fin = fs.open(fileStat.getPath());
                                BufferedReader br = new BufferedReader(new InputStreamReader(fin));
                                System.out.println("read file: " + fileStat.getPath());

                                //now read the file and write contents to the merge file until the end of the working file
                                String line="";
                                Integer lineCount = 0;
                                while ((line=br.readLine()) != null)
                                {
                                    bw.write(line);
                                    bw.newLine();

                                    lineCount ++;
                                    if (lineCount >= 1000)
                                    {
                                        Thread.sleep(100);
                                        System.out.println("line count reached: " + lineCount);

                                        //flush every 1000 lines
                                        bw.flush();
                                        fout.flush();

                                        /*
                                        hsync() internally calls the private flushOrSync(boolean isSync, EnumSet<SyncFlag> syncFlags) with no flag,
                                        and the length is only updated on the namenode if SyncFlag.UPDATE_LENGTH is provided.
                                        */
                                        fout.hflush();
                                        //fout.hsync();

                                        /*To update the size, you can alternatively call (without the proper class type checking):*/
                                        (((DFSOutputStream) fout.getWrappedStream())).hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));


                                        //if the merge file size is reached the sizeLimit
                                        if (fs.getContentSummary(new Path(mergeInto)).getLength() >= Long.parseLong(sizeLimit)*1024*1024 )
                                        {
                                            System.out.println("merge file reached sizeLimit: " + fs.getContentSummary(new Path(mergeInto)).getLength());

                                            bw.flush();
                                            fout.flush();

                                            System.out.println("merge file closed");

                                            //delete the original working file copy of the merge file
                                            //update: delete this file after the end of the file read operation
                                            //if delete here then may loose data
                                            //fs.delete(new Path(mergeInto.replace(".merge", "")), true);
                                            arrMergeFile.add(mergeInto.replace(".merge", ""));

                                            //rename the .merge file to original working file name
                                            //now the original file size is around the sizeLimit
                                            //update: will handle after the operation
                                            //merge files are kept in the arrMergeFiles
                                            //fs.rename(new Path(mergeInto), new Path(mergeInto.replace(".merge", "")));
                                            //fs.setTimes( new Path(mergeInto.replace(".merge", "")), orgFileModificationTime, orgFileAccessTime) ;

                                            //open the next merge file
                                            fout = null;
                                            bw = null;
                                            System.gc();

                                            //update: will hande after the operation
                                            //merge files are ept in the arrMergeFiles
                                            /*
                                            if (compress)
                                            {
                                                compress(mergeInto.replace(".merge", ""));

                                                fs.setTimes( new Path(mergeInto.replace(".merge", "").concat(".gz")), orgFileModificationTime, orgFileAccessTime) ;
                                                fs.delete(new Path(mergeInto.replace(".merge", "")), true);
                                            }
                                            */

                                            mergeInto = fileStat.getPath() + ".merge" ;
                                            fout = fs.create(new Path(mergeInto));

                                            bw = new BufferedWriter(new OutputStreamWriter(fout));
                                            System.out.println("new merge file opened: " + mergeInto);
                                            lineCount = 0 ;
                                        }
                                        else
                                        {
                                            lineCount = 0 ;
                                        }
                                    }
                                }
                                //flush the write when the reader file is completed
                                bw.flush();
                                fout.flush();
                                fout.hflush();

                                fs.delete(fileStat.getPath(), true);
                            }

                            fileLimitCtl ++;
                            if (fileLimitCtl >= Integer.parseInt(fileLimit))
                            {
                                break;
                            }
                        }
                    }

                    //after all the files are processed
                    //compress if compress is true
                    //and delete the original file
                    if (compress)
                    {
                        Iterator it = arrMergeFile.listIterator();
                        while (it.hasNext())
                        {
                            String mergeFile = it.next().toString();
                            System.out.println(mergeFile);
                            compress(mergeFile + ".merge");
                            fs.delete(new Path(mergeFile + ".merge"), true);
                        }
                    }

                    //close the fout and bw
                    if (bw != null && fout != null)
                    {
                        bw.flush();
                        fout.flush();

                        bw.close();
                        fout.close();
                    }

                    //rename the last .merge file if it still exists
                    if (!mergeInto.trim().equalsIgnoreCase(""))
                    {
                        if (fs.isFile(new Path(mergeInto)))
                        {
                            fs.rename(new Path(mergeInto), new Path(mergeInto.replace(".merge", "")));
                        }
                    }
                }
                else
                {
                    System.err.println("hdfs_file_merge: path " + hdfsPath + " is not a directory!.");
                }
            }
        }
        catch(Exception ex)
        {
            if (ex.getMessage().contains("Failed to close inode"))
            {
                //we are deleting files we have created
                //when fs is closing, fs is trying to close the lease on created files
                //which is causing this exception
            }
            else
            {
                System.out.println(ex.getMessage());
            }
        }
        finally
        {

        }

        disconnectHadoop();
    }


    public static void compress (String gPath)
    {
        try
        {
            org.apache.hadoop.fs.Path inPath = new org.apache.hadoop.fs.Path(gPath);
            if (fs.exists(inPath))
            {
                if (fs.isFile(inPath))
                {
                    Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
                    Configuration conf = new Configuration();
                    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                    //CompressionOutputStream out = codec.createOutputStream(System.out);
                    CompressionOutputStream out = codec.createOutputStream(fs.create(new Path(gPath + ".gz")));

                    IOUtils.copyBytes(fs.open(inPath), out, 4096, false);
                    out.flush();
                    out.finish();
                }
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
        }
    }

    public static Boolean isOpenForWrite(String gFile)
    {
        Boolean retVal = true;

        try
        {
            DistributedFileSystem dfs = new DistributedFileSystem();
            Path path = new Path(gFile);
            FileSystem fs = path.getFileSystem(conf);
            dfs.initialize(fs.getUri(),conf);

            if (dfs.exists(path) && !dfs.isFileClosed(path) && dfs.isFile(path))
            {
                System.out.println("File " + path + " already opened in write mode");
                retVal = true;
            }
            else
            {
                System.out.println("File " + path + " closed");
                retVal = false;
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            return retVal;
        }
    }

    public static Boolean isCompressed(String gFile)
    {
        Boolean retVal = false;
        String codec = "";
        try
        {
            if (gFile.trim().toLowerCase().split("\\.").length > 0)
            {
                codec = gFile.trim().toLowerCase().split("\\.")[gFile.trim().toLowerCase().split("\\.").length-1];

                if (codec.equalsIgnoreCase("gz") ||
                        codec.equalsIgnoreCase("gzip") ||
                        codec.equalsIgnoreCase("lzo") ||
                        codec.equalsIgnoreCase("lz4") ||
                        codec.equalsIgnoreCase("snappy") ||
                        codec.equalsIgnoreCase("deflate") ||
                        codec.equalsIgnoreCase("bzip2") ||
                        codec.equalsIgnoreCase("bz2")  )
                {
                    retVal = true;
                }
            }

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            return retVal;
        }
    }

    public static void merge (String firstFile, String secondFile)
    {
        try
        {


        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
        }
    }

    public static void connectHadoop()
    {
        try
        {
            conf.set("fs.defaultFS", hdfsConnect );
            conf.set("mapreduce.framework.name", "local");
            conf.set("dfs.replication","1");

            fs = org.apache.hadoop.fs.FileSystem.get(conf);
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {

        }
    }

    public static void disconnectHadoop()
    {
        try
        {
            fs.close();

        }
        catch(org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException leaseEx)
        {

        }
        catch(org.apache.hadoop.ipc.RemoteException remoteEx)
        {
            if (remoteEx.getMessage().toLowerCase().contains("Failed to close inode"))
            {
                //do nothing
                //this exception is expected due to the file deletion after compression
            }
            else
            {
                System.out.println(remoteEx.getMessage());
            }

        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        finally
        {

        }
    }

    //an alternative getSize method
    //reads the file to return the size
    //https://stackoverflow.com/questions/32231105/why-is-hsync-not-flushing-my-hdfs-file
    private long getSize(FileSystem filesys, Path file) throws IOException {
        long length = 0;
        try (final  FSDataInputStream input = filesys.open(file)) {
            while (input.read() >= 0) {
                length++;
            }
        }
        return length;
    }
}
