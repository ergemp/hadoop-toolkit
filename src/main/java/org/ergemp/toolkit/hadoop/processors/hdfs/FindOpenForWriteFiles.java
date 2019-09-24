package org.ergemp.toolkit.hadoop.processors.hdfs;

import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.GenericOptionsParser;

public class FindOpenForWriteFiles {
    private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    private static org.apache.hadoop.fs.FileSystem fs;

    private static String hdfsConnect="hdfs://localhost:9000";
    private static String hdfsPath="/";

    public static void main(String[] args)
    {
        Options options = new Options();
        options.addOption("hdfsConnect", true, "connection string for hdfs. ex: hdfs://<hostname>:<port>");
        options.addOption("hdfsPath", true, "path to search for retained files. ex: /<folder_name>");

        try
        {
            GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = optionParser.getRemainingArgs();

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
            System.out.println("-----------------------");
        }

        connectHadoop();

        try
        {
            if (!fs.exists(new Path("/tmpfolder/openfile2")))
            {
                fs.create(new Path("/tmpfolder/openfile2"));
            }
            System.out.println(isOpenForWrite("/tmpfolder/openfile2").toString());
            //fs.close();
            System.out.println(isOpenForWrite("/tmpfolder/openfile2").toString());

            FindOpenForWriteFiles.listOpenForWrite("/");
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
        }

        disconnectHadoop();
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

    public static void listOpenForWrite(String gFile)
    {
        Boolean retVal = true;
        DistributedFileSystem dfs = new DistributedFileSystem();
        Path path = new Path(gFile);

        try
        {
            org.apache.hadoop.fs.FileStatus[] fileStats = fs.listStatus(path);
            dfs.initialize(fs.getUri(),conf);

            for (FileStatus fileStat : fileStats)
            {
                if (dfs.isFile(fileStat.getPath()))
                {
                    if (!dfs.isFileClosed(fileStat.getPath()))
                    {
                        System.out.println("File " + fileStat.getPath() + " already opened in write mode");
                    }
                }
                else
                {
                    listOpenForWrite(fileStat.getPath().toString());
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (Exception ex)
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
        catch(Exception ex)
        {
            //ex.printStackTrace();
        }
        finally
        {

        }
    }
}
