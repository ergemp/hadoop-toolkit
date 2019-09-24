package org.ergemp.toolkit.hadoop.processors.hdfs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;

public class FileSplitter {
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

        disconnectHadoop();


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
