package com.ocac.HDFS;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSSample {
	public static final String inputfile="hdfsinput.txt";

	public static final String inputmsg="count the amount of words in the sentence";
	

	public static void main(String[] args)  throws IOException{
		//create a default hadoop configuration
		Configuration config=new Configuration();
		//parse created config to the HDFS
		FileSystem fs=FileSystem.get(config);
		//specifies a new file in HDFS
		Path filenamepath=new Path(inputfile);
		try{
			//if the file already exists delete it 
			if(fs.exists(filenamepath)){
				//remove the file
				fs.delete(filenamepath,true);
		}
		//Fsoutputstream to write the input msg into the HDFS file
		FSDataOutputStream fin=fs.create(filenamepath);
		fin.writeUTF(inputmsg);
		fin.close();
		//FsInputStream to read out of the file name path file
		FSDataInputStream fout=fs.open(filenamepath);
		String msgIn=fout.readUTF();
		//print to screen
		System.out.println(msgIn);
		fout.close();
	}
	catch(IOException ioe)
	{
		System.err.println("IoException during operation"+ioe.toString());	
			System.exit(1);	
	}
	}
}





