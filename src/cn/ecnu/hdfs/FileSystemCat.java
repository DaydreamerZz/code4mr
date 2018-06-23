package cn.ecnu.hdfs;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import sun.nio.ch.IOUtil;

public class FileSystemCat {

	public static void main(String[] args) throws Exception{
		
		String urlString = "hdfs://localhost:9000/user/hadoop/keyword";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(urlString), conf);
		
		InputStream in = null;
		try{
			in = fs.open(new Path(urlString));
			IOUtils.copyBytes(in, System.out, 4096, false);
			
			
		}finally{
			IOUtils.closeStream(in);
		}
		
		System.out.println("------------------------------------");
		
		FSDataInputStream fsin = null;
		try{
			fsin = fs.open(new Path(urlString));
			IOUtils.copyBytes(fsin, System.out, 4096, false);
			
			fsin.seek(3);
			IOUtils.copyBytes(fsin, System.out, 4096, false);
			
		}finally{
			IOUtils.closeStream(fsin);
		}
	}
}
