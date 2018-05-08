package com.test.hadoop;

import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;  

public class AppTest {
	private static final String HDFS_PATH = "hdfs://localhost:9000";  
    private static final String DIR_PATH = "/test";  
    private static final String FILE_PATH = "/test/data"; 
	public static void main(String[] args) {
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
			//创建文件夹
			//MyFSDataInputStream.makeDir(fileSystem,DIR_PATH);
			//上传文件
			//MyFSDataOutputStream.uploadData(fileSystem, FILE_PATH);
			//读取文件
			MyFSDataInputStream.downloadData(fileSystem, FILE_PATH);
			System.out.println("--------------------END---------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}	
}
