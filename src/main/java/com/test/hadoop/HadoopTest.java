package com.test.hadoop;

import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.util.ResourceUtils;

public class HadoopTest {
	
	private final static String excelName = "goodsBrand";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HadoopTest hadoopTest = new HadoopTest();
		//hadoopTest.execWriter();
		hadoopTest.execReader();
	}

	public void execReader() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			conf.set("fs.defaultFS","hdfs://localhost:9000");
			FileSystem fs = FileSystem.get(conf);
			// 模板地址
			//String filePath = "classpath:static/file/"+ excelName +".xlsx";
			//File xlsFile = ResourceUtils.getFile(filePath);
			Path file =new Path("test");  
			FSDataInputStream getIt = fs.open(file);
			
			BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
			String content = d.readLine();
			System.out.println("-----content-----"+content);
			d.close();
			fs.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void execWriter() {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","hdfs://localhost:9000");
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			FileSystem fs = FileSystem.get(conf);
			// 模板地址
			//String filePath = "classpath:static/file/"+ excelName +".xlsx";
			//File xlsFile = ResourceUtils.getFile(filePath);
			byte[] buff ="Hello World".getBytes();
			String filename ="test"; 
			FSDataOutputStream os = fs.create(new Path(filename));
			os.write(buff, 0, buff.length);
		
			System.out.println("Create:"+filename);
			os.close();
			fs.close();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
