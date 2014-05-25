package ff3;

import java.io.*;
import java.util.*;
import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

public class reader {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path curPath = new Path("ff3/fb1/schimmy.compg5/round-8/master/-26940203");
		SequenceFile.Reader mis = new SequenceFile.Reader(hdfs, curPath, conf);
		IntWritable u = new IntWritable();
		Vertex v = new Vertex();
		while (mis.next(u,v)) System.out.println(v);
		mis.close();
	}
}
