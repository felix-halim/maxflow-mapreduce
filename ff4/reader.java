package ff4;

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
		/*
		Path curPath = new Path("ff4/fb1/schimmy.compg5/round-8/master/-26940203");
		SequenceFile.Reader mis = new SequenceFile.Reader(hdfs, curPath, conf);
		IntWritable u = new IntWritable();
		Vertex v = new Vertex();
		while (mis.next(u,v)) System.out.println(v);
		mis.close();
		*/

		Path path = new Path("ff5/fb8/m0001.ff5.fb8.300r.flow75.s512.sb10000.compg5.randss256/round-2/flows");
		FSDataInputStream dis = hdfs.open(path);
		int N = dis.readInt();
		while (N-- > 0){
			int u = dis.readInt();
			int v = dis.readInt();
			int f = dis.readInt();
			System.out.printf("%d %d = %d\n",u,v,f);
		}
	}
}
