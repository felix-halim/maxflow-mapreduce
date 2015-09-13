package bfs;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class bfsRead extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		SequenceFile.Reader reader = new SequenceFile.Reader(
			FileSystem.get(getConf()), new Path("graph-bfs-"+args[0]), getConf());
		IntWritable key = new IntWritable();
		Vertex node = new Vertex(1);
		while (reader.next(key,node))
			System.out.printf("%d\t%d\n",key.get(),node.distance);
		reader.close();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new bfsRead(), args));
	}
}
