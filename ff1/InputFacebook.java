package ff1;

import mf.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

public class InputFacebook extends Configured implements Tool {
	int check(long id){
		if (id >= 100000000000000L) id = 100000000000000L - id;
		if (id < Integer.MIN_VALUE) throw new RuntimeException("fail : " + id);
		if (id > Integer.MAX_VALUE) throw new RuntimeException("fail : " + id);
		if (id==1 || id==2) throw new RuntimeException("Reserved ID : " + id);
		return (int) id;
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("dfs.replication","2");
		conf.set("dfs.block.size","67108864");
		conf.set("mapred.output.compress","false");
		conf.set("mapred.compress.map.output","false");

		long startTime = System.currentTimeMillis();
		String inputFile = args[0], inputGraph = args[1];
		System.out.printf("Run Facebook %s -> %s\n",inputFile,inputGraph);
		InputStream is = new FileInputStream(inputFile);
		if (inputFile.endsWith(".gz")) is = new GZIPInputStream(is);
		BufferedReader bf = new BufferedReader(new InputStreamReader(is));
		FileSystem hdfs = FileSystem.get(conf);

		hdfs.delete(new Path(inputGraph), true);
		SequenceFile.Writer writer = SequenceFile.createWriter(
			hdfs, conf, new Path(inputGraph),IntWritable.class, Vertex.class);
		int nRecords = 0;
		for (String s=bf.readLine(); s!=null; s=bf.readLine()){
			String[] ss = s.split("\t");
			try {
				Vertex node = new Vertex(check(Long.parseLong(ss[0])));
				for (int i=2; i<ss.length; i++){
					try {
						int to = check(Long.parseLong(ss[i]));
						if (to==node.getId()) continue;
						node.addEdge(new Edge(node.getId(),to,0,0,0));
					} catch (Exception ex){ continue; }
				}
				writer.append(new IntWritable(node.getId()), node);
				nRecords++;
				if (nRecords%1000000==0){ System.err.print("."); System.err.flush(); }
			} catch (Exception ex){ continue; }
		}
		writer.append(new IntWritable(1), new Vertex(1)); // source
		writer.append(new IntWritable(2), new Vertex(2)); // sink
		writer.close(); bf.close(); System.err.println(); System.err.flush();
		long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
		System.err.printf("Time = %02d:%02d\n",elapsedTime/60,elapsedTime%60);
		return 0;
	}
}
