package ff3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;

public class MapFF extends Mapper<IntWritable,Vertex, IntWritable,Vertex> {
	Map<Long,Edge> augEdges = new HashMap<Long,Edge>();
//	SequenceFile.Writer mos;
	IntWritable first, last;
	String updatedMasterPrefix;

	public void setup(Mapper.Context context) {
		Configuration conf = context.getConfiguration();
		try {
			String filePrefix = "ff3/" + conf.get("mf.graph");
			String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
			String roundPrefix = jobPrefix + "/round-"+conf.getInt("mf.round",0);
			String flowsPrefix = roundPrefix + "/flows";
			updatedMasterPrefix = roundPrefix + "/updated/";

			first = last = null;
			FileSystem hdfs = FileSystem.get(conf);
			Path path = new Path(flowsPrefix);
			if (!hdfs.exists(path)) return;
			augEdges.clear();
			FSDataInputStream dis = hdfs.open(path);
			int N = dis.readInt();
			while (N-- > 0){
				int u = dis.readInt();
				int v = dis.readInt();
				int f = dis.readInt();
				Edge edge = new Edge(u,v,f,Integer.MAX_VALUE);
				augEdges.put(edge.getId(), edge);
			}
			dis.close();
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	public void cleanup(Context context) {
		try {
			context.write(first, new Vertex(first.get()));
			context.write(last, new Vertex(last.get()));
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}

		try {
//			mos.close(); mos = null;
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
	
	public void map(IntWritable key, Vertex node, Context context) throws IOException, InterruptedException {
		if (first==null) first = new IntWritable(key.get());
		last = new IntWritable(key.get());

/*
		if (mos==null){
			Configuration conf = context.getConfiguration();
			mos = SequenceFile.createWriter(FileSystem.get(conf), conf, 
				new Path(updatedMasterPrefix + key.get()),
				IntWritable.class, Vertex.class);
		}
*/

		long t1 = System.currentTimeMillis();

		int oS = node.getS().size(), oT = node.getT().size();

		int[] stsaturated = node.updateEdgeFlows(augEdges);

		context.getCounter("FF3","SOURCE_E_SATURATED").increment(stsaturated[0]);
		context.getCounter("FF3","SINK_E_SATURATED").increment(stsaturated[1]);
		if (oS>0 && oS == stsaturated[0]) context.getCounter("FF3","LOSE_EXCESS").increment(1);
		if (oT>0 && oT == stsaturated[1]) context.getCounter("FF3","LOSE_EXCESS").increment(1);

		long t2 = System.currentTimeMillis();
		context.getCounter("FF3","TIME_UPDATE_EDGES").increment(t2-t1);

		//	node.checkSSMeet();

		long t3 = System.currentTimeMillis();
		context.getCounter("FF3","TIME_MEET_IN_THE_MIDDLE").increment(t3-t2);

		if (node.getS().size()>0){
			List<Excess> exs = node.extendS();
			context.getCounter("FF3","EXTEND_SOURCE_E").increment(exs.size());
			for (Excess ex : exs){
				Vertex v = new Vertex(ex.getTo());
				v.addS(ex);
				context.write(new IntWritable(v.getId()), v);
			}
		}

		long t4 = System.currentTimeMillis();
		context.getCounter("FF3","TIME_EXTEND_SOURCE_E").increment(t4-t3);

		if (node.getT().size()>0){
			List<Excess> exs = node.extendT();
			context.getCounter("FF3","EXTEND_SINK_E").increment(exs.size());
			for (Excess ex : exs){
				Vertex v = new Vertex(ex.getFrom());
				v.addT(ex);
				context.write(new IntWritable(v.getId()), v);
			}
		}

		long t5 = System.currentTimeMillis();
		context.getCounter("FF3","TIME_EXTEND_SINK_E").increment(t5-t4);

//		mos.append(new IntWritable(node.getId()), node);

		context.getCounter("FF3","TIME_MAPPER").increment(t5-t1);
	}
}
