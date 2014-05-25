package ff5;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;

public class MapFF extends Mapper<IntWritable,Vertex, IntWritable,Vertex> {
	Map<Long,Edge> augEdges = new HashMap<Long,Edge>();
	IntWritable first = new IntWritable();
	IntWritable last = new IntWritable();
	IntWritable vid = new IntWritable();
	Vertex vertex = new Vertex();

	public void setup(Mapper.Context context) {
		Configuration conf = context.getConfiguration();
		try {
			String filePrefix = "ff5/" + conf.get("mf.graph");
			String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
			String roundPrefix = jobPrefix + "/round-"+conf.getInt("mf.round",0);
			String flowsPrefix = roundPrefix + "/flows";

			first.set(0);
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
				Edge edge = new Edge();
				edge.set(u,v,f,Integer.MAX_VALUE);
				augEdges.put(edge.id, edge);
			}
			dis.close();
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	public void cleanup(Context context) {
		try {
			vertex.init(first.get()); context.write(first, vertex);
			vertex.init(last.get()); context.write(last, vertex);
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}
	
	public void map(IntWritable key, Vertex node, Context context) throws IOException, InterruptedException {
		if (first.get()==0) first.set(key.get());
		last.set(key.get());

		long t1 = System.currentTimeMillis();

		int oS = node.nS, oT = node.nT;

		int[] stsaturated = node.updateEdgeFlows(augEdges);

		context.getCounter("ff5","SOURCE_E_SATURATED").increment(stsaturated[0]);
		context.getCounter("ff5","SINK_E_SATURATED").increment(stsaturated[1]);
		if (oS>0 && oS == stsaturated[0]) context.getCounter("ff5","LOSE_EXCESS").increment(1);
		if (oT>0 && oT == stsaturated[1]) context.getCounter("ff5","LOSE_EXCESS").increment(1);

		long t2 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_UPDATE_EDGES").increment(t2-t1);

		//	node.checkSSMeet();

		Random rand = new Random(node.id + node.nE + node.nS + node.nT);

		long t3 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_MEET_IN_THE_MIDDLE").increment(t3-t2);

		if (node.nS > 0){
			int cnt = 0;
			Set<Integer> fset = new HashSet<Integer>();
			for (int i=0; i<node.nS; i++) fset.addAll(node.S[i].fset);
			for (int i=0; i<node.nE; i++){
				Edge edge = node.E[i];
				if (fset.contains(edge.V)) continue;
				if (edge.getResidue(edge.U,edge.V) <= 0) continue;
				Excess ex = node.S[rand.nextInt(node.nS)];
				vertex.S[0].copy(ex);
				vertex.S[0].fset.clear();
				if (vertex.S[0].addRight(node.id, edge.V, edge.F, edge.C)){
					ex.fset.add(edge.V); // take note
					vertex.init(edge.V);
					vertex.nS = 1; // filled in already
					vid.set(vertex.id);
					context.write(vid, vertex);
					cnt++;
				}
			}
			context.getCounter("ff5","EXTEND_SOURCE_E").increment(cnt);
		}

		long t4 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_EXTEND_SOURCE_E").increment(t4-t3);

		if (node.nT > 0){
			int cnt = 0;
			Set<Integer> fset = new HashSet<Integer>();
			for (int i=0; i<node.nT; i++) fset.addAll(node.T[i].fset);
			for (int i=0; i<node.nE; i++){
				Edge edge = node.E[i];
				if (fset.contains(edge.V)) continue;
				if (edge.getResidue(edge.V,edge.U) <= 0) continue;
				Excess ex = node.T[rand.nextInt(node.nT)];
				vertex.T[0].copy(ex);
				vertex.T[0].fset.clear();
				if (vertex.T[0].addLeft(edge.V, node.id, -edge.F, edge.C)){
					ex.fset.add(edge.V);
					vertex.init(edge.V);
					vertex.nT = 1;
					vid.set(vertex.id);
					context.write(vid, vertex);
					cnt++;
				}
			}
			context.getCounter("ff5","EXTEND_SINK_E").increment(cnt);
		}

		long t5 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_EXTEND_SINK_E").increment(t5-t4);
		context.getCounter("ff5","TIME_MAPPER").increment(t5-t1);
	}
}
