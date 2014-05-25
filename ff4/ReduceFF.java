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

public class ReduceFF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
	Map<Long,Edge> augEdges = new HashMap<Long,Edge>();
	String masterPrefix, nextMasterPrefix;
	SequenceFile.Reader mis;
	SequenceFile.Writer mos;
	ApRemote rmi;
	int K; // max number of excess path to be stored on each vertex
	Vertex master = new Vertex();
	Vertex node = new Vertex();
	IntWritable uu = new IntWritable();

	public void setup(Reducer.Context context) {
		Configuration conf = context.getConfiguration();
		K = conf.getInt("mf.excess.list.max", 32);

		int round = conf.getInt("mf.round", 0);
		String filePrefix = "ff4/" + conf.get("mf.graph");
		String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
		String roundPrefix = jobPrefix + "/round-" + round;
		String nextRoundPrefix = jobPrefix + "/round-" + (round + 1);
		String flowsPrefix = roundPrefix + "/flows";
		masterPrefix = roundPrefix + "/master/";
		nextMasterPrefix = nextRoundPrefix + "/master/";

		try {
			String host = conf.get("mf.rmi.host",null);
			Registry registry = LocateRegistry.getRegistry(host);
			rmi = (ApRemote) registry.lookup("ApRemote4");

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

	public void cleanup(Reducer.Context context) {
		try {
			if (mis!=null) mis.close(); mis = null;
			if (mos!=null) mos.close(); mos = null;
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	public void reduce(IntWritable u, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
		long t1 = System.currentTimeMillis();

		if (mis==null){
			Configuration conf = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(conf);
		
			Path curPath = new Path(masterPrefix + u.get());
			mis = new SequenceFile.Reader(hdfs, curPath, conf);

			if (mos!=null) throw new RuntimeException("HEEH");
			Path nextMasterPath = new Path(nextMasterPrefix + u.get());
			hdfs.delete(nextMasterPath, true);
			mos = SequenceFile.createWriter(hdfs, conf,
				nextMasterPath, IntWritable.class, Vertex.class);

			long t2 = System.currentTimeMillis();
			context.getCounter("ff4","TIME_OPEN_HDFS").increment(t2-t1);
		}

		mis.next(uu, master);
		master.updateEdgeFlows(augEdges);
		while (master.id < u.get()){
			mos.append(uu,master);
			mis.next(uu, master);
			master.updateEdgeFlows(augEdges);
		}

		if (master.id != u.get())
			throw new RuntimeException("Join mismatch");

	
		Augmenter As = new Augmenter();
		Augmenter At = new Augmenter();
		node.init(u.get());
		for (Vertex v : values){
			int[] stdrop = node.merge(v,As,At);
			context.getCounter("ff4","SOURCE_E_DROPPED").increment(stdrop[0]);
			context.getCounter("ff4","SINK_E_DROPPED").increment(stdrop[1]);
		}

		if (master.nS==0 && node.nS>0)
			context.getCounter("ff4","SOURCE_MOVE").increment(1);
		if (master.nT==0 && node.nT>0)
			context.getCounter("ff4","SINK_MOVE").increment(1);

		int[] stdrop = node.merge(master,As,At);
		context.getCounter("ff4","SOURCE_E_DROPPED").increment(stdrop[0]);
		context.getCounter("ff4","SINK_E_DROPPED").increment(stdrop[1]);


		if (node.nS > 0 && node.nT > 0){ // an augmenting path is found
			List<List<Edge>> S = new ArrayList<List<Edge>>();
			List<List<Edge>> T = new ArrayList<List<Edge>>();
			for (int i=0; i<node.nS; i++) S.add(node.S[i].compact());
			for (int i=0; i<node.nT; i++) T.add(node.T[i].compact());
			rmi.augment(S, T);
		}

		if (node.nS > K){
			for (int i=0; i<node.nS; i++){
				int j = (int) (Math.random() * node.nS);
				if (i==j) continue;
				Excess te = node.S[i];
				node.S[i] = node.S[j];
				node.S[j] = te;
			}
			context.getCounter("ff4","SOURCE_E_TRUNCATED").increment(node.nS - K);
			node.nS = K;
		}

		if (node.nS > 0){
			context.getCounter("ff4","SOURCE_EPATH_COUNT").increment(1);
			context.getCounter("ff4","SOURCE_EPATH_TOTAL").increment(node.nS);
		}

		if (node.nT > K){
			for (int i=0; i<node.nT; i++){
				int j = (int) (Math.random() * node.nT);
				if (i==j) continue;
				Excess te = node.T[i];
				node.T[i] = node.T[j];
				node.T[j] = te;
			}
			context.getCounter("ff4","SINK_E_TRUNCATED").increment(node.nT - K);
			node.nT = K;
		}

		if (node.nT > 0){
			context.getCounter("ff4","SINK_EPATH_COUNT").increment(1);
			context.getCounter("ff4","SINK_EPATH_TOTAL").increment(node.nT);
		}

		context.getCounter("ff4","N").increment(1);
		context.getCounter("ff4","E").increment(node.nE);

		mos.append(u, node);

		long t2 = System.currentTimeMillis();
		context.getCounter("ff4","TIME_REDUCER").increment(t2-t1);
	}
}

class CombineFF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
	Vertex node = new Vertex();

	public void reduce(IntWritable u, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
		long t1 = System.currentTimeMillis();

		Augmenter As = new Augmenter();
		Augmenter At = new Augmenter();
		node.init(u.get());
		for (Vertex v : values){
			int[] stdrop = node.merge(v,As,At);
			context.getCounter("ff4","SOURCE_E_DROPPED").increment(stdrop[0]);
			context.getCounter("ff4","SINK_E_DROPPED").increment(stdrop[1]);
		}
		context.write(u, node);

		long t2 = System.currentTimeMillis();
		context.getCounter("ff4","TIME_COMBINER").increment(t2-t1);
	}
}
