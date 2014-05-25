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

public class ReduceFF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
	Map<Long,Edge> augEdges = new HashMap<Long,Edge>();
	String masterPrefix, updatedMasterPrefix, nextMasterPrefix;
	SequenceFile.Reader mis;
	SequenceFile.Writer mos;
	ApRemote rmi;
	int K; // max number of excess path to be stored on each vertex

	public void setup(Reducer.Context context) {
		Configuration conf = context.getConfiguration();
		K = conf.getInt("mf.excess.list.max", 32);

		int round = conf.getInt("mf.round", 0);
		String filePrefix = "ff3/" + conf.get("mf.graph");
		String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
		String roundPrefix = jobPrefix + "/round-" + round;
		String nextRoundPrefix = jobPrefix + "/round-" + (round + 1);
		String flowsPrefix = roundPrefix + "/flows";
		masterPrefix = roundPrefix + "/master/";
		updatedMasterPrefix = roundPrefix + "/updated/";
		nextMasterPrefix = nextRoundPrefix + "/master/";

		try {
			String host = conf.get("mf.rmi.host",null);
			Registry registry = LocateRegistry.getRegistry(host);
			rmi = (ApRemote) registry.lookup("ApRemote3");

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

	private long take(Collection<Excess> c, int mx){
		if (c.size() > mx){
			long ret = c.size() - mx;
			List<Excess> L = new ArrayList<Excess>(c);
			Collections.shuffle(L);
			c.clear();
			for (int i=0; i<mx; i++)
				c.add(L.get(i));
			return ret;
		}
		return 0;
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
			mos = SequenceFile.createWriter(hdfs, conf,
				new Path(nextMasterPrefix + u.get()),
				IntWritable.class, Vertex.class);

			long t2 = System.currentTimeMillis();
			context.getCounter("FF3","TIME_OPEN_HDFS").increment(t2-t1);
		}

		Vertex master = new Vertex();
		IntWritable uu = new IntWritable();

		try {
			mis.next(uu, master);
			master.updateEdgeFlows(augEdges);
			while (master.getId() < u.get()){
				mos.append(uu,master);
				mis.next(uu, master);
				master.updateEdgeFlows(augEdges);
			}
		} catch (Exception ex){
			throw new RuntimeException("EOF keknya: cur = " + master.getId() + " need = " + u.get());
		}

		if (master.getId() != u.get())
			throw new RuntimeException("Join mismatch");

		
		Augmenter As = new Augmenter();
		Augmenter At = new Augmenter();
		Vertex node = new Vertex(u.get());
		for (Vertex v : values){
			int[] stdrop = node.merge(v,As,At);
			context.getCounter("FF3","SOURCE_E_DROPPED").increment(stdrop[0]);
			context.getCounter("FF3","SINK_E_DROPPED").increment(stdrop[1]);
		}

		if (master.getS().size()==0 && node.getS().size()>0)
			context.getCounter("FF3","SOURCE_MOVE").increment(1);
		if (master.getT().size()==0 && node.getT().size()>0)
			context.getCounter("FF3","SINK_MOVE").increment(1);

		int[] stdrop = node.merge(master,As,At);
		context.getCounter("FF3","SOURCE_E_DROPPED").increment(stdrop[0]);
		context.getCounter("FF3","SINK_E_DROPPED").increment(stdrop[1]);

		List<Excess> S = node.getS(), T = node.getT();

		if (S.size() > 0 && T.size() > 0){ // an augmenting path is found
			rmi.augment(S, T);
		}

		context.getCounter("FF3","SOURCE_E_TRUNCATED").increment(take(S, K));
		if (S.size() > 0){
			context.getCounter("FF3","SOURCE_EPATH_COUNT").increment(1);
			context.getCounter("FF3","SOURCE_EPATH_TOTAL").increment(S.size());
		}

		context.getCounter("FF3","SINK_E_TRUNCATED").increment(take(T, K));
		if (T.size() > 0){
			context.getCounter("FF3","SINK_EPATH_COUNT").increment(1);
			context.getCounter("FF3","SINK_EPATH_TOTAL").increment(T.size());
		}

		context.getCounter("FF3","N").increment(1);
		context.getCounter("FF3","E").increment(node.getEdges().size());

		mos.append(new IntWritable(node.getId()), node);

		long t2 = System.currentTimeMillis();
		context.getCounter("FF3","TIME_REDUCER").increment(t2-t1);
	}
}

class CombineFF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
	public void reduce(IntWritable u, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
		long t1 = System.currentTimeMillis();

		Augmenter As = new Augmenter();
		Augmenter At = new Augmenter();
		Vertex node = new Vertex(u.get());
		for (Vertex v : values){
			int[] stdrop = node.merge(v,As,At);
			context.getCounter("FF3","SOURCE_E_DROPPED").increment(stdrop[0]);
			context.getCounter("FF3","SINK_E_DROPPED").increment(stdrop[1]);
		}
		context.write(new IntWritable(node.getId()), node);

		long t2 = System.currentTimeMillis();
		context.getCounter("FF3","TIME_COMBINER").increment(t2-t1);
	}
}
