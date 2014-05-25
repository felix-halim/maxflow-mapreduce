package ff5;

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
	Vertex master = new Vertex();
	Vertex node = new Vertex();
	IntWritable uu = new IntWritable();
	boolean zipMaster;

	public void setup(Reducer.Context context) {
		Configuration conf = context.getConfiguration();

		int round = conf.getInt("mf.round", 0);
		String filePrefix = "ff5/" + conf.get("mf.graph");
		String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
		String roundPrefix = jobPrefix + "/round-" + round;
		String nextRoundPrefix = jobPrefix + "/round-" + (round + 1);
		String flowsPrefix = roundPrefix + "/flows";
		masterPrefix = roundPrefix + "/master/";
		nextMasterPrefix = nextRoundPrefix + "/master/";
		zipMaster = conf.getBoolean("mf.zip.master",true);

		try {
			String host = conf.get("mf.rmi.host",null);
			Registry registry = LocateRegistry.getRegistry(host);
			rmi = (ApRemote) registry.lookup("ApRemote5");

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

	public void updateMaster(Context context){
		master.updateEdgeFlows(augEdges);

		Random rand = new Random(master.id + master.nE + master.nS + master.nT);
		
		if (master.nS > 0){
			int prevented = 0;
			Set<Integer> fset = new HashSet<Integer>();
			for (int i=0; i<master.nS; i++) fset.addAll(master.S[i].fset);
			for (int i=0; i<master.nE; i++){
				Edge edge = master.E[i];
				if (fset.contains(edge.V)) continue;
				if (edge.getResidue(edge.U,edge.V) <= 0) continue;
				Excess ex = master.S[rand.nextInt(master.nS)];
				node.S[0].copy(ex);
				node.S[0].fset.clear();
				if (node.S[0].addRight(master.id, edge.V, edge.F, edge.C)){
					ex.fset.add(edge.V); // take note
					prevented++;
				}
			}
			context.getCounter("ff5","SOURCE_E_PREVENTED").increment(prevented);
		}

		if (master.nT > 0){
			int prevented = 0;
			Set<Integer> fset = new HashSet<Integer>();
			for (int i=0; i<master.nT; i++) fset.addAll(master.T[i].fset);
			for (int i=0; i<master.nE; i++){
				Edge edge = master.E[i];
				if (fset.contains(edge.V)) continue;
				if (edge.getResidue(edge.V,edge.U) <= 0) continue;
				Excess ex = master.T[rand.nextInt(master.nT)];
				node.T[0].copy(ex);
				node.T[0].fset.clear();
				if (node.T[0].addLeft(edge.V, master.id, -edge.F, edge.C)){
					ex.fset.add(edge.V);
					prevented++;
				}
			}
			context.getCounter("ff5","SINK_E_PREVENTED").increment(prevented);
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

			if (zipMaster){
				mos = SequenceFile.createWriter(hdfs, conf,
					nextMasterPath, IntWritable.class, Vertex.class,
					SequenceFile.CompressionType.BLOCK);
			} else {
				mos = SequenceFile.createWriter(hdfs, conf,
					nextMasterPath, IntWritable.class, Vertex.class);
			}

			long t2 = System.currentTimeMillis();
			context.getCounter("ff5","TIME_OPEN_HDFS").increment(t2-t1);
		}

		long t2 = System.currentTimeMillis();

		mis.next(uu, master);

		long t3 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_READ_HDFS").increment(t3-t2);

		updateMaster(context);
		while (master.id < u.get()){
			long t4 = System.currentTimeMillis();
				mos.append(uu,master);
			long t5 = System.currentTimeMillis();
			context.getCounter("ff5","TIME_WRITE_HDFS").increment(t5-t4);
	
				mis.next(uu, master);
		
			long t6 = System.currentTimeMillis();
			context.getCounter("ff5","TIME_READ_HDFS").increment(t6-t5);
			updateMaster(context);
		}


		if (master.id != u.get())
			throw new RuntimeException("Join mismatch");

	
		Augmenter As = new Augmenter();
		Augmenter At = new Augmenter();
		node.init(u.get());
		
		int[] stdrop = node.merge(master,As,At);
		context.getCounter("ff5","SOURCE_E_DROPPED").increment(stdrop[0]);
		context.getCounter("ff5","SINK_E_DROPPED").increment(stdrop[1]);

		int som = 0, sim = 0;
		for (Vertex v : values){
			stdrop = node.merge(v,As,At);
			context.getCounter("ff5","SOURCE_E_DROPPED").increment(stdrop[0]);
			context.getCounter("ff5","SINK_E_DROPPED").increment(stdrop[1]);
		}

		if (master.nS==0 && node.nS>0)
			context.getCounter("ff5","SOURCE_MOVE").increment(1);
		if (master.nT==0 && node.nT>0)
			context.getCounter("ff5","SINK_MOVE").increment(1);



		long t7 = System.currentTimeMillis();
		if (node.nS > 0 && node.nT > 0){ // an augmenting path is found
			List<List<Edge>> S = new ArrayList<List<Edge>>();
			List<List<Edge>> T = new ArrayList<List<Edge>>();
			for (int i=0; i<node.nS; i++) S.add(node.S[i].compact());
			for (int i=0; i<node.nT; i++) T.add(node.T[i].compact());
			rmi.augment(S, T);
		}
		long t8 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_RMI_CALL").increment(t8-t7);

		if (node.nS > 0){
			context.getCounter("ff5","SOURCE_EPATH_COUNT").increment(1);
			context.getCounter("ff5","SOURCE_EPATH_TOTAL").increment(node.nS);
		}

		if (node.nT > 0){
			context.getCounter("ff5","SINK_EPATH_COUNT").increment(1);
			context.getCounter("ff5","SINK_EPATH_TOTAL").increment(node.nT);
		}

		context.getCounter("ff5","N").increment(1);
		context.getCounter("ff5","E").increment(node.nE);

		long t4 = System.currentTimeMillis();
			mos.append(u, node);
		long t5 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_WRITE_HDFS").increment(t5-t4);

		long t6 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_REDUCER").increment(t6-t1);
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
			context.getCounter("ff5","SOURCE_E_DROPPED").increment(stdrop[0]);
			context.getCounter("ff5","SINK_E_DROPPED").increment(stdrop[1]);
		}
		context.write(u, node);

		long t2 = System.currentTimeMillis();
		context.getCounter("ff5","TIME_COMBINER").increment(t2-t1);
	}
}
