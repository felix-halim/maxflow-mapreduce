package ff2;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

public class FordFulkerson extends Configured implements Tool {
	public static class MRBase extends MapReduceBase {
		Map<Long,Edge> augEdges = new HashMap<Long,Edge>(); // the augmented edges from the previous round.
		FileSystem hdfs; // a reference to the HDFS to globally store the augmented paths
		int[] sources, sinks;
		int excessListMaxSize; // how many simple paths stored for the excess paths
		int maxRandomCapacity;
		int round;
		String graph;
		ApRemote AP;

		public void configure(JobConf job) {
			super.configure(job);

			round = job.getInt("mf.round", 0);
			graph = job.get("mf.graph")+"-"+job.get("mf.comment");
			maxRandomCapacity = job.getInt("mf.max.random.capacity", 1);
			excessListMaxSize = job.getInt("mf.excess.list.max", 512);

			String[] ss = job.get("mf.sources", "null").split(",");
			sources = new int[ss.length];
			for (int i=0; i<ss.length; i++) sources[i] = Integer.parseInt(ss[i]);
			ss = job.get("mf.sinks", "null").split(",");
			sinks = new int[ss.length];
			for (int i=0; i<ss.length; i++) sinks[i] = Integer.parseInt(ss[i]);

			try {
				Registry registry = LocateRegistry.getRegistry(job.get("mf.rmi.host",null));
				AP = (ApRemote) registry.lookup("ApRemote");

				hdfs = FileSystem.get(job);
			} catch (Exception ex){
				throw new RuntimeException(ex);
			}
		}
	}

	public static class MapFF extends MRBase implements org.apache.hadoop.mapred.Mapper<IntWritable,Vertex, IntWritable,Vertex> {
		public void configure(JobConf job) {
			super.configure(job);

			try {
				String p = "flows-ff2-"+graph+"-"+round;
				Path path = new Path(p);
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

		public void map(IntWritable key, Vertex node, OutputCollector<IntWritable, Vertex> output, Reporter reporter) throws IOException {
			long t1 = System.currentTimeMillis();

			if (round==0){
				if (node.getId()==1){
					for (int id : sources) node.addNewEdge(id, 0, 0);
					node.addS(new Excess());
				} else if (node.getId()==2){
					for (int id : sinks) node.addNewEdge(id, 0, 0);
					node.addT(new Excess());
				}
				for (Edge edge : node.getEdges()){
					if (edge.getTo()==node.getId()) throw new RuntimeException("Self Loop");
					Vertex v = new Vertex(edge.getTo());
					v.addNewEdge(edge.getFrom(), 0, 0);
					output.collect(new IntWritable(v.getId()),v);
				}
				output.collect(new IntWritable(node.getId()), node);
			} else {
				int oS = node.getS().size(), oT = node.getT().size();

				int[] stsaturated = node.updateEdgeFlows(augEdges);

				reporter.incrCounter(mf.MFCounter.SOURCE_E_SATURATED, stsaturated[0]);
				reporter.incrCounter(mf.MFCounter.SINK_E_SATURATED, stsaturated[1]);

				if (oS>0 && oS == stsaturated[0]){
					node.setBroadcast(true);
					reporter.incrCounter(mf.MFCounter.LOSE_EXCESS, 1);
					for (Edge e : node.getEdges()){
						Vertex v = new Vertex(e.getTo());
						v.setBroadcast(true);
						output.collect(new IntWritable(v.getId()), v);
					}
				}

				if (oT>0 && oT == stsaturated[1]){
					node.setBroadcast(true);
					reporter.incrCounter(mf.MFCounter.LOSE_EXCESS, 1);
					for (Edge e : node.getEdges()){
						Vertex v = new Vertex(e.getTo());
						v.setBroadcast(true);
						output.collect(new IntWritable(v.getId()), v);
					}
				}

				long t2 = System.currentTimeMillis();
				reporter.incrCounter(mf.MFCounter.TIME_UPDATE_EDGES, t2-t1);

				//	node.checkSSMeet();

				long t3 = System.currentTimeMillis();
				reporter.incrCounter(mf.MFCounter.TIME_MEET_IN_THE_MIDDLE, t3-t2);

				if (node.getS().size()>0 && node.getT().size()>0 && false){
					String S = "Source Excess :\n", T = "Sink Excess :\n";
					for (Excess se : node.getS()) S += se.toString() + "\n";
					for (Excess te : node.getT()) T += te.toString() + "\n";
					throw new RuntimeException(S + T + "Missed Augmenting Path: " + 
						node.getS().size() + " " + node.getT().size());
				}
				
				if (node.getS().size()>0 /* && node.getBroadcast() */){
					List<Excess> exs = node.extendS();
					reporter.incrCounter(mf.MFCounter.EXTEND_SOURCE_E, exs.size());
					for (Excess ex : exs){
						Vertex v = new Vertex(ex.getTo());
						v.addS(ex);
						output.collect(new IntWritable(v.getId()), v);
					}
					node.setBroadcast(false);
				}

				long t4 = System.currentTimeMillis();
				reporter.incrCounter(mf.MFCounter.TIME_EXTEND_SOURCE_E, t4-t3);

				if (node.getT().size()>0 /* && node.getBroadcast() */){
					List<Excess> exs = node.extendT();
					reporter.incrCounter(mf.MFCounter.EXTEND_SINK_E, exs.size());
					for (Excess ex : exs){
						Vertex v = new Vertex(ex.getFrom());
						v.addT(ex);
						output.collect(new IntWritable(v.getId()), v);
					}
					node.setBroadcast(false);
				}

				long t5 = System.currentTimeMillis();
				reporter.incrCounter(mf.MFCounter.TIME_EXTEND_SINK_E, t5-t4);

				output.collect(new IntWritable(node.getId()), node);
			}

			long t2 = System.currentTimeMillis();
			reporter.incrCounter(mf.MFCounter.TIME_MAPPER, t2-t1);
		}
	}

	public static class ReduceFF extends MRBase implements org.apache.hadoop.mapred.Reducer<IntWritable, Vertex, IntWritable, Vertex> {
		Vertex node, master;

		protected void merge(Vertex a, Vertex b, Reporter reporter, Augmenter As, Augmenter At){
			int[] stdrop = a.merge(b,As,At);
			reporter.incrCounter(mf.MFCounter.SOURCE_E_DROPPED, stdrop[0]);
			reporter.incrCounter(mf.MFCounter.SINK_E_DROPPED, stdrop[1]);
		}

		protected void parseNodes(IntWritable u, Iterator<Vertex> values, Reporter reporter, Augmenter As, Augmenter At){
			node = new Vertex(u.get());
			master = null;
			while (values.hasNext()){
				Vertex v = values.next();
				if (round>0 && v.getEdges().size()>0){
					if (master!=null) throw new RuntimeException("Two Masters");
					master = new Vertex(v);
				} else {
					merge(node,v,reporter,As,At);
				}
			}
		}

		private int take(Collection c, int mx){
			if (c.size() > mx){
				int ret = c.size() - mx;
				List L = new ArrayList(c);
				Collections.shuffle(L);
				c.clear();
				for (int i=0; i<mx; i++)
					c.add(L.get(i));
				return ret;
			}
			return 0;
		}

		public void reduce(IntWritable u, Iterator<Vertex> values, OutputCollector<IntWritable, Vertex> output, Reporter reporter) throws IOException {
			long t1 = System.currentTimeMillis();
			
			/*
			AUGPATH_CANDIDATES,
			*/

			Augmenter As = new Augmenter();
			Augmenter At = new Augmenter();
			parseNodes(u,values,reporter,As,At);

			if (master!=null){
				if (master.getS().size()==0 && node.getS().size()>0)
					reporter.incrCounter(mf.MFCounter.SOURCE_MOVE, 1);
				if (master.getT().size()==0 && node.getT().size()>0)
					reporter.incrCounter(mf.MFCounter.SINK_MOVE, 1);
				merge(node,master,reporter,As,At);
			} else if (round>0) throw new RuntimeException("No Master");

			if (round==0){
				if (node.getEdges().size()==0) return;
				int U = node.getId();
				for (Edge e : node.getEdges()){
					int V = e.getTo();
					if (U==1 || U==2 || V==1 || V==2){
						e.setCapacity(10000000);
					} else {
						e.setCapacity((int)(Math.random()*maxRandomCapacity)+1);
					}
				}
				node.setBroadcast(true); // initially, set it to full broadcast
			}

			List<Excess> S = node.getS(), T = node.getT();

			if (S.size() > 0 && T.size() > 0){ // an augmenting path is found
				AP.augment(S, T);
				node.setBroadcast(true); // this is on the edge, actively broadcast S and T
			}

			reporter.incrCounter(mf.MFCounter.SOURCE_E_TRUNCATED, take(S, excessListMaxSize));
			if (S.size() > 0){
				reporter.incrCounter(mf.MFCounter.SOURCE_EPATH_COUNT, 1);
				reporter.incrCounter(mf.MFCounter.SOURCE_EPATH_TOTAL, S.size());
			}

			reporter.incrCounter(mf.MFCounter.SINK_E_TRUNCATED, take(T, excessListMaxSize));
			if (T.size() > 0){
				reporter.incrCounter(mf.MFCounter.SINK_EPATH_COUNT, 1);
				reporter.incrCounter(mf.MFCounter.SINK_EPATH_TOTAL, T.size());
			}

			reporter.incrCounter(mf.MFCounter.N, 1);
			reporter.incrCounter(mf.MFCounter.E, node.getEdges().size());
			reporter.incrCounter(mf.MFCounter.BROADCAST, node.getBroadcast()?1:0);
			output.collect(new IntWritable(node.getId()), node);

			long t2 = System.currentTimeMillis();
			reporter.incrCounter(mf.MFCounter.TIME_REDUCER, t2-t1);
		}
	}

	public static class CombineFF extends ReduceFF {
		public void reduce(IntWritable u, Iterator<Vertex> values, OutputCollector<IntWritable, Vertex> output, Reporter reporter) throws IOException {
			long t1 = System.currentTimeMillis();

			Augmenter As = new Augmenter();
			Augmenter At = new Augmenter();
			parseNodes(u,values,reporter,As,At);

			if (master != null) output.collect(new IntWritable(master.getId()), master);
			if (!node.isEmpty()) output.collect(new IntWritable(node.getId()), node);

			long t2 = System.currentTimeMillis();
			reporter.incrCounter(mf.MFCounter.TIME_COMBINER, t2-t1);
		}
	}



	int check(long id){
		if (id >= 100000000000000L) id = 100000000000000L - id;
		if (id < Integer.MIN_VALUE) throw new RuntimeException("fail : " + id);
		if (id > Integer.MAX_VALUE) throw new RuntimeException("fail : " + id);
		if (id==1 || id==2) throw new RuntimeException("Reserved ID : " + id);
		return (int) id;
	}

	public int runInput(String[] args) throws IOException {
		Configuration conf = getConf();

		long startTime = System.currentTimeMillis();
		String inputFile = args[0], inputGraph = "graph-ff2-"+args[1];
		System.out.printf("Run Facebook %s -> %s\n",inputFile,inputGraph);
		InputStream is = new FileInputStream(inputFile);
		if (inputFile.endsWith(".gz")) is = new GZIPInputStream(is);
		BufferedReader bf = new BufferedReader(new InputStreamReader(is));
		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(new Path(inputGraph))) hdfs.delete(new Path(inputGraph));
		SequenceFile.Writer writer = SequenceFile.createWriter(
			hdfs, conf, new Path(inputGraph),IntWritable.class, Vertex.class);
		int nRecords = 0;
		for (String s=bf.readLine(); s!=null; s=bf.readLine()){
			String[] ss = s.split("\t");
			Vertex node = new Vertex(check(Long.parseLong(ss[0])));
			for (int i=2; i<ss.length; i++){
				int to = check(Long.parseLong(ss[i]));
				if (to==node.getId()) continue;
				node.addNewEdge(to,0,0);
			}
			writer.append(new IntWritable(node.getId()), node);
			nRecords++;
			if (nRecords%1000000==0){ System.err.print("."); System.err.flush(); }
		}
		writer.append(new IntWritable(1), new Vertex(1)); // source
		writer.append(new IntWritable(2), new Vertex(2)); // sink
		writer.close(); bf.close(); System.err.println(); System.err.flush();
		long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
		System.err.printf("Time = %02d:%02d\n",elapsedTime/60,elapsedTime%60);
		return 0;
	}


    public int runRmiServer(String args[]) throws Exception {
		ApServer obj = new ApServer(FileSystem.get(getConf()));
		ApRemote stub = (ApRemote) UnicastRemoteObject.exportObject(obj, 0);
		Registry registry = LocateRegistry.getRegistry();
		try {
			registry.unbind("ApRemote");
			System.out.println("unbinded");
		} catch (NotBoundException nbe){}
		registry.bind("ApRemote", stub);
		System.out.println("ApServer is READY!");
		return 0;
	}


	public int run(String[] args) throws Exception {
		if (getConf().getBoolean("mf.input.facebook", false)) return runInput(args);
		if (getConf().getBoolean("mf.rmi.server", false)) return runRmiServer(args);

		System.out.printf("\tGraph   = %s\n",getConf().get("mf.graph"));
		System.out.printf("\tSources = %s\n",getConf().get("mf.sources"));
		System.out.printf("\tSinks   = %s\n",getConf().get("mf.sinks"));
		System.out.printf("\tComment = %s\n",getConf().get("mf.comment"));
		System.out.printf("\tSinkEx  = %s\n",getConf().get("mf.sink.excess"));
		System.out.printf("\tListMax = %s\n",getConf().get("mf.excess.list.max"));
		System.out.printf("\tAttBcst = %s\n",getConf().get("mf.broadcast.attenuation"));
		for (int round=getConf().getInt("mf.round",0), tacflow=0, totalTime=0; ; round++){
			JobConf conf = new JobConf(getConf(), FordFulkerson.class);
			conf.setJobName("ff2");
			int nRed = conf.getInt("mf.reducers",50);
			conf.setNumReduceTasks(nRed);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(Vertex.class);

			conf.setOutputFormat(SequenceFileOutputFormat.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Vertex.class);

			conf.setMapperClass(MapFF.class);
			conf.setCombinerClass(CombineFF.class);
			conf.setReducerClass(ReduceFF.class);
			conf.setInt("mf.round", round);
	
			String inputGraph = "graph-ff2-" + conf.get("mf.graph");
			Path curPath = new Path(inputGraph+(round==0?"":("-"+round)));
			Path nextPath = new Path(inputGraph+"-"+(round+1));
			FileInputFormat.setInputPaths(conf, curPath);
			FileOutputFormat.setOutputPath(conf, nextPath);
			System.err.printf("round=%d, graph=%s\n",round,inputGraph);

			if (round>1 && conf.getBoolean("mf.delete.prev.results",true)){
				FileSystem hdfs = FileSystem.get(getConf());
				String fileName = inputGraph + "-" + (round-1);
				if (hdfs.exists(new Path(fileName)) && hdfs.delete(new Path(fileName), true))
					System.err.printf("FS deleted : %s\n",fileName);
			}

			FileSystem hdfs = FileSystem.get(getConf());
			String fileName = inputGraph + "-" + (round+1);
			if (hdfs.exists(new Path(fileName)) && hdfs.delete(new Path(fileName), true))
				System.err.printf("FS deleted : %s\n",fileName);


			Registry registry = LocateRegistry.getRegistry(conf.get("mf.rmi.host",null));
			ApRemote AP = (ApRemote) registry.lookup("ApRemote");
			AP.prepare();

			long startTime = System.currentTimeMillis();
			RunningJob runj = JobClient.runJob(conf);
			long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;

			long[] f = AP.finish(conf.get("mf.graph")+"-"+conf.get("mf.comment"), round);

			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","AUGEDGES_SIZE",f[0]);
			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","ACCEPTED_FLOWS",f[1]);
			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","ACCEPTED_AUGPATHS",f[2]);
			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","RMI_SERVER_MAX_QS",f[3]);

			Counters ctrs = runj.getCounters();
			System.out.println(Arrays.toString(f));

			long N = ctrs.getCounter(mf.MFCounter.N);
			long soN = ctrs.getCounter(mf.MFCounter.SOURCE_EPATH_COUNT);
			long siN = ctrs.getCounter(mf.MFCounter.SINK_EPATH_COUNT);
			long som = ctrs.getCounter(mf.MFCounter.SOURCE_MOVE);
			long sim = ctrs.getCounter(mf.MFCounter.SINK_MOVE);
			long acflow = f[1];
			long le = ctrs.getCounter(mf.MFCounter.LOSE_EXCESS);
			
			tacflow += acflow;
			totalTime += elapsedTime;

			/*
			long size = 0;
			for (Counters.Group group : ctrs)
				for (Counters.Counter counter : group) 
					if ("Map output bytes".equals(counter.getDisplayName().trim()))
						size = counter.getCounter();
			red = Math.max(50, (int) (size/50000000));
			System.out.println("Previous Map output bytes: " + size + "; next red job = " + red);
			*/

			String o = String.format("i=%d; %d:%02d:%02d/%02d:%02d:%02d; Flows=%d/%d; LE=%d; So=%d/%d; Si=%d/%d\n",
				round, elapsedTime/60/60,(elapsedTime/60)%60,elapsedTime%60,
				totalTime/60/60,(totalTime/60)%60,totalTime%60,
				acflow,tacflow, le, som,soN, sim,siN
			);

			System.out.println(o);
			AP.echo(o);

			if (round > 0 && le==0 && acflow==0 && (som==0 || sim==0)) { //
				System.out.printf("Maximum-Flow complete\n");
				System.err.printf("Maximum-Flow complete\n");
				break;
			}
		}
		return 0;
	}
}
