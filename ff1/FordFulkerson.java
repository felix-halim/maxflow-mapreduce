package ff1;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

public class FordFulkerson extends Configured implements Tool {
	public static class Base extends MapReduceBase {
		Map<Long,Edge> augEdges = new HashMap<Long,Edge>(); // the augmented edges from the previous round.
		FileSystem hdfs; // a reference to the HDFS to globally store the augmented paths
		boolean useSinkExcess;
		int[] sources, sinks;
		int excessListMaxSize; // how many simple paths stored for the excess paths
		int maxRandomCapacity;
		int round;
		String graph;

		public void configure(JobConf job) {
			super.configure(job);

			graph = job.get("mf.graph")+"-"+job.get("mf.comment");
			round = job.getInt("mf.round", 0);
			maxRandomCapacity = job.getInt("mf.max.random.capacity", 1);
			useSinkExcess = job.getBoolean("mf.sink.excess", true);
			excessListMaxSize = job.getInt("mf.excess.list.max", 512);

			String[] ss = job.get("mf.sources", "null").split(",");
			sources = new int[ss.length];
			for (int i=0; i<ss.length; i++) sources[i] = Integer.parseInt(ss[i]);
			ss = job.get("mf.sinks", "null").split(",");
			sinks = new int[ss.length];
			for (int i=0; i<ss.length; i++) sinks[i] = Integer.parseInt(ss[i]);

			try { hdfs = FileSystem.get(job); } catch (IOException ex){ throw new RuntimeException(ex); }
		}
	}

	public static class PSMapper extends Base implements org.apache.hadoop.mapred.Mapper<IntWritable,Vertex, IntWritable,Vertex> {
		public void configure(JobConf job) {
			super.configure(job);
			try { // Initialize the augPath with the previous round's flow if any.
				Path path = new Path("flows-ff1-"+graph+"-"+round);
				if (!hdfs.exists(path)) return;
				augEdges.clear();
				BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
				for (String line=br.readLine(); line!=null; line=br.readLine()){
					if (line.trim().length()==0) continue;
					String[] s = line.split(" ");
					int a = Integer.parseInt(s[0]);
					int b = Integer.parseInt(s[1]);
					Edge edge = new Edge(a,b,Integer.parseInt(s[2]),0,0);
					augEdges.put(edge.getId(), edge);
				}
				br.close();
			} catch (Exception ex){
				throw new RuntimeException(ex);
			}
		}

		/**
		 * The map function will update the node's edge residue, excesses, fragments,
		 * push the excesses of the node to the neighboring vertices and vertices reachable
		 * from the outE, creates a single forward fragment from
		 * the randomly selected edge with positive residue, notify the node's
		 * forward and backward fragments to the other endpoints of the fragments.
		 */
		public void map(IntWritable key, Vertex node, OutputCollector<IntWritable, Vertex> output, Reporter reporter) throws IOException {
			long t1 = System.currentTimeMillis();
				node.setorhr(output,reporter,hdfs,new Random(key.get()*round));

				if (round==0){
					for (Edge edge : node.getEdges()){
						Vertex vn = new Vertex(edge.getOtherPair(node.getId()));
						if (vn.getId()==node.getId()) throw new RuntimeException("Self Loop");
						vn.addEdge(new Edge(edge));
						output.collect(new IntWritable(vn.getId()),vn);
					}
					output.collect(new IntWritable(node.getId()), node);
				} else {
						node.updateE(augEdges,useSinkExcess);

					long t2 = System.currentTimeMillis();
					reporter.incrCounter(mf.MFCounter.TIME_UPDATE_EDGES, t2-t1);

						node.checkSSMeet();

					long t3 = System.currentTimeMillis();
					reporter.incrCounter(mf.MFCounter.TIME_MEET_IN_THE_MIDDLE, t3-t2);

						node.pushSSE();
						output.collect(new IntWritable(node.getId()), node);
				}
			long t2 = System.currentTimeMillis();
			reporter.incrCounter(mf.MFCounter.TIME_MAPPER, t2-t1);
		}
	}

	/**
	 * The sole purpose of the combine/reducer is collapsing all the values into one except if the node is the sink node.
	 * If the specified node u is the sink node, then all excesses in this node will be considered
	 * as augmented paths. The augmented paths will be "accepted" one by one according to excess priority.
	 * Once an augmented path is accepted, the edges flow (and capacity) used by the path will be noted.
	 * The next augmented path will be accepted if no edge in the path if the flow is added to
	 * the current note will exceed the capacity of the edge.
	 * Once the list of augmented paths is produced, it will be persisted in HDFS globally
	 * so that in the next round it will be used to drop "invalid" fragments and excesses.
	 * The values is sorted based on the excess amount (desc), path length (asc).
	 */
	public static class PSCombiner extends Base implements org.apache.hadoop.mapred.Reducer<IntWritable, Vertex, IntWritable, Vertex> {
		public void reduce(IntWritable u, Iterator<Vertex> values, OutputCollector<IntWritable, Vertex> output, Reporter reporter) throws IOException {
			if (u.get()==Vertex.SINK){
				while (values.hasNext()){
					Vertex v = values.next();
					output.collect(new IntWritable(v.getId()),v);
				}
				return;
			}

			long t1 = System.currentTimeMillis();

				Vertex node = new Vertex(u.get());
				node.setorhr(output,reporter,hdfs,new Random(u.get()*round));
				node.mergeNodes(values,graph,round,excessListMaxSize,false);
				output.collect(new IntWritable(node.getId()), node);

			long t2 = System.currentTimeMillis();
			reporter.incrCounter(mf.MFCounter.TIME_COMBINER, t2-t1);
		}
	}

	public static class PSReducer extends Base implements org.apache.hadoop.mapred.Reducer<IntWritable, Vertex, IntWritable, Vertex> {
		public void reduce(IntWritable u, Iterator<Vertex> values, OutputCollector<IntWritable, Vertex> output, Reporter reporter) throws IOException {
			long t1 = System.currentTimeMillis();
				if (round == 0){
					Vertex node = new Vertex(u.get());
					node.mergeNodes0(values, sources, sinks, maxRandomCapacity,useSinkExcess);
					output.collect(new IntWritable(node.getId()), node);
					reporter.incrCounter(mf.MFCounter.N, 1);
					reporter.incrCounter(mf.MFCounter.E, node.getEdges().size());
				} else {
					Vertex node = new Vertex(u.get());
					node.setorhr(output,reporter,hdfs,new Random(u.get()*round));
					node.mergeNodes(values,graph,round,excessListMaxSize,true);
					output.collect(new IntWritable(node.getId()), node);
				}
			long t2 = System.currentTimeMillis();
			reporter.incrCounter(mf.MFCounter.TIME_REDUCER, t2-t1);
		}
	}

	public int run(String[] args) throws Exception {
		System.out.printf("\tGraph   = %s\n",getConf().get("mf.graph"));
		System.out.printf("\tSources = %s\n",getConf().get("mf.sources"));
		System.out.printf("\tSinks   = %s\n",getConf().get("mf.sinks"));
		System.out.printf("\tComment = %s\n",getConf().get("mf.comment"));
		System.out.printf("\tDebug   = %s\n",getConf().get("mf.debug"));
		System.out.printf("\tSinkEx  = %s\n",getConf().get("mf.sink.excess"));
		System.out.printf("\tListMax = %s\n",getConf().get("mf.excess.list.max"));
		for (int round=getConf().getInt("mf.round",0), tacflow=0, totalTime=0; ; round++){
			JobConf conf = new JobConf(getConf(), FordFulkerson.class);
			conf.setJobName("ff1");
			int nRed = conf.getInt("mf.reducers",50);
			conf.setNumReduceTasks(nRed);

			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(Vertex.class);

			conf.setOutputFormat(SequenceFileOutputFormat.class);
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(Vertex.class);

			conf.setMapperClass(PSMapper.class);
			if (round > 0){
				conf.setCombinerClass(PSCombiner.class);
				//conf.setInt("mapred.reduce.tasks", red);
			}
			conf.setReducerClass(PSReducer.class);
			conf.setInt("mf.round", round);
	
			String inputGraph = "graph-ff1-" + conf.get("mf.graph");
			Path curPath = new Path(inputGraph+(round==0?"":("-"+round)));
			Path nextPath = new Path(inputGraph+"-"+(round+1));
			FileInputFormat.setInputPaths(conf, curPath);
			FileOutputFormat.setOutputPath(conf, nextPath);
			System.err.printf("round=%d, graph=%s\n",round,inputGraph);

			if (round>1){
				FileSystem hdfs = FileSystem.get(getConf());
				String fileName = inputGraph + "-" + (round-1);
				if (hdfs.exists(new Path(fileName)) && hdfs.delete(new Path(fileName), true))
					System.err.printf("FS deleted : %s\n",fileName);
			}

			FileSystem hdfs = FileSystem.get(getConf());
			String fileName = inputGraph + "-" + (round+1);
			if (hdfs.exists(new Path(fileName)) && hdfs.delete(new Path(fileName), true))
				System.err.printf("FS deleted : %s\n",fileName);

			long startTime = System.currentTimeMillis();
			RunningJob runj = JobClient.runJob(conf);
			long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;

			Counters ctrs = runj.getCounters();
			long N = ctrs.getCounter(mf.MFCounter.N);
			long soN = ctrs.getCounter(mf.MFCounter.SOURCE_EPATH_COUNT);
			long siN = ctrs.getCounter(mf.MFCounter.SINK_EPATH_COUNT);
			long som = ctrs.getCounter(mf.MFCounter.SOURCE_MOVE);
			long sim = ctrs.getCounter(mf.MFCounter.SINK_MOVE);
			long acflow = ctrs.getCounter(mf.MFCounter.ACCEPTED_FLOWS);
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

			System.out.printf("i=%d; %d:%02d:%02d/%02d:%02d:%02d; Flows=%d/%d\tSSM=%d\tSSF=%d/%d\tSSMv=%d/%d\n",
				round,
				elapsedTime/60/60,(elapsedTime/60)%60,elapsedTime%60,
				totalTime/60/60,(totalTime/60)%60,totalTime%60,
				acflow,tacflow, ctrs.getCounter(mf.MFCounter.MEET_IN_THE_MIDDLE),
				soN, siN, som,sim
			);

			boolean useSinkE = getConf().get("mf.sink.excess").equals("true");

			boolean useSinkF = conf.getBoolean("mf.use.sink.fragment", true);
			if (round > 0 && acflow==0 && (som==0 || (useSinkE && sim==0))){
				System.out.printf("Maximum-Flow complete\n");
				System.err.printf("Maximum-Flow complete\n");
				break;
			}
		}
		return 0;
	}
}
