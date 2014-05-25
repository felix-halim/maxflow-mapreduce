package ff3;

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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class FordFulkerson extends Configured implements Tool {
	public static class Map0FF extends Mapper<LongWritable,Text, IntWritable,Vertex> {
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			Vertex source = new Vertex(1);
			String[] ss = conf.get("mf.sources", "null").split(",");
			source.addS(new Excess());
			for (int i=0; i<ss.length; i++)
				source.addNewEdge(Integer.parseInt(ss[i]),0,0);

			Vertex sink = new Vertex(2);
			sink.addT(new Excess());
			ss = conf.get("mf.sinks", "null").split(",");
			for (int i=0; i<ss.length; i++)
				sink.addNewEdge(Integer.parseInt(ss[i]),0,0);

			try {
				context.write(new IntWritable(1), source);
				context.write(new IntWritable(2), sink);
			} catch (Exception ex){
				throw new RuntimeException(ex);
			}
		}

		private int check(long id){
			if (id >= 100000000000000L) id = 100000000000000L - id;
			if (id < Integer.MIN_VALUE) throw new RuntimeException("fail : " + id);
			if (id > Integer.MAX_VALUE) throw new RuntimeException("fail : " + id);
			if (id==0 || id==1 || id==2) throw new RuntimeException("Reserved ID : " + id);
			return (int) id;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			String[] ss = s.split("\t");
			Vertex node = new Vertex(check(Long.parseLong(ss[0])));
			for (int i=2; i<ss.length; i++){
				int to = check(Long.parseLong(ss[i]));
				if (to==node.getId()) continue;
				node.addNewEdge(to,0,0);

				Vertex v = new Vertex(to);
				v.addNewEdge(node.getId(), 0, 0);
				context.write(new IntWritable(v.getId()),v);
			}
			context.write(new IntWritable(node.getId()), node);
		}
	}

	public static class Reduce0FF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
		SequenceFile.Writer mos;
		String masterPrefix;
		FileSystem hdfs;
		int maxC;

		public void setup(Reducer.Context context) {
			Configuration conf = context.getConfiguration();

			String filePrefix = "ff3/" + conf.get("mf.graph");
			String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
			masterPrefix = jobPrefix + "/round-1/master/";
			maxC = conf.getInt("mf.max.random.capacity", 1);
			
			try {
				hdfs = FileSystem.get(conf);
			} catch (Exception ex){
				throw new RuntimeException(ex);
			}
		}

		public void cleanup(Reducer.Context context) {
			try {
				mos.close(); mos = null;
			} catch (Exception ex){
				throw new RuntimeException(ex);
			}
		}

		public void reduce(IntWritable u, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
			long t1 = System.currentTimeMillis();
	
			if (mos==null){
				Path masterPath = new Path(masterPrefix + u.get());
				if (hdfs.delete(masterPath, true))
					throw new RuntimeException("Master exists : " + masterPrefix + u.get());
				mos = SequenceFile.createWriter(hdfs, context.getConfiguration(),
					masterPath, IntWritable.class, Vertex.class);

				long t2 = System.currentTimeMillis();
				context.getCounter("FF3","TIME_OPEN_HDFS").increment(t2-t1);
			}

			Augmenter As = new Augmenter();
			Augmenter At = new Augmenter();
			Vertex node = new Vertex(u.get());
			for (Vertex v : values) node.merge(v,As,At);
			if (node.getEdges().size()==0) return;

			int U = node.getId();
			for (Edge e : node.getEdges()){
				int V = e.getTo();
				e.setCapacity((U==1||U==2||V==1||V==2)? 10000000 : ((int)(Math.random()*maxC)+1));
			}
			mos.append(u, node); // master vertex

			context.getCounter("FF3","N").increment(1);
			context.getCounter("FF3","E").increment(node.getEdges().size());

			long t2 = System.currentTimeMillis();
			context.getCounter("FF3","TIME_REDUCER").increment(t2-t1);
		}
	}

	public static class UnsplitableInput<K,V> extends SequenceFileInputFormat<K,V> {
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		System.out.printf("\tGraph   = %s\n",conf.get("mf.graph"));
		System.out.printf("\tSources = %s\n",conf.get("mf.sources"));
		System.out.printf("\tSinks   = %s\n",conf.get("mf.sinks"));
		System.out.printf("\tComment = %s\n",conf.get("mf.comment"));
		System.out.printf("\tSinkEx  = %s\n",conf.get("mf.sink.excess"));
		System.out.printf("\tListMax = %s\n",conf.get("mf.excess.list.max"));
		for (int round=conf.getInt("mf.round",0), tacflow=0, totalTime=0; ; round++){
			FileSystem hdfs = FileSystem.get(conf);
			conf.setInt("mf.round", round);

			Job job = new Job(conf,"ff3");
			job.setJarByClass(FordFulkerson.class);
			int nRed = conf.getInt("mf.reducers",50);
			job.setNumReduceTasks(nRed);

			String filePrefix = "ff3/" + conf.get("mf.graph");
			String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
			String roundPrefix = jobPrefix + "/round-"+round;
			String prevRoundPrefix = jobPrefix + "/round-"+(round-1);
			String nextRoundPrefix = jobPrefix + "/round-"+(round+1);
			String roundMasterPath = roundPrefix + "/master";
			String nextRoundMasterPath = nextRoundPrefix + "/master";
			String flowsPrefix = nextRoundPrefix + "/flows";

			if (hdfs.delete(new Path(nextRoundPrefix), true))
				System.out.println("Deleted: " + nextRoundPrefix);

			hdfs.mkdirs(new Path(nextRoundMasterPath));
			
			if (round>1 && conf.getBoolean("mf.delete.prev.results",true))
				if (hdfs.delete(new Path(prevRoundPrefix), true))
					System.out.println("Deleted: " + prevRoundPrefix);

			Path curPath=null;
			if (round==0){
				curPath = new Path(filePrefix + "/yzcrawler");
				job.setInputFormatClass(TextInputFormat.class);
				job.setMapperClass(Map0FF.class);
				job.setReducerClass(Reduce0FF.class);
			} else {
				curPath = new Path(roundMasterPath);
				job.setInputFormatClass(UnsplitableInput.class);
				job.setMapperClass(MapFF.class);
				job.setReducerClass(ReduceFF.class);
			}
			job.setCombinerClass(CombineFF.class);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Vertex.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Vertex.class);

			Path nextPath = new Path(jobPrefix+"/dummyout");
			hdfs.delete(nextPath, true);
			FileInputFormat.setInputPaths(job, curPath);
			FileOutputFormat.setOutputPath(job, nextPath);


			Registry registry = LocateRegistry.getRegistry(conf.get("mf.rmi.host",null));
			ApRemote rmi = (ApRemote) registry.lookup("ApRemote3");
			System.err.printf("\nround=%d, nRed=%d\n",round,nRed);
			long startTime = System.currentTimeMillis();

				rmi.clear();
				job.waitForCompletion(true);
				long[] f = rmi.finish(flowsPrefix);

			long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;

			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","AUGEDGES_SIZE",f[0]);
			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","ACCEPTED_FLOWS",f[1]);
			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","ACCEPTED_AUGPATHS",f[2]);
			System.out.printf("10/06/06 05:28:12 INFO mapred.JobClient:     %s=%d\n","RMI_SERVER_MAX_QS",f[3]);

			Counters ctrs = job.getCounters();
			long N = ctrs.findCounter("FF3","N").getValue();
			long soN = ctrs.findCounter("FF3","SOURCE_EPATH_COUNT").getValue();
			long siN = ctrs.findCounter("FF3","SINK_EPATH_COUNT").getValue();
			long som = ctrs.findCounter("FF3","SOURCE_MOVE").getValue();
			long sim = ctrs.findCounter("FF3","SINK_MOVE").getValue();
			long le = ctrs.findCounter("FF3","LOSE_EXCESS").getValue();
			long acflow = f[1]; tacflow += acflow;
			totalTime += elapsedTime;

			String o = String.format("i=%d; %d:%02d:%02d/%02d:%02d:%02d; Flows=%d/%d(%d/%d); So=%d/%d; Si=%d/%d",
				round, elapsedTime/60/60,(elapsedTime/60)%60,elapsedTime%60,
				totalTime/60/60,(totalTime/60)%60,totalTime%60,
				acflow,tacflow,f[0],f[3], som,soN, sim,siN
			);

			System.out.println(o);
			rmi.echo(o);

			if (round > 0 && le==0 && acflow==0 && (som==0 || sim==0)) { //
				System.out.printf("Maximum-Flow complete\n");
				System.err.printf("Maximum-Flow complete\n");
				break;
			}
		}
		return 0;
	}
}
