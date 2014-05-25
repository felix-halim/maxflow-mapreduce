package ff4;

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
		IntWritable vid = new IntWritable();
		Vertex vertex = new Vertex();
		Vertex vertex2 = new Vertex();

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			vertex.init(1);
			for (String s : conf.get("mf.sources", "null").split(",")) 
				vertex.addNewEdge(Integer.parseInt(s),0,0);
			vertex.sortEdges();
			vertex.nS = 1; vertex.S[0].clear();
			try { vid.set(1); context.write(vid, vertex); }
			catch (Exception ex){ throw new RuntimeException(ex); }

			vertex.init(2);
			for (String s : conf.get("mf.sinks", "null").split(","))
				vertex.addNewEdge(Integer.parseInt(s),0,0);
			vertex.sortEdges();
			vertex.nT = 1; vertex.T[0].clear();
			try { vid.set(2); context.write(vid, vertex); }
			catch (Exception ex){ throw new RuntimeException(ex); }
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
			vertex.init(check(Long.parseLong(ss[0])));
			if (vertex.id==0 || vertex.id==1 || vertex.id==2)
				throw new RuntimeException("Reserved ID : " + vertex.id);
			for (int i=2; i<ss.length; i++){
				int to = check(Long.parseLong(ss[i]));
				if (to == vertex.id) continue;
				if (to==0 || to==1 || to==2) throw new RuntimeException("Reserved ID " + to);
				vertex.addNewEdge(to,0,0);

				vertex2.init(to);
				vertex2.addNewEdge(vertex.id, 0, 0);
				vid.set(to);
				context.write(vid, vertex2);
			}
			vertex.sortEdges();
			vid.set(vertex.id);
			context.write(vid, vertex);
		}
	}

	public static class Reduce0FF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
		Vertex vertex = new Vertex();
		SequenceFile.Writer mos;
		String masterPrefix;
		FileSystem hdfs;
		int maxC;

		public void setup(Reducer.Context context) {
			Configuration conf = context.getConfiguration();

			String filePrefix = "ff4/" + conf.get("mf.graph");
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
				hdfs.delete(masterPath, true);
				mos = SequenceFile.createWriter(hdfs, context.getConfiguration(),
					masterPath, IntWritable.class, Vertex.class);

				long t2 = System.currentTimeMillis();
				context.getCounter("ff4","TIME_OPEN_HDFS").increment(t2-t1);
			}

			Augmenter As = new Augmenter();
			Augmenter At = new Augmenter();
			vertex.init(u.get());
			for (Vertex v : values) vertex.merge(v,As,At);
			if (vertex.nE == 0) return;

			int U = u.get();
			for (int i=0; i<vertex.nE; i++){
				Edge e = vertex.E[i];
				e.C = (U==1||U==2||e.V==1||e.V==2)? 10000000 : ((int)(Math.random()*maxC)+1);
			}
			mos.append(u, vertex); // master vertex

			context.getCounter("ff4","N").increment(1);
			context.getCounter("ff4","E").increment(vertex.nE);

			long t2 = System.currentTimeMillis();
			context.getCounter("ff4","TIME_REDUCER").increment(t2-t1);
		}
	}

	public static class UnsplitableInput<K,V> extends SequenceFileInputFormat<K,V> {
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}
	}

	public int run(String[] args) throws Exception {
		/*
		Vertex v = new Vertex();
		v.T[0].clear();
		v.init(2);
		v.nT = 1;
		v.T[0].addLeft(3429443,2,0,1000000);
		System.out.println(v.toString(true));
		if (true) return 0;
		*/

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

			Job job = new Job(conf,"ff4");
			job.setJarByClass(FordFulkerson.class);
			int nRed = conf.getInt("mf.reducers",50);
			job.setNumReduceTasks(nRed);

			String filePrefix = "ff4/" + conf.get("mf.graph");
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
//			job.setCombinerClass(CombineFF.class);

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
			ApRemote rmi = (ApRemote) registry.lookup("ApRemote4");
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
			long N = ctrs.findCounter("ff4","N").getValue();
			long soN = ctrs.findCounter("ff4","SOURCE_EPATH_COUNT").getValue();
			long siN = ctrs.findCounter("ff4","SINK_EPATH_COUNT").getValue();
			long som = ctrs.findCounter("ff4","SOURCE_MOVE").getValue();
			long sim = ctrs.findCounter("ff4","SINK_MOVE").getValue();
			long le = ctrs.findCounter("ff4","LOSE_EXCESS").getValue();
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
