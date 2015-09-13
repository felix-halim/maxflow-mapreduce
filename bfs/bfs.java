package bfs;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class bfs extends Configured implements Tool {
	public static class Map0FF extends Mapper<LongWritable,Text, IntWritable,Vertex> {
		IntWritable id = new IntWritable();
		Vertex u = new Vertex();
		Vertex v = new Vertex();

		@Override
		public void setup(Context context) {
			// Vertex 1 will be created many times (as the number of mappers)
			// but OK since the reducer will merge them
			try { id.set(1); u.init(1); context.write(id,u); }
			catch (Exception ex){ throw new RuntimeException(ex); }
		}

		private int check(long id){
			if (id >= 100000000000000L) id = 100000000000000L - id;
			if (id < Integer.MIN_VALUE) throw new RuntimeException("fail : " + id);
			if (id > Integer.MAX_VALUE) throw new RuntimeException("fail : " + id);
			if (id==0 || id==1 || id==2) throw new RuntimeException("Reserved ID : " + id);
			return (int) id;
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] ss = value.toString().split("\t");
			u.init(check(Long.parseLong(ss[0])));
			for (int i=2,to=0; i<ss.length; i++){
				try { to = check(Long.parseLong(ss[i])); } catch (Exception ex){ continue; }
				if (to==u.id) continue;
				u.addCon(to);
				v.init(to);
				v.addCon(u.id);
				id.set(v.id);
				context.write(id, v);
			}
			id.set(u.id);
			if (u.nE > 0) context.write(id, u);
			else context.getCounter("bfs","DISCARDED").increment(1);
		}
	}

	public static class Reduce0FF extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
		IntWritable id = new IntWritable();
		Vertex u = new Vertex();
		HashSet<Integer> S = new HashSet<Integer>();

		@Override
		public void setup(Context context) {
			String[] ss = context.getConfiguration().get("mf.sources", "null").split(",");
			S.clear(); for (String s : ss) S.add(Integer.parseInt(s));
		}

		@Override
		public void reduce(IntWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
			u.init(key.get());
			if (u.id == 1){
				u.distance = 0; 
				for (int i : S) u.addCon(i); 
			}
			List<Integer> cons = new ArrayList<Integer>();
			for (Vertex v : values){
				u.addAllCon(v);
				cons.add(v.nE);
				cons.add(v.con[0]);
				if (u.id != v.id) throw new RuntimeException("AAA");
			}
			if (u.nE == 0) throw new RuntimeException("nih " + u + " " + cons);
			id.set(u.id);
			context.write(id, u);
			context.getCounter("bfs","N").increment(1);
			context.getCounter("bfs","E").increment(u.nE);
		}
	}

	public static class MapBFS extends Mapper<IntWritable,Vertex, IntWritable,Vertex> {
		IntWritable id = new IntWritable();
		Vertex v = new Vertex();
		public void map(IntWritable id, Vertex u, Context context) throws IOException, InterruptedException {
			if (u.distance <= 0){
				u.distance *= -1;
				for (int i=0; i<u.nE; i++){
					v.init(u.con[i]);
					v.distance = -(u.distance + 1);
					id.set(v.id);
					context.write(id, v);
				}
			}
			id.set(u.id);
			context.write(id, u);
		}
	}

	public static class ReduceBFS extends Reducer<IntWritable,Vertex, IntWritable,Vertex> {
		Vertex u = new Vertex();
		public void reduce(IntWritable id, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
			u.init(id.get());
			for (Vertex v : values){
				if (v.id!=u.id) throw new RuntimeException("AX");
				if (u.distance == Vertex.MAX_DIST){
					u.distance = v.distance;
				} else if (u.distance < 0){
					if (v.distance == Vertex.MAX_DIST){
						// NOOP
					} else if (v.distance >= 0){
						if (v.distance > -u.distance) throw new RuntimeException("Noded = "+u.distance + " vd = " + v.distance);
						u.distance = v.distance;
					} else {
						u.distance = Math.max(u.distance, v.distance);
					}
				} else {
					if (v.distance == Vertex.MAX_DIST){
						// NOOP
					} else if (v.distance >= 0){
						throw new RuntimeException("HERE");
					} else {
						if (u.distance > -v.distance) throw new RuntimeException("Noded = "+u.distance + " vd = " + v.distance);
					}
				}
				u.addAllCon(v);
			}
			if (u.distance < Vertex.MAX_DIST)
				context.getCounter("bfs","VISITED").increment(1);
			context.write(id, u);
			context.getCounter("bfs","N").increment(1);
			context.getCounter("bfs","E").increment(u.nE);
		}
	}

	private static long val(Counters ctrs, String key){
		return ctrs.findCounter("bfs",key).getValue();
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		System.out.printf("\tGraph   = %s\n",conf.get("mf.graph"));
		System.out.printf("\tSources = %s\n",conf.get("mf.sources"));
		System.out.printf("\tSinks   = %s\n",conf.get("mf.sinks"));
		System.out.printf("\tComment = %s\n",conf.get("mf.comment"));
		for (int round=0, totalTime=0, pvis=-1; ; round++){
			FileSystem hdfs = FileSystem.get(getConf());


			Job job = new Job(conf);
			job.setJarByClass(bfs.class);
			job.setJobName("bfs");
			int nRed = conf.getInt("mf.reducers",50);
			job.setNumReduceTasks(nRed);

			String filePrefix = "bfs/" + conf.get("mf.graph");
			String jobPrefix = filePrefix + "/" + conf.get("mf.comment");
			String roundPrefix = jobPrefix + "/round-"+round;
			String prevRoundPrefix = jobPrefix + "/round-"+(round-1);
			String nextRoundPrefix = jobPrefix + "/round-"+(round+1);

			if (hdfs.delete(new Path(nextRoundPrefix), true))
				System.out.println("Deleted: " + nextRoundPrefix);

			hdfs.mkdirs(new Path(jobPrefix));

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
				curPath = new Path(roundPrefix);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setMapperClass(MapBFS.class);
				job.setReducerClass(ReduceBFS.class);
			}

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Vertex.class);

			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Vertex.class);

			FileInputFormat.setInputPaths(job, curPath);
			FileOutputFormat.setOutputPath(job, new Path(nextRoundPrefix));
			System.err.printf("round=%d\n",round);

			long startTime = System.currentTimeMillis();
				job.waitForCompletion(true);
			long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;

			Counters ctrs = job.getCounters();
			long N = val(ctrs,"N");
			long vis = val(ctrs,"VISITED");
			long E = val(ctrs,"TOTAL_EDGES");
			totalTime += elapsedTime;

			System.out.printf("i=%d; %d:%02d:%02d/%02d:%02d:%02d; %d/%d\n",
				round,
				elapsedTime/60/60,(elapsedTime/60)%60,elapsedTime%60,
				totalTime/60/60,(totalTime/60)%60,totalTime%60,
				vis,N
			);

			if (vis==N || pvis==vis){
				System.err.printf("BFS complete\n");
				break;
			}
			pvis = (int) vis;
		}
		return 0;
	}
}

class Vertex implements Writable {
	public static int MAX_DIST = Integer.MAX_VALUE/2;
	int[] con = new int[1000000];
	int id, distance, nE;

	public Vertex(){
		init(-1);
	}

	public void init(int id){
		this.id = id;
		this.distance = MAX_DIST;
		this.nE = 0;
	}

	public Vertex(int id){
		this.id = id;
		this.distance = MAX_DIST;
		this.nE = 0;
	}

	public void addCon(int vid){
		con[nE++] = vid;
	}

	public void addAllCon(Vertex v){
		for (int i=0; i<v.nE; i++)
			con[nE++] = v.con[i];
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(distance);
		out.writeInt(nE);
		for (int i=0; i<nE; i++)
			out.writeInt(con[i]);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		distance = in.readInt();
		nE = in.readInt();
		for (int i=0; i<nE; i++)
			con[i] = in.readInt();
	}

	public String toString(){
		return id + "\t" + distance + " :: (" + nE + ")";
	}
}

