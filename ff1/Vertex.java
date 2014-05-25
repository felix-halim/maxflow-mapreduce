package ff1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

public class Vertex extends Configured implements Tool, Writable {
	public static int SOURCE = 1, SINK = 2;
	private int id;
	//private boolean broadcast;
	private Map<Long,Edge> edges = new HashMap<Long,Edge>();
	private TreeSet<Excess> sourceEs = new TreeSet<Excess>();
	private TreeSet<Excess> sinkEs = new TreeSet<Excess>();

	private OutputCollector<IntWritable, Vertex> output;
	private Reporter reporter;
	private FileSystem hdfs;			// a reference to the HDFS to globally store the augmented paths
	private Random rand;

	public Vertex(){}
	public Vertex(int id){ this.id = id; }

	public int getId(){
		return id;
	}

	public void setorhr(OutputCollector<IntWritable, Vertex> output, Reporter reporter, FileSystem hdfs, Random rand){
		this.output = output;
		this.reporter = reporter;
		this.hdfs = hdfs;
		this.rand = rand;
	}

	public void addEdge(Edge edge){
		if (edge.getA()!=id && edge.getB()!=id) throw new RuntimeException("Not Linked");
		if (edges.put(edge.getId(), edge)!=null) throw new RuntimeException("Edge Exists");
	}

	public Collection<Edge> getEdges(){
		return edges.values();
	}

	public void mergeNodes0(Iterator<Vertex> values, int[] sources, int[] sinks, int maxRandomCapacity, boolean useSinkExcess) throws IOException {
		while (values.hasNext()){
			for (Edge te : values.next().edges.values()){
				Edge edge = edges.get(te.getId());
				if (edge == null){
					edges.put(te.getId(), te);
				} else {
					edge.merge(te);
				}
			}
		}

		for (Edge edge : getEdges()){
			Random rand = new Random(edge.getId());
			int ca = rand.nextInt(maxRandomCapacity) + 1;
			int cb = rand.nextInt(maxRandomCapacity) + 1;
			edge.setInternalFlow(id == edge.getA()? cb : ca);
			edge.setInternalCapacity(ca + cb);
			if (edge.getInternalFlow()!=1 || edge.getInternalCapacity()!=2)
				throw new RuntimeException("" + edge);
		}

		if (id == 1){
			for (int i : sources) addEdge(new Edge(1,i,0,0,Excess.MAX_FLOW));
			sourceEs.add(new Excess(id));
			//broadcast = true;
		} else if (id == 2){
			for (int i : sinks) addEdge(new Edge(2,i,0,Excess.MAX_FLOW,Excess.MAX_FLOW));
			if (useSinkExcess) sinkEs.add(new Excess(id));
//			broadcast = true;
		}

		for (int i : sources) if (id==i)
			addEdge(new Edge(i,1,0,Excess.MAX_FLOW,Excess.MAX_FLOW));
		for (int i : sinks) if (id==i)
			addEdge(new Edge(i,2,0,0,Excess.MAX_FLOW));
	}

	/**
	 * This method is used by the Combiner and Reducer class to merge the values of Vertex objects.
	 * During the merge, only the best excess is kept unless this node is a sink node then all excesses
	 * will be kept (it will be processed after it's merged), the fragments are always
	 * filtered to have only exponential length of fragments, and two counters are maintained:
	 * ActiveRecords and nMerges. The excess that is kept is the one that has the greatest amount,
	 * if tie the one with shorter length. The number of active records may be a little misleading
	 * because of double count on the combiner and reducer.
	 */
	public void mergeNodes(Iterator<Vertex> values, String graph, int iteration, int excessListMaxSize, boolean inReduce) throws IOException {
		long t1 = System.currentTimeMillis();
		AugmentingPaths ap = new AugmentingPaths();
		AugmentingPaths soap = new AugmentingPaths();
		AugmentingPaths siap = new AugmentingPaths();
		int nMerges = 0, sourceEsize = -1, sinkEsize = -1, acflow = 0;
		List<Excess> acceptedEs = new ArrayList<Excess>();
		while (values.hasNext()){
			Vertex v = values.next();
			//broadcast |= v.broadcast;
			if (v.edges.size() > 0){
				sourceEsize = v.sourceEs.size();
				sinkEsize = v.sinkEs.size();
			}
			for (Excess e : v.sourceEs){
				if (id==SINK){
					if (e.getFrom()!=SOURCE || e.getTo()!=SINK) throw new RuntimeException(""+e);
					reporter.incrCounter(mf.MFCounter.AUGPATH_CANDIDATES, 1);
					int f = ap.accept(e);
					if (f > 0){
						acceptedEs.add(e);
						acflow += f;
						reporter.incrCounter(mf.MFCounter.ACCEPTED_AUGPATHS, 1);
					}
				} else if (sourceEs.size() < excessListMaxSize){
					if (soap.accept(e) > 0) sourceEs.add(e);
				} else if (sourceEs.last().compareTo(e) > 0){
					sourceEs.pollLast();
					sourceEs.add(e); // upgrade to the better excess
				}
			}
			for (Excess e : v.sinkEs){
				if (sinkEs.size() < excessListMaxSize){
					if (siap.accept(e) > 0) sinkEs.add(e);
				} else if (sinkEs.last().compareTo(e) > 0){
					sinkEs.pollLast();
					sinkEs.add(e); // upgrade
				}
			}
			for (Edge te : v.edges.values()){
				Edge edge = edges.get(te.getId());
				if (edge == null){
					edges.put(te.getId(), te);
				} else {
					edge.merge(te);
				}
			}
			nMerges++;
		}
		
		long t2 = System.currentTimeMillis();
		reporter.incrCounter(mf.MFCounter.TIME_MERGE_NODES, t2-t1);

		if (id==SINK){
			if (!inReduce) throw new RuntimeException("Cannot augment in combine");
			Path path = new Path("flows-ff1-"+graph+"-"+(iteration+1));
			if (hdfs.exists(path)) hdfs.delete(path);
			if (acflow > 0){
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(path)));
				int fcheck = 0;
				for (Edge edge : ap.getEdges()){
					int a = edge.getReference(), b = edge.getOtherPair(a), f = edge.getPotential(a);
					if (f == 0) continue;
					if (a == SOURCE) fcheck += f;
					String s = a + " " + b + " " + f + "\n";
					bw.write(s,0,s.length());
				}
				if (fcheck <= 0) throw new RuntimeException("X");
				if (fcheck != acflow) throw new RuntimeException(acflow + " " + fcheck + "\n" + ap.getEdges());
				bw.close();
				reporter.incrCounter(mf.MFCounter.ACCEPTED_FLOWS, acflow);
			}

			FileWriter fw = new FileWriter("/tmp/total-flow-ff1-"+graph, true);
			fw.write(acflow+"\n");
			fw.close();

			path = new Path("flowpaths-ff1-"+graph+"-"+(iteration+1));
			if (hdfs.exists(path)) hdfs.delete(path);
			if (acflow > 0){
//				if (acflow != acceptedEs.size()) throw new RuntimeException("WRONG");
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(path)));
				for (Excess excess : acceptedEs){
					int prev = excess.getFrom();
					String s = prev+"";
					for (Edge edge : excess.getEdges()){
						edge.mustHas(prev);
						prev = edge.getOtherPair(prev);
						s += ":"+ prev;
					}
					s += "="+excess.getPotential()+"\n";
					bw.write(s,0,s.length());
				}
				bw.close();
			}
		}
		if (inReduce){
			reporter.incrCounter(mf.MFCounter.N, 1);
			reporter.incrCounter(mf.MFCounter.E, edges.size());
			reporter.incrCounter(mf.MFCounter.SOURCE_EPATH_COUNT, sourceEs.size()>0?1:0);
			reporter.incrCounter(mf.MFCounter.SINK_EPATH_COUNT, sinkEs.size()>0?1:0);
			reporter.incrCounter(mf.MFCounter.SOURCE_EPATH_TOTAL, sourceEs.size());
			reporter.incrCounter(mf.MFCounter.SINK_EPATH_TOTAL, sinkEs.size());
		}
		boolean gotSourceE = edges.size()>0 && sourceEsize==0 && sourceEs.size()>0;
		boolean gotSinkE = edges.size()>0 && sinkEsize==0 && sinkEs.size()>0;
//		if (gotSourceE || gotSinkE) broadcast = true;
		reporter.incrCounter(mf.MFCounter.SOURCE_MOVE, gotSourceE?1:0);
		reporter.incrCounter(mf.MFCounter.SINK_MOVE, gotSinkE?1:0);

		long t3 = System.currentTimeMillis();
		reporter.incrCounter(mf.MFCounter.TIME_AUGMENTING, t3-t2);
	}

	/**
	 * Remove all Excess in the specified list of excesses that have zero amount
	 * after they are updated with the augmented paths from the previous iteration.
	 * @param excesses the list of excesses to be updated.
	 */
	public void updateE(Map<Long,Edge> augPaths, boolean useSinkExcess) throws IOException {
		if (id==SOURCE && sourceEs.size()!=1) throw new RuntimeException(""+sourceEs);
		if (useSinkExcess && id==SINK && sinkEs.size()!=1) throw new RuntimeException(""+sinkEs);
		boolean soEb = sourceEs.size() > 0;
		boolean siEb = sinkEs.size() > 0;
		if (sourceEs.size()>0){
			List<Excess> nexs = new ArrayList<Excess>();
			for (Excess srcE : sourceEs){
				srcE.augment(augPaths);
				if (srcE.getPotential()>0) nexs.add(srcE);
			}
			sourceEs.clear();
			sourceEs.addAll(nexs);
		}
		if (sinkEs.size()>0 && id!=SINK){
			List<Excess> nexs = new ArrayList<Excess>();
			for (Excess siE : sinkEs){
				siE.augment(augPaths);
				if (siE.getPotential()>0) nexs.add(siE);
			}
			sinkEs.clear();
			sinkEs.addAll(nexs);
		}

		boolean nb = (soEb && sourceEs.size()==0) || (siEb && sinkEs.size()==0);
		for (Edge edge : edges.values()){
			int conId = edge.getOtherPair(id);
			if (nb){
				Vertex v = new Vertex(conId);
//				v.broadcast = true;
				output.collect(new IntWritable(conId),v);
			}
			Edge aedge = augPaths.get(edge.getId());
			if (aedge == null) continue;
			if (edge.getPotential(id)!=0) throw new RuntimeException("Cannot have potential");
			edge.setFlow(id, edge.getFlow(id) + aedge.getPotential(id));
		}
	}


	/**
	 * For all excesses stored in the specified node, push them all to the neighboring vertices
	 * which connecting edge has positive residue and to all available outE the node has.
	 * An excess will be pushed if it doesn't contain cycle when it's pushed.
	 * @param node The specified node to push its excesses.
	 */
	public void pushSSE() throws IOException {
//		if (!broadcast) return; else broadcast = false;

		long t1 = System.currentTimeMillis();
		if (sourceEs.size()>0){
			List<Excess> arr = new ArrayList<Excess>(sourceEs);
			for (Edge edge : edges.values()){
				if (edge.getResidue(id)==0) continue;
				int to = edge.getOtherPair(id);
				int i = rand.nextInt(arr.size());
				Excess srcE = new Excess(arr.get(i));
				Vertex v = new Vertex(to);
				v.sourceEs.add(srcE);
				if (srcE.addEdge(edge, false))
					output.collect(new IntWritable(v.getId()), v);
			}
		}
		
		long t2 = System.currentTimeMillis();
		reporter.incrCounter(mf.MFCounter.TIME_EXTEND_SOURCE_E, t2-t1);

		if (sinkEs.size()>0){
			List<Excess> arr = new ArrayList<Excess>(sinkEs);
			for (Edge edge : edges.values()){
				int vid = edge.getOtherPair(id);
				if (edge.getResidue(vid)==0) continue;
				int i = rand.nextInt(arr.size());
				Excess siE = new Excess(arr.get(i));
				Vertex v = new Vertex(vid);
				v.sinkEs.add(siE);
				if (siE.addEdge(edge, true))
					output.collect(new IntWritable(v.getId()), v);
			}
		}

		long t3 = System.currentTimeMillis();
		reporter.incrCounter(mf.MFCounter.TIME_EXTEND_SINK_E, t3-t2);
	}

	public boolean checkSSMeet() throws IOException {
		AugmentingPaths ap = new AugmentingPaths();
		List<Excess> cands = new ArrayList<Excess>(sinkEs);
		for (Excess sourceE : sourceEs){
			Collections.shuffle(cands, rand);
			for (Excess sinkE : cands){
				Excess srcE = new Excess(sourceE);
				if (!srcE.concat(sinkE)) continue;
				if (srcE.getFrom()!=SOURCE || srcE.getTo()!=SINK)
					throw new RuntimeException("Meet Failed\n"+this);
				if (ap.accept(srcE) > 0){
					Vertex v = new Vertex(SINK);
					v.sourceEs.add(srcE);
					output.collect(new IntWritable(v.getId()),v);
					reporter.incrCounter(mf.MFCounter.MEET_IN_THE_MIDDLE, 1);
					break;
				}
			}
		}
		return true;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
//		out.writeBoolean(broadcast);
		out.writeInt(sourceEs.size()); for (Excess sourceE : sourceEs) sourceE.write(out);
		out.writeInt(sinkEs.size()); for (Excess sinkE : sinkEs) sinkE.write(out);
		out.writeInt(edges.size()); for (Edge edge : edges.values()) edge.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
//		broadcast = in.readBoolean();
		sourceEs.clear();
		for (int i=in.readInt(); i>0; i--){
			Excess sourceE = new Excess();
			sourceE.readFields(in);
			sourceEs.add(sourceE);
		}
		sinkEs.clear();
		for (int i=in.readInt(); i>0; i--){
			Excess sinkE = new Excess();
			sinkE.readFields(in);
			sinkEs.add(sinkE);
		}
		edges.clear();
		for (int i=in.readInt(); i>0; i--){
			Edge edge = new Edge();
			edge.readFields(in);
			edges.put(edge.getId(), edge);
		}
	}

	public String toString(){
		String ret = id + " : \n";
		for (Excess sourceE : sourceEs) ret += "\tsE : " + sourceE + "\n";
		for (Excess sinkE : sinkEs) ret += "\ttE : " + sinkE + "\n";
		for (Edge edge : edges.values()){
			int b = edge.getOtherPair(id);
			ret += String.format("\tto=%d, f=%d, c=%d\n",b,edge.getFlow(id),edge.getInternalCapacity());
		}
		return ret;
	}

	public int run(String[] args) throws Exception {
		for (int i=0; i<Integer.parseInt(args[1]); i++){
			String p = String.format("%s/part-%05d",args[0],i);
			SequenceFile.Reader reader = new SequenceFile.Reader(
				FileSystem.get(getConf()), new Path(p), getConf());
			IntWritable key = new IntWritable();
			Vertex value = new Vertex();
			while (reader.next(key,value)){
				if (key.get()==1059183205){
					System.out.println(value);
				}
			}
			reader.close();
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new Vertex(), args));
	}
}
