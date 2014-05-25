package ff1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Excess implements WritableComparable<Excess> {
	public static final int MAX_FLOW = Integer.MAX_VALUE/2;

	private Set<Integer> idSet = new HashSet<Integer>();
	private LinkedList<Edge> path = new LinkedList<Edge>();
	private int from, to;

	public Excess(){}
	public Excess(int from){
		this.from = from;
		this.to = from;
		this.idSet.add(from);
	}
	public Excess(Excess E){
		this.from = E.from;
		this.to = E.to;
		this.path.addAll(E.path);
		this.idSet.addAll(E.idSet);
	}
	public int getTo(){ return to; }
	public int getFrom(){ return from; }
	public int getLength(){ return 1+path.size(); }
	public int getPotential(){ return path.size()==0? MAX_FLOW : path.get(0).getPotential(from); }
	public Collection<Edge> getEdges(){ return path; }

	public void updatePotential(){
		int pot = Excess.MAX_FLOW;
		int prev = from;
		for (Edge edge : path){
			edge.setPotential(prev, 0);
			pot = Math.min(pot, edge.getResidue(prev));
			prev = edge.getOtherPair(prev);
		}
		prev = from;
		for (Edge edge : path){
			edge.setPotential(prev, pot);
			prev = edge.getOtherPair(prev);
		}
	}

	public boolean concat(Excess that){ // cycle is handled!
		if (to != that.from) throw new RuntimeException(
			"Not Linked\n"+this+"\nWith\n"+that);
		int prev = that.getFrom(), plen = getLength();
		for (Edge edge : that.getEdges()){
			int id = edge.getOtherPair(prev);
			if (!idSet.contains(id)){
				addEdge(edge, false);
			} else {
				while (to != id){
					int newTo = path.removeLast().getOtherPair(to);
					idSet.remove(to);
					to = newTo;
				}
			}
			prev = id;
		}
		if (plen>getLength()) return false; //throw new RuntimeException("Aih");
		updatePotential();
		return true;
	}

	public void augment(Map<Long,Edge> augPaths){
		boolean updated = false;
		for (Edge edge : path){
			Edge aedge = augPaths.get(edge.getId());
			if (aedge == null) continue;
			int ref = aedge.getReference();
			edge.setPotential(ref, 0);
			edge.setFlow(ref, edge.getFlow(ref) + aedge.getPotential(ref));
			updated = true;
		}
		if (updated) updatePotential();
	}

	public void removeEdge(boolean left){
		if (path.size()==0) throw new RuntimeException("No edge left");
		if (left){
			Edge edge = path.removeFirst();
			if (edge.getA()!=from && edge.getB()!=from) throw new RuntimeException("X");
			from = edge.getOtherPair(from);
		} else {
			Edge edge = path.removeLast();
			if (edge.getA()!=to && edge.getB()!=to) throw new RuntimeException("X");
			to = edge.getOtherPair(to);
		}
	}

	public boolean addEdge(Edge e, boolean left){ // clone the edge
		Edge edge = new Edge(e);
		edge.setPotential(edge.getReference(), 0);
		int oldPot = getPotential(), newRes = 0;
		if (oldPot < 0) throw new RuntimeException("EX");
		if (left){
			if (edge.getA()!=from && edge.getB()!=from) throw new RuntimeException(
				"Not linked edge = "+ edge + " with this excess : \n"+this);
			int newId = edge.getOtherPair(from);
			if (idSet.contains(newId)) return false;
			newRes = edge.getResidue(newId);
			idSet.add(newId);
			path.addFirst(edge);
			from = newId;
		} else {
			if (edge.getA()!=to && edge.getB()!=to) throw new RuntimeException(
				"Not linked edge = "+ edge + " with this excess : \n"+this);
			int newId = edge.getOtherPair(to);
			if (idSet.contains(newId)) return false;
			newRes = edge.getResidue(to);
			idSet.add(newId);
			path.addLast(edge);
			int oldTo = to;
			to = newId;
		}
		if (newRes < 0) throw new RuntimeException("EX");
		path.get(0).setPotential(from, Math.min(oldPot, newRes));
		if (getPotential()==0) throw new RuntimeException("Zero pot");
		return true;
	}

	@Override
	public boolean equals(Object o){
		Excess that = (Excess) o;
		return from==that.from && to==that.to && getEdges().equals(that.getEdges());
	}

	@Override
	public int hashCode(){
		return (from*123 + to)*13 + getEdges().hashCode();
	}

	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for (Edge edge : getEdges()) sb.append("\t"+edge.toString()+"\n");
		return String.format("From = %d, To = %d, Length = %d, Potential = %d, Paths : \n%s\n",
			from,to,getLength(),getPotential(),sb);
	}

	@Override
	public int compareTo(Excess that){
		int cmpPot = new Integer(that.getPotential()).compareTo(getPotential());
		if (cmpPot != 0) return cmpPot; // descending flow
		int cmpLen = getLength() - that.getLength();	
		if (cmpLen != 0) return cmpLen; // ascending path length
		for (int i=0; i<path.size(); i++){
			Long a = path.get(i).getId();
			int cmpPath = a.compareTo(that.path.get(i).getId());
			if (cmpPath != 0) return cmpPath;
		}
		return 0;
	}

	@Override public void write(DataOutput out) throws IOException {
		out.writeInt(from);
		out.writeInt(to);
		out.writeInt(path.size());
		for (Edge edge : path) edge.write(out);
	}

	@Override public void readFields(DataInput in) throws IOException {
		idSet.clear();
		idSet.add(from = in.readInt());
		idSet.add(to = in.readInt());
		path.clear();
		for (int i=in.readInt(); i>0; i--){
			Edge edge = new Edge();
			edge.readFields(in);
			path.add(edge);
			idSet.add(edge.getA());
			idSet.add(edge.getB());
		}
		updatePotential();
	}
}

		/*
class MaxFlowGraph {
	private int pivotId;
	private Comparator<Edge> resCmp = new Comparator<Edge>(){
		public int compare(Edge a, Edge b){
			return new Integer(b.getResidue(pivotId)).compareTo(a.getResidue(pivotId));
		}
	};

	private Map<Integer,Set<Edge>> con = new HashMap<Integer,Set<Edge>>();
	private Map<Long,Edge> edges = new HashMap<Long,Edge>();
	private int from, to;

	public int size(){ return edges.size(); }
	public void clear(){ con.clear(); edges.clear(); }

	public void add(Excess excess){
		if (con.size()==0){ from=excess.from; to=excess.to; }
		else if (from!=excess.from || to!=excess.to) throw new RuntimeException(
			"From = "+from+" to = " + to + " Edges = "+edges+"\nThat = "+excess+"\n");
		for (Edge e : excess.getEdges()){
			Edge edge = new Edge(e);
			edge.setPotential(edge.getReference(),0);
			if (edges.containsKey(edge.getId())) continue; else edges.put(edge.getId(), edge);
			if (!con.containsKey(edge.getA())) con.put(edge.getA(), new HashSet<Edge>());
			if (!con.get(edge.getA()).add(edge)) throw new RuntimeException("Hey");
			if (!con.containsKey(edge.getB())) con.put(edge.getB(), new HashSet<Edge>());
			if (!con.get(edge.getB()).add(edge)) throw new RuntimeException("Hey");
		}
	}

	public Excess extract(){
		Excess excess = augmentingPath();
		if (excess == null) return null;
		excess.updatePotential();
		return excess;
	}

	private Excess augmentingPath(){
		LinkedList<Integer> queue = new LinkedList<Integer>();
		Map<Integer,Edge> parent = new TreeMap<Integer,Edge>();
		queue.offer(from);
		while (queue.size()>0){
			int u = queue.poll();
			if (u == to) break;
			List<Edge> cons = new ArrayList(con.get(u));
			pivotId = u;
			Collections.sort(cons, resCmp);
			for (Edge edge : cons){
				int v = edge.getOtherPair(u);
				if (parent.containsKey(v) || edge.getResidue(u) == 0) continue;
				parent.put(v, edge);
				if (v==to){ queue.clear(); break; }
				queue.offer(v);
			}
		}
		if (!parent.containsKey(to)) return null;
		int flow = MAX_FLOW;
		for (int i=to; i!=from; ){
			Edge edge = parent.get(i);
			i = edge.getOtherPair(i);
			flow = Math.min(flow, edge.getResidue(i));
		}
		if (flow<=0) throw new RuntimeException(flow+"\n"+parent);
		for (int i=to; i!=from; ){
			Edge edge = parent.get(i);
			i = edge.getOtherPair(i);
			edge.setPotential(i,edge.getPotential(i)+flow);
		}

		Excess ret = new Excess(to);
		for (int i=to; i!=from; ){
			Edge edge = parent.get(i);
			i = edge.getOtherPair(i);
			ret.addEdge(edge,true);
		}
		ret.updatePotential();
		return ret;
	}
}
		*/

class AugmentingPaths {
	private Map<Long,Edge> augEdges = new HashMap<Long,Edge>();

	public Collection<Edge> getEdges(){
		return augEdges.values();
	}

	public int accept(Excess excess){
		if (excess.getLength()==1) return Excess.MAX_FLOW;
		if (excess.getPotential() <= 0)
			throw new RuntimeException("ExcessList non positive : "+ excess);

		int maxFlow = excess.getPotential();
		for (int i=0; i<2; i++){ // 0:check, 1:perform
			for (Edge edge : excess.getEdges()){
				Edge aedge = augEdges.get(edge.getId());
				if (aedge == null){
					if (i==0){
						aedge = new Edge(edge);
						int ref = aedge.getReference();
						aedge.setPotential(ref, 0);
						augEdges.put(aedge.getId(), aedge);
						maxFlow = Math.min(maxFlow,aedge.getResidue(ref));
					} else {
						throw new RuntimeException("HE?");
					}
				} else {
					int ref = edge.getReference();
					if (i==0){
						maxFlow = Math.min(maxFlow,aedge.getResidue(ref));
						if (maxFlow == 0) return 0;
					} else {
						aedge.setPotential(ref, aedge.getPotential(ref) + maxFlow);
					}
				}
			}
			if (maxFlow < 0) throw new RuntimeException("AAA");
		}
		return maxFlow;
	}
}
