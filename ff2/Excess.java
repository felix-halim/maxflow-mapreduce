package ff2;

import java.io.*;
import java.util.*;

public class Excess implements Serializable {
	public static final long serialVersionUID = 86483746853232323L;

	private Set<Integer> idSet = new HashSet<Integer>();
	private LinkedList<Edge> path = new LinkedList<Edge>();

	public Excess(){}
	public Excess(Excess E){
		this.path.addAll(E.path);
		this.idSet.addAll(E.idSet);
	}
	public int getTo(){ return path.getLast().getTo(); }
	public int getFrom(){ return path.getFirst().getFrom(); }
	public int getLength(){ return path.size(); }
	public LinkedList<Edge> getEdges(){ return path; }
	public void removeLeft(){ idSet.remove(path.removeFirst().getFrom()); }
	public void removeRight(){ idSet.remove(path.removeLast().getTo()); }
	public void concat(Excess that){ // cycle is handled!
		if (getTo() != that.getFrom()) throw new RuntimeException("Not Linked\n"+this+"\nWith\n"+that);
		for (Edge edge : that.getEdges()){
			if (idSet.contains(edge.getTo())){
				while (getTo() != edge.getTo()) removeRight();
			} else {
				int u = edge.getFrom(), v = edge.getTo();
				addRight(u, v, edge.getFlow(u,v), edge.getCapacity());
			}
		}
	}

	public boolean augment(Map<Long,Edge> augPaths){
		int flow = Integer.MAX_VALUE;
		for (Edge edge : path){
			Edge aedge = augPaths.get(edge.getId());
			if (aedge != null){
				edge.setFlow(aedge.getFrom(),aedge.getTo(),aedge.getFlowFromTo());
				if (edge.getResidue(edge.getFrom(),edge.getTo())==0) return true;
			}
			flow = Math.min(flow, edge.getResidue(edge.getFrom(),edge.getTo()));
		}
		if (flow==0) throw new RuntimeException("INVALID, but acc?");
		return false;
	}

	public boolean addLeft(int U, int V, int F, int C){ // clone the edge
		Edge e = new Edge(U,V,F,C);
		if (path.size()==0){
			idSet.add(e.getFrom());
			idSet.add(e.getTo());
			path.add(e);
		} else {
			if (e.getTo()!=getFrom()) throw new RuntimeException("Mismatch "+ e.getTo() + " != "+getFrom());
			if (idSet.contains(e.getFrom())) return false;
			idSet.add(e.getFrom());
			path.addFirst(e);
		}
		return true;
	}

	public boolean addRight(int U, int V, int F, int C){
		Edge e = new Edge(U,V,F,C);
		if (path.size()==0){
			idSet.add(e.getFrom());
			idSet.add(e.getTo());
			path.add(e);
		} else {
			if (e.getFrom()!=getTo()) throw new RuntimeException("Mismatch "+ e.getFrom() + " != "+getTo());
			if (idSet.contains(e.getTo())) return false;
			idSet.add(e.getTo());
			path.addLast(e);
		}
		return true;
	}

	public String toString(){
		String ret = "" + getFrom();
		for (Edge e : path){
			ret += " (" + e.getFlowFromTo() + "/" + e.getCapacity() + ") " + e.getTo();
		}
		return ret;
	}
}
