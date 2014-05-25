package ff5;

import java.io.*;
import java.util.*;

public class Excess implements Serializable, Iterable<Edge> {
	public static final long serialVersionUID = 86483746853232323L;

	TreeSet<Integer> fset = new TreeSet<Integer>();
	Edge[] path;
	int F,B;

	public Excess(int n){
		F = B = 0;
		path = new Edge[n];
		for (int i=0; i<n; i++)
			path[i] = new Edge();
	}

	public class ExcessIterator implements Iterator<Edge> {
		int i;
		public void setI(int i){ this.i = i; }
		public boolean hasNext(){ return i != B; }
		public Edge next(){
			Edge ret = path[i];
			i = (i+1) % path.length;
			return ret;
		}
		public void remove(){
			i = (path.length + i - 1) % path.length;
			B = (path.length + B - 1) % path.length;
			path[i].copy(path[B]);
			throw new RuntimeException("Erase not supported");
		}
	}

	ExcessIterator it = new ExcessIterator();

	public List<Edge> compact(){
		List<Edge> ret = new ArrayList<Edge>();
		for (Edge e : this){
			Edge ne = new Edge();
			ne.copy(e);
			ret.add(ne);
		}
		return ret;
	}

	public void clear(){ F = B = 0; fset.clear(); }
	public void copy(Excess ex){ clear(); for (Edge e : ex) path[B++].copy(e); fset.addAll(ex.fset); }
	public int getTo(){ return path[(path.length + B - 1) % path.length].V; }
	public int getFrom(){ return path[F].U; }
	public int getLength(){ int t = B-F; return t<0? t+path.length : t; }
	public Iterator<Edge> iterator(){ it.setI(F); return it; }

	public boolean augment(Map<Long,Edge> augPaths){
		int flow = Integer.MAX_VALUE;
		for (Edge edge : this){
			Edge aedge = augPaths.get(edge.id);
			if (aedge != null){
				edge.setFlow(aedge.U,aedge.V,aedge.F);
				if (edge.getResidue(edge.U,edge.V)==0) return true;
			}
			flow = Math.min(flow, edge.getResidue(edge.U,edge.V));
		}
		if (flow==0) throw new RuntimeException("INVALID, but acc?");
		return false;
	}

	public boolean addLeft(int U, int V, int F, int C){
		if (getLength()>0){
			if (V!=getFrom()) throw new RuntimeException("Mismatch "+ V + " != "+getFrom());
			for (Edge e : this) if (e.U==U || e.V==U) return false;
		}
		this.F = (path.length + this.F - 1) % path.length;
		path[this.F].set(U,V,F,C);
		return true;
	}

	public boolean addRight(int U, int V, int F, int C){
		if (getLength()>0){
			if (U!=getTo()) throw new RuntimeException("Mismatch "+ U + " != "+getTo());
			for (Edge e : this) if (e.U==V || e.V==V) return false;
		}
		path[B].set(U,V,F,C);
		B = (B + 1) % path.length;
		return true;
	}

	public String toString(){
		String ret = "" + getFrom();
		for (Edge e : this) ret += " (" + e.F + "/" + e.C + ") " + e.V;
		return ret;
	}
}
