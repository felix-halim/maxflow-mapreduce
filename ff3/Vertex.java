package ff3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;

public class Vertex implements Writable {
	private int id;
	private Map<Integer,Edge> E = new HashMap<Integer,Edge>();
	private List<Excess> S = new ArrayList<Excess>();
	private List<Excess> T = new ArrayList<Excess>();

	public Vertex(){}
	public Vertex(Vertex v){
		this.id = v.id;
		this.E = new HashMap<Integer,Edge>(v.E);
		this.S.addAll(v.S);
		this.T.addAll(v.T);
	}
	public Vertex(int id){ this.id = id; }
	public int getId(){ return id; }
	public void setId(int id){ this.id = id; }
	public void addS(Excess exs){ S.add(exs); }
	public void addT(Excess exs){ T.add(exs); }
	public Collection<Edge> getEdges(){ return E.values(); }
	public List<Excess> getS(){ return S; }
	public List<Excess> getT(){ return T; }
	public boolean isEmpty(){ return E.size()==0 && S.size()==0 && T.size()==0; }

	public void addNewEdge(int V, int F, int C){
		if (id==V) throw new RuntimeException("Loop : "+id);
		if (E.put(V, new Edge(id,V,F,C))!=null)
			throw new RuntimeException("Edge "+V+" Exists");
	}

	public int[] merge(Vertex v, Augmenter As, Augmenter At){
		if (id != v.id) throw new RuntimeException("Merging different id : " + id + " " + v.id);
		for (Edge e : v.E.values()){
			Edge edge = E.get(e.getTo());
			if (edge == null){
				E.put(e.getTo(), new Edge(e));
			} else {
				edge.merge(e);
			}
		}
		int sdrop=0, tdrop=0;
		for (Excess ex : v.S) if (As.canAugment(ex)) S.add(ex); else sdrop++;
		for (Excess ex : v.T) if (At.canAugment(ex)) T.add(ex); else tdrop++;
		return new int[]{ sdrop, tdrop};
	}

	public int[] updateEdgeFlows(Map<Long,Edge> AE){ // update E, S, T
		for (Edge edge : E.values()){
			Edge aedge = AE.get(edge.getId());
			if (aedge == null) continue;
			edge.setFlow(aedge.getFrom(),aedge.getTo(),aedge.getFlowFromTo());
		}
		int sSat = 0, tSat = 0;
		for (Iterator<Excess> i=S.iterator(); i.hasNext(); )
			if (i.next().augment(AE)){ i.remove(); sSat++; }
		for (Iterator<Excess> i=T.iterator(); i.hasNext(); )
			if (i.next().augment(AE)){ i.remove(); tSat++; }
		return new int[]{ sSat, tSat };
	}

	public List<Excess> extendS(){
		List<Excess> newS = new ArrayList<Excess>();
		if (S.size()>0){
			for (Edge edge : E.values()){
				if (edge.getResidue(edge.getFrom(),edge.getTo()) <= 0) continue;
				Excess nS = new Excess(S.get((int) (Math.random() * S.size())));
				if (nS.addRight(id, edge.getTo(), edge.getFlowFromTo(), edge.getCapacity())) newS.add(nS);
			}
		}
		return newS;
	}

	public List<Excess> extendT(){
		List<Excess> newT = new ArrayList<Excess>();
		if (T.size()>0){
			for (Edge edge : E.values()){
				if (edge.getResidue(edge.getTo(),edge.getFrom()) <= 0) continue;
				Excess nT = new Excess(T.get((int) (Math.random() * T.size())));
				if (nT.addLeft(edge.getTo(), id, -edge.getFlowFromTo(), edge.getCapacity())) newT.add(nT);
			}
		}
		return newT;
	}

	private void writeExcess(DataOutput out, Excess ex) throws IOException {
		Collection<Edge> path = ex.getEdges();
		out.writeInt(path.size());
		if (path.size()>0) out.writeInt(ex.getFrom());
		for (Edge e : path){
			out.writeInt(e.getTo());
			out.writeInt(e.getFlowFromTo());
			out.writeInt(e.getCapacity());
		}
	}

	private Excess readExcess(DataInput in) throws IOException {
		Excess ex = new Excess();
		int N = in.readInt(), U = -1;
		if (N>0) U = in.readInt();
		for (int i=N; i>0; i--){
			int V = in.readInt();
			int F = in.readInt();
			int C = in.readInt();
			ex.addRight(U,V,F,C);
			U = V;
		}
		return ex;
	}


	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(E.size());
		for (Edge edge : E.values()){
			out.writeInt(edge.getTo());
			out.writeInt(edge.getFlowFromTo());
			out.writeInt(edge.getCapacity());
		}
		out.writeInt(S.size()); for (Excess ex : S) writeExcess(out,ex);
		out.writeInt(T.size()); for (Excess ex : T) writeExcess(out,ex);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		E.clear();
		for (int i=in.readInt(); i>0; i--){
			int V = in.readInt();
			int F = in.readInt();
			int C = in.readInt();
			addNewEdge(V,F,C);
		}
		S.clear();
		for (int i=in.readInt(); i>0; i--)
			S.add(readExcess(in));
		T.clear();
		for (int i=in.readInt(); i>0; i--)
			T.add(readExcess(in));
	}

	public String toString(){
		return String.format("Vertex[%d] : E=%d; S=%d; T=%d",id,E.size(),S.size(),T.size());
	}
}
