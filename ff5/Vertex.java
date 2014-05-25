package ff5;

import java.io.*;
import java.util.*;

public class Vertex implements org.apache.hadoop.io.Writable {
	public static int MAX_EDGE = 8001;

	int id, nE, nS, nT;
	Edge[] E = new Edge[MAX_EDGE];
	Excess[] S = new Excess[MAX_EDGE];
	Excess[] T = new Excess[MAX_EDGE];

	Comparator<Edge> toCmp = new Comparator<Edge>(){
		public int compare(Edge a, Edge b){
			int x = a.V, y = b.V;
			return (x < y)? -1 : ((x > y)? 1 : 0);
		}
	};

	public Vertex(){
		for (int i=0; i<E.length; i++) E[i] = new Edge();
		for (int i=0; i<S.length; i++) S[i] = new Excess(20);
		for (int i=0; i<T.length; i++) T[i] = new Excess(20);
	}
	public void init(int id){ this.id = id; nE = nS = nT = 0; }
	public void addS(Excess ex){ S[nS++].copy(ex); }
	public void addT(Excess ex){ T[nT++].copy(ex); }
	public boolean isEmpty(){ return nE==0 && nS==0 && nT==0; }
	public void addNewEdge(int V, int F, int C){ E[nE++].set(id,V,F,C); }
	public void sortEdges(){ Arrays.sort(E,0,nE,toCmp); }

	public int[] merge(Vertex v, Augmenter As, Augmenter At){
		if (id != v.id) throw new RuntimeException("Merging different id : " + id + " " + v.id);
		int added = 0;
		for (int i=0,j=0; j<v.nE; ){
			if (i>=nE){
				E[nE+(added++)].copy(v.E[j++]);
			} else {
				if (E[i].V < v.E[j].V){
					i++;
				} else if (E[i].V == v.E[j].V){
					E[i++].merge(v.E[j++]);
				} else {
					E[nE+(added++)].copy(v.E[j++]);
				}
			}
		}
		nE += added;
		if (nE>0 && added>0) sortEdges();

		// REMOVE THIS
		for (int i=1; i<nE; i++)
			if (E[i-1].V == E[i].V)
				throw new RuntimeException("" + toString(true));

		int sdrop=0, tdrop=0;
		for (int i=0; i<v.nS; i++){
			Excess ex = v.S[i];
			if (As.canAugment(ex)) addS(ex); else sdrop++;
		}
		for (int i=0; i<v.nT; i++){
			Excess ex = v.T[i];
			if (At.canAugment(ex)) addT(ex); else tdrop++;
		}
		return new int[]{ sdrop, tdrop};
	}

	public int[] updateEdgeFlows(Map<Long,Edge> AE){ // update E, S, T
		for (int i=0; i<nE; i++){
			Edge edge = E[i], aedge = AE.get(edge.id);
			if (aedge == null) continue;
			edge.setFlow(aedge.U,aedge.V,aedge.F);
		}
		int sSat = 0, tSat = 0;
		for (int i=0; i<nS; i++)
			if (S[i].augment(AE)){
				S[i].copy(S[--nS]);
				sSat++;
				i--;
			}
		for (int i=0; i<nT; i++)
			if (T[i].augment(AE)){
				T[i].copy(T[--nT]);
				tSat++;
				i--;
			}
		return new int[]{ sSat, tSat };
	}

	private void writeExcess(DataOutput out, Excess ex) throws IOException {
		int len = ex.getLength();
		out.writeInt(len);
		if (len>0){
			out.writeInt(ex.getFrom());
			for (Edge e : ex){
				out.writeInt(e.V);
				out.writeInt(e.F);
				out.writeInt(e.C);
			}
		}
		out.writeInt(ex.fset.size());
		for (int id : ex.fset) out.writeInt(id);
	}

	private void readExcess(DataInput in, Excess ex) throws IOException {
		ex.clear();
		int len = in.readInt();
		if (len > 0){
			int U = in.readInt();
			for (int i=0; i<len; i++){
				int V = in.readInt();
				int F = in.readInt();
				int C = in.readInt();
				ex.path[ex.B++].set(U,V,F,C);
				U = V;
			}
		}
		int n = in.readInt();
		for (int i=0; i<n; i++)
			ex.fset.add(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(nE);
		for (int i=0; i<nE; i++){
			Edge edge = E[i];
			out.writeInt(edge.V);
			out.writeInt(edge.F);
			out.writeInt(edge.C);
		}
		out.writeInt(nS); for (int i=0; i<nS; i++) writeExcess(out,S[i]);
		out.writeInt(nT); for (int i=0; i<nT; i++) writeExcess(out,T[i]);
	}

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		nE = in.readInt();
		for (int i=0; i<nE; i++){
			int V = in.readInt();
			int F = in.readInt();
			int C = in.readInt();
			E[i].set(id,V,F,C);
		}
		nS = in.readInt(); for (int i=0; i<nS; i++) readExcess(in,S[i]);
		nT = in.readInt(); for (int i=0; i<nT; i++) readExcess(in,T[i]);
	}

	public String toString(){
		return String.format("Vertex[%d] : E=%d; S=%d; T=%d",id,nE,nS,nT);
	}

	public String toString(boolean edges){
		String ret = String.format("Vertex[%d] : E=%d; S=%d; T=%d",id,nE,nS,nT);
		if (edges){ ret += "\nE ="; for (int i=0; i<nE; i++) ret += " ["+E[i]+"]"; }
		ret += "\nS ="; for (int i=0; i<nS; i++) ret += " ["+S[i]+"]";
		ret += "\nT ="; for (int i=0; i<nT; i++) ret += " ["+T[i]+"]";
		return ret;
	}
}
