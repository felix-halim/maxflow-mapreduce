package ff3;

public class Edge implements java.io.Serializable {
	public static final long serialVersionUID = 8347568375638L;

	private int U,V,F,C; // from[0,N-1], to[0,N-1], flow [-C,+C], capacity
	
	Edge(Edge e){ this(e.U, e.V, e.F, e.C); }
	Edge(int u, int v, int f, int c){ U = u; V = v; F = f; C = c; check(); }

	// to convert back :: rH = (int) (X>>>32), rL = (int) (X);
	private long mergeInts(int H, int L){ return (((long)H)<<32) | (0xFFFFFFFFL&((long)L)); }
	public long getId(){ return (U < V)? mergeInts(U,V) : mergeInts(V,U); }
	public int getFrom(){ return U; }
	public int getTo(){ return V; }
	public int getFlowFromTo(){ return F; }
	public int getFlow(int u, int v){
		if (u==U && v==V) return F;
		if (u==V && v==U) return -F;
		throw new RuntimeException("Edge Mismatch " + U + " " + V + " != " + u + " " + v);
	}
	public int getCapacity(){ return C; }
	public void setFlow(int u, int v, int f){
		if (u==U && v==V) F = f;
		else if (u==V && v==U) F = -f;
		else throw new RuntimeException("Edge Mismatch " + U + " " + V + " != " + u + " " + v);
	}
	public void setCapacity(int C){ this.C = C; }
	public int getResidue(int u, int v){
		if (U==u && V==v) return C - F;
		if (U==v && V==u) return F + C;
		throw new RuntimeException("Edge Mismatch " + U + " " + V + " != " + u + " " + v);
	}
	public void check(){ if (Math.abs(F) > C) throw new RuntimeException("Overflow : F="+F+"; C="+C); }
	public void merge(Edge e){
		if (e.U != U) throw new RuntimeException("Mismatch U : "+e.U + " != " + U);
		if (e.V != V) throw new RuntimeException("Mismatch V : "+e.V + " != " + V);
		F += e.F;
		C += e.C;
		check();
	}
}
