package ff5;

public class Edge implements java.io.Serializable {
	public static final long serialVersionUID = 8347568375638L;

	long id;
	int U,V,F,C; // from[0,N-1], to[0,N-1], flow [-C,+C], capacity

	// to convert back :: rH = (int) (X>>>32), rL = (int) (X);
	private long mergeInts(int H, int L){ return (((long)H)<<32) | (0xFFFFFFFFL&((long)L)); }
	public void setId(int u, int v){ U=u; V=v; id = (U < V)? mergeInts(U,V) : mergeInts(V,U); }
	public void copy(Edge E){ setId(E.U, E.V); F = E.F; C = E.C; }
	public void set(int u, int v, int f, int c){ setId(u,v); F = f; C = c; }

	public int getResidue(int u, int v){ return C - getFlow(u,v); }
	public int getFlow(int u, int v){
		if (u==U && v==V) return F;
		if (u==V && v==U) return -F;
		throw new RuntimeException("Edge Mismatch " + U + " " + V + " != " + u + " " + v);
	}
	public void setFlow(int u, int v, int f){
		if (u==U && v==V) F = f;
		else if (u==V && v==U) F = -f;
		else throw new RuntimeException("Edge Mismatch " + U + " " + V + " != " + u + " " + v);
	}
	public void merge(Edge e){
		if (id != e.id) throw new RuntimeException(String.format("Mismatch (%d,%d) != (%d,%d)",U,V,e.U,e.V));
		F += e.F; C += e.C;
		if (Math.abs(F) > C) throw new RuntimeException("Overflow : F="+F+"; C="+C);
	}
	public String toString(){ return U+","+V+","+F+","+C; }
}
