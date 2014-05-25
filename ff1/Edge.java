package ff1;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;

public class Edge implements Writable, Comparable<Edge> {
	private int a, b, pot, flow, capacity;
	private long id; // the concatenation of two ints: a and b

	public Edge(){}
	public Edge(Edge e){ this(e.a, e.b, e.pot, e.flow, e.capacity); }
	public Edge(int a, int b, int pot, int flow, int capacity){
		if (a>b){
			this.a = b;
			this.b = a;
			this.pot = -pot;
			this.flow = capacity - flow;
			this.capacity = capacity;
		} else {
			this.a = a;
			this.b = b;
			this.pot = pot;
			this.flow = flow;
			this.capacity = capacity;
		}
		this.id = mergeInts(this.a, this.b);
	}
	
	// to convert back :: rH = (int) (X>>>32), rL = (int) (X);
	private static long mergeInts(int H, int L){
		return (((long)H)<<32) | (0xFFFFFFFFL&((long)L));
	}

	public void mustHas(int id){
		if (a!=id && b!=id) throw new RuntimeException("Edge "+this+" doesn't has "+id);
	}

	public void merge(Edge that){
		pot += that.pot;
		flow += that.flow;
		capacity += that.capacity;
	}

	public long getId(){ return id; }
	public int getA(){ return a; }
	public int getB(){ return b; }
	public int getReference(){ return (pot>0)? a : b; }
	public int getOtherPair(int x){ return x==a? b : a; }
	public int getFlow(int from){ return from==a? flow : capacity-flow; }
	public int getPotential(int from){ return from==a? pot : -pot; }
	public int getResidue(int from){ return capacity - getFlow(from) - getPotential(from); }
	public int getInternalFlow(){ return flow; }
	public int getInternalPotential(){ return pot; }
	public int getInternalCapacity(){ return capacity; }
	public void setInternalFlow(int flow){ this.flow = flow; }
	public void setInternalPotential(int pot){ this.pot = pot; }
	public void setInternalCapacity(int cap){ this.capacity = cap; }
	public void setFlow(int from, int amt){ this.flow = (from==a)? amt : capacity-amt; check(); }
	public void setPotential(int from, int amt){
		this.pot = (from==a)? amt : -amt;
		if (getResidue(from) < 0) throw new RuntimeException("Negative Residue "+this);
		if (getResidue(from) > capacity) throw new RuntimeException("Residue > capacity "+this);
		check();
	}
	public void check(){
		if (flow < 0) throw new RuntimeException("Negative Flow "+this);
		if (flow > capacity) throw new RuntimeException("Flow > Capacity "+this);
		if (flow + pot < 0) throw new RuntimeException("Flow + Pot < 0 "+this);
		if (flow + pot > capacity) throw new RuntimeException("Flow + Pot > capacity "+this);
	}
	@Override public int hashCode(){ return new Long(id).hashCode(); }
	@Override public boolean equals(Object o){ return id == ((Edge)o).id; }
	@Override public int compareTo(Edge that){ return new Long(id).compareTo(that.id); }
	@Override public String toString(){ return pot>=0? 
		String.format("%d -> %d (f=%d,p=%d,c=%d)",a,b,flow,pot,capacity) :
		String.format("%d -> %d (f=%d,p=%d,c=%d)",b,a,capacity-flow,-pot,capacity); }
	@Override public void write(DataOutput out) throws IOException {
		out.writeInt(a);
		out.writeInt(b);
		//out.writeInt(pot);
		out.writeInt(flow);
		out.writeInt(capacity);
	}
	@Override public void readFields(DataInput in) throws IOException {
		a = in.readInt();
		b = in.readInt();
		id = mergeInts(a,b);
		pot = 0; //in.readInt();
		flow = in.readInt();
		capacity = in.readInt();
	}
}
