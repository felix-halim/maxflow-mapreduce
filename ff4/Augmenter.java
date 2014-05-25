package ff4;

import java.util.*;

public class Augmenter {
	private Map<Long,Edge> augEdges = new HashMap<Long,Edge>();
	
	public Collection<Edge> getEdges(){ return augEdges.values(); }
	public int size(){ return augEdges.size(); }
	public void clear(){ augEdges.clear(); }

	public int getFlow(Iterable<Edge> ex){
		int ret = Integer.MAX_VALUE;
		for (Edge edge : ex){
			int u = edge.U, v = edge.V;
			ret = Math.min(ret, edge.getResidue(u,v));

			Edge aedge = augEdges.get(edge.id);
			if (aedge != null)
				ret = Math.min(ret, aedge.getResidue(u,v));
		}
		return ret;
	}

	public void augmentFlow(Iterable<Edge> ex, int flowAmt){
		for (Edge edge : ex){
			Edge aedge = augEdges.get(edge.id);
			if (aedge == null){
				aedge = new Edge();
				aedge.copy(edge);
				augEdges.put(edge.id, aedge);
			}
			int u = edge.U, v = edge.V;
			int oflow = aedge.getFlow(u,v);
			aedge.setFlow(u, v, oflow + flowAmt);
		}
	}

	// return true if saturated
	public boolean canAugment(Excess ex){
		int flow = getFlow(ex);
		if (flow > 0) augmentFlow(ex,flow);
		return flow > 0;
	}
}
