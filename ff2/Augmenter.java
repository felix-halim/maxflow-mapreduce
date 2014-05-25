package ff2;

import java.util.*;

public class Augmenter {
	private Map<Long,Edge> augEdges = new HashMap<Long,Edge>();
	
	public Collection<Edge> getEdges(){ return augEdges.values(); }
	public int size(){ return augEdges.size(); }
	public void clear(){ augEdges.clear(); }

	public int getFlow(Excess ex){
		int ret = Integer.MAX_VALUE;
		for (Edge edge : ex.getEdges()){
			int u = edge.getFrom(), v = edge.getTo();
			ret = Math.min(ret, edge.getResidue(u,v));

			Edge aedge = augEdges.get(edge.getId());
			if (aedge != null)
				ret = Math.min(ret, aedge.getResidue(u,v));
		}
		return ret;
	}

	public void augmentFlow(Excess ex, int flowAmt){
		for (Edge edge : ex.getEdges()){
			Edge aedge = augEdges.get(edge.getId());
			if (aedge == null){
				aedge = new Edge(edge);
				augEdges.put(edge.getId(), aedge);
			}
			int u = edge.getFrom(), v = edge.getTo();
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
