package ff3;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class ApServer implements ApRemote {
	BlockingQueue<List<Excess>[]> q = new LinkedBlockingQueue<List<Excess>[]>();
	LinkedBlockingQueue<Integer> jctr = new LinkedBlockingQueue<Integer>(50);
	Augmenter A = new Augmenter();
	int echonum, nFlows, nPaths, nP;
	FileSystem hdfs;

	public ApServer(FileSystem hdfs){
		this.hdfs = hdfs;
		new Thread(new Runnable(){
			public void run() {
				try { while (true){ while (process()); jctr.put(new Integer(1)); } }
				catch (InterruptedException ex) { throw new RuntimeException(ex); }
			}
			boolean process() throws InterruptedException {
				if (q.size()>nP) nP = q.size();
				List<Excess>[] os = q.take();
				if (os[0]==null || os[1]==null) return false;
				LinkedList<Excess> sL = new LinkedList<Excess>(os[0]);
				LinkedList<Excess> tL = new LinkedList<Excess>(os[1]);
				Collections.shuffle(sL);
				Collections.shuffle(tL);
				for (Excess sE, tE; sL.size() > 0 && tL.size() > 0; ){
					do { sE = sL.removeLast(); } while (A.getFlow(sE)==0 && sL.size()>0);
					do { tE = tL.removeLast(); } while (A.getFlow(tE)==0 && tL.size()>0);
					if (A.getFlow(sE)==0) break; else sL.addLast(sE);
					if (A.getFlow(tE)==0) break; else tL.addLast(tE);
					int flow = Math.min(A.getFlow(sE), A.getFlow(tE));
					if (flow <= 0) throw new RuntimeException("Zero flow");
					A.augmentFlow(sE, flow);
					A.augmentFlow(tE, flow);
					nFlows += flow;
					nPaths++;
				}
				return true;
			}
		}).start();
	}
	
	@SuppressWarnings("unchecked")
    public void augment(List<Excess> S, List<Excess> T) {
		try { q.put(new List[]{ S, T }); }
		catch (InterruptedException ex){ throw new RuntimeException(ex); }
    }

	@SuppressWarnings("unchecked")
	public long[] finish(String flowOutputFile){
		try {
			q.put(new List[]{ null, null });
			jctr.take(); // wait until finish

			FSDataOutputStream dos = hdfs.create(new Path(flowOutputFile));
			dos.writeInt(A.size());
			for (Edge e : A.getEdges()){
				dos.writeInt(e.getFrom());
				dos.writeInt(e.getTo());
				dos.writeInt(e.getFlowFromTo());
			}
			dos.close();
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
		return new long[]{ A.size(), nFlows, nPaths, nP };
	}

	public void clear(){ A.clear(); nFlows = nPaths = nP = 0; }
	public void echo(String s){ System.out.printf("echo [%d]: %s\n",++echonum,s); }

	public static void main(String[] args) throws Exception {
		ApServer obj = new ApServer(FileSystem.get(new Configuration()));
		ApRemote stub = (ApRemote) UnicastRemoteObject.exportObject(obj, 0);
		Registry registry = LocateRegistry.getRegistry();
		try { registry.unbind("ApRemote3"); } catch (NotBoundException nbe){}
		registry.bind("ApRemote3", stub);
		System.out.println("ApServer3 is READY!");
	}
}
