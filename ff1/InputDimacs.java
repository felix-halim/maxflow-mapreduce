package ff1;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;

public class InputDimacs extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		String inputFile = args[0], inputGraph = args[1];
		BufferedReader bf = new BufferedReader(new FileReader(inputFile));
		int nRecords = 0, N = -1, M = -1, source = -1, sink = -1;
		Map<Integer,Map<Integer,int[]>> con = new TreeMap<Integer,Map<Integer,int[]>>();
		for (String s=bf.readLine(); s!=null; s=bf.readLine()){
			String[] ss = s.split(" ");
			if (ss[0].equals("c")){
			} else if (ss[0].equals("p")){
				N = Integer.parseInt(ss[2]);
				M = Integer.parseInt(ss[3]);
			} else if (ss[0].equals("n")){
				int id = Integer.parseInt(ss[1]);
				if (ss[2].equals("s")) source = id;
				else if (ss[2].equals("t")) sink = id;
				else throw new RuntimeException("Invalid : "+s);
			} else if (ss[0].equals("a")){
				int a = Integer.parseInt(ss[1]);
				int b = Integer.parseInt(ss[2]);
				int c = Integer.parseInt(ss[3]);
				if (!con.containsKey(a)) con.put(a,new TreeMap<Integer,int[]>());
				Map<Integer,int[]> toCap = con.get(a);
				if (toCap.containsKey(b)){
					toCap.get(b)[1] += c;
				} else {
					toCap.put(b,new int[]{0,c});
				}

				if (!con.containsKey(b)) con.put(b,new TreeMap<Integer,int[]>());
				toCap = con.get(b);
				if (!toCap.containsKey(a)) toCap.put(a, new int[]{0,0});
			}
			nRecords++;
			if (nRecords%1000000==0){ System.err.print("."); System.err.flush(); }
		}
		bf.close();

		// make bidirectional flow
		for (int a : con.keySet()){
			for (Map.Entry<Integer,int[]> bc : con.get(a).entrySet()){
				int b = bc.getKey(), cab = bc.getValue()[1], cba = 0;
				if (a>=b) continue;
				if (!con.containsKey(b)) con.put(b, new TreeMap<Integer,int[]>());
				if (con.get(b).containsKey(a)) cba = con.get(b).get(a)[1];
				int sum = cab + cba;
				bc.setValue(new int[]{ cba, sum });
				con.get(b).put(a, new int[]{ cab, sum });
			}
		}

		FileSystem hdfs = FileSystem.get(getConf());
		if (hdfs.exists(new Path(inputGraph))) hdfs.delete(new Path(inputGraph));

		Configuration conf = new Configuration();
		SequenceFile.Writer writer = SequenceFile.createWriter(
			hdfs, getConf(), new Path(inputGraph),
			IntWritable.class, Vertex.class);

		for (int id : con.keySet()){
			Vertex node = new Vertex(id);
			for (Map.Entry<Integer,int[]> tc : con.get(id).entrySet()){
				int to = tc.getKey(), flow = tc.getValue()[0], cap = tc.getValue()[1];
				if (to==node.getId()) continue;
				node.addEdge(new Edge(id,to,0,flow,cap));
			}
			writer.append(new IntWritable(node.getId()), node);
		}
		writer.close();
		return 0;
	}	
}
