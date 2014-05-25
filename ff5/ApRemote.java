package ff5;

import java.util.*;
import java.rmi.*;

public interface ApRemote extends Remote {
	public void clear() throws RemoteException;
	public void echo(String s) throws RemoteException;
	public long[] finish(String flowPath) throws RemoteException;
	public void augment(List<List<Edge>> S, List<List<Edge>> T) throws RemoteException;

	public void addIds(List<Integer> ids) throws RemoteException;
	public int[][] getSourcesAndSinks(int n) throws RemoteException;
}
