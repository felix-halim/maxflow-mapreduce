package ff2;

import java.util.*;
import java.rmi.*;

public interface ApRemote extends Remote {
	public void echo(String s) throws RemoteException;
	public void prepare() throws RemoteException;
	public long[] finish(String graph, int round) throws RemoteException;
	public void augment(Collection<Excess> S, Collection<Excess> T) throws RemoteException;
}
