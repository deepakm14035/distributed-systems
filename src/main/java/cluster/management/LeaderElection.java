package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
//mvn clean package
public class LeaderElection implements Watcher{
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT=3000;
    private static final String ELECTION_NAMESPACE="/election";
    private ZooKeeper zookeeper;
    private String currentZnodeName;
    private OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zookeeper, OnElectionCallback callback){
        this.zookeeper=zookeeper;
        onElectionCallback=callback;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException{
        String znodePrefix=ELECTION_NAMESPACE+"/c_";
        String znodeFullPath=zookeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name "+znodeFullPath);
        this.currentZnodeName=znodeFullPath.replace(ELECTION_NAMESPACE+"/","");
;    }

    public void electLeader() throws KeeperException,InterruptedException{
        List<String> children=zookeeper.getChildren(ELECTION_NAMESPACE,false);
        Collections.sort(children);
        String smallestChild=children.get(0);
        if(smallestChild.equals(currentZnodeName)){
            System.out.println("I am the leader");
            return;
        }
        System.out.println("I am not the leader, "+smallestChild+" is the leader");
    }

    public void reelectLeader() throws KeeperException,InterruptedException{
        Stat predecessorStat=null;
        String predecessorZnodeName="";
        while(predecessorStat==null) {
            List<String> children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                onElectionCallback.OnElectedToBeLeader();
                return;
            } else {
                System.out.println("I am not the leader");
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        onElectionCallback.OnWorker();
        System.out.println("Watching znode "+predecessorZnodeName);
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch(watchedEvent.getType()){
            case None:
                if(watchedEvent.getState()==Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to ZooKeeper");
                }else{
                    synchronized (zookeeper){
                        System.out.println("Disconnected from Zookeeper");
                        zookeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}
