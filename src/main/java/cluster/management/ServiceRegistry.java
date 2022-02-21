package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry  implements Watcher {
    public static String WORKERS_REGISTRY_NODE="/workers_service_registry";
    public static String COORDINATORS_REGISTRY_NODE="/coordinators_service_registry";
    private static String Z_NODE_NAME;
    private final ZooKeeper zookeeper;
    private String nodeName;
    private List<String> addressList;

    public ServiceRegistry(ZooKeeper zooKeeper, String znodeName){
        zookeeper=zooKeeper;
        Z_NODE_NAME = znodeName;
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        nodeName = zookeeper.create(Z_NODE_NAME+"/n_",metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to the cluster");
    }

    public void unregisterFromCluster() {
        try {
            if (nodeName != null && zookeeper.exists(nodeName, false) != null) {
                zookeeper.delete(nodeName, -1);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if (addressList == null) {
            updateAddresses();
        }
        return addressList;
    }

    public void CreateServiceRegistryNode(){
        try{
            if(zookeeper.exists(Z_NODE_NAME, false)==null){
                zookeeper.create(Z_NODE_NAME,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


            }
        }catch(KeeperException | InterruptedException e){
            e.printStackTrace();
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workers = zookeeper.getChildren(WORKERS_REGISTRY_NODE, this);

        List<String> addresses = new ArrayList<>(workers.size());

        for (String worker : workers) {
            String serviceFullpath = WORKERS_REGISTRY_NODE + "/" + worker;
            Stat stat = zookeeper.exists(serviceFullpath, false);
            if (stat == null) {
                continue;
            }

            byte[] addressBytes = zookeeper.getData(serviceFullpath, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }

        this.addressList = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are: " + this.addressList);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
    }
}
