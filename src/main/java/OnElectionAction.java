import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistry;
import networking.WebClient;
import networking.WebServer;
import org.apache.zookeeper.KeeperException;
import search.SearchCoordinator;
import search.SearchWorker;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {

    private final ServiceRegistry workerServiceRegistry;
    private final ServiceRegistry coordinatorServiceRegistry;
    private final int port;
    private WebServer webServer;

    public OnElectionAction(ServiceRegistry workerServiceRegistry, ServiceRegistry coordinatorServiceRegistry, int port) {
        this.workerServiceRegistry = workerServiceRegistry;
        this.coordinatorServiceRegistry=coordinatorServiceRegistry;
        this.port=port;
    }


    @Override
    public void OnElectedToBeLeader() {
        workerServiceRegistry.unregisterFromCluster();
        workerServiceRegistry.registerForUpdates();

        if(webServer!=null){
            webServer.stop();
        }

        SearchCoordinator searchCoordinator=new SearchCoordinator(workerServiceRegistry, new WebClient());
    }

    @Override
    public void OnWorker() {
        SearchWorker searchWorker=new SearchWorker();
        webServer=new WebServer(port, searchWorker);
        webServer.startServer();
        try{
            String currentServerAddress=String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(), port, searchWorker.getEndpoint());
            workerServiceRegistry.registerToCluster(currentServerAddress);
        }catch(InterruptedException| UnknownHostException| KeeperException e){
            e.printStackTrace();
            return;
        }
    }
}
