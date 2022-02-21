package cluster.management;

public interface OnElectionCallback {
    public void OnElectedToBeLeader();
    public void OnWorker();
}
