package search;

import cluster.management.ServiceRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import model.DocumentData;
import model.Result;
import model.SerializationUtils;
import model.Task;
import model.proto.SearchModel;
import networking.OnRequestCallback;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SearchCoordinator implements OnRequestCallback {
    private static final String ENDPOINT="/search";
    private static final String BOOKS_DIRECTORY="./resources/books/";
    private final ServiceRegistry workerServiceRegistry;
    private final WebClient client;
    private List<String> documents;

    public SearchCoordinator(ServiceRegistry serviceRegistry, WebClient webClient){
        workerServiceRegistry=serviceRegistry;
        client=webClient;
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        SearchModel.Request request= null;
        try {
            request = SearchModel.Request.parseFrom(requestPayload);
            SearchModel.Response response = createResponse(request);
            return response.toByteArray();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance().toByteArray();
        }
    }

    private SearchModel.Response createResponse(SearchModel.Request searchRequest){
        SearchModel.Response.Builder searchResponse = SearchModel.Response.newBuilder();

        System.out.println("Received search query: "+searchRequest.getSearchQuery());

        List<String> searchTerms = TFIDF.getWordsFromLine(searchRequest.getSearchQuery());

        List<String> workers = null;
        try {
            workers = workerServiceRegistry.getAllServiceAddresses();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(workers==null || workers.isEmpty()){
            System.out.println("No search workers currently available");
            return searchResponse.build();
        }
        List<Task> tasks = createTasks(workers.size(), searchTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);

        List<SearchModel.Response.DocumentStats> sortedDocuments = aggregateResults(results, searchTerms);
        searchResponse.addAllRelevantDocuments(sortedDocuments);

        return searchResponse.build();
    }

    private List<SearchModel.Response.DocumentStats> aggregateResults(List<Result> results, List<String> terms){
        Map<String, DocumentData> allDocumentsResults=new HashMap<>();

        for(Result result: results){
            allDocumentsResults.putAll(result.getDocumentToDocumentData());
        }
        System.out.println("Calculating score for all documents");
        Map<Double, List<String>> scoreToDocuments=TFIDF.getDocumentsSortedByScore(terms, allDocumentsResults);

        return sortDocumentsByScore(scoreToDocuments);
    }

    private List<SearchModel.Response.DocumentStats> sortDocumentsByScore(Map<Double, List<String>> scoreToDocuments){
        List<SearchModel.Response.DocumentStats> sortedDocumentsStatsList=new ArrayList<>();
        for(Map.Entry<Double, List<String>> docScorePair: scoreToDocuments.entrySet()){
            double score=docScorePair.getKey();

            for(String document: docScorePair.getValue()){
                File documentPath=new File(document);

                SearchModel.Response.DocumentStats documentStats=SearchModel.Response.DocumentStats.newBuilder()
                        .setScore(score)
                        .setDocumentName(documentPath.getName())
                        .setDocumentSize(documentPath.length())
                        .build();
                sortedDocumentsStatsList.add(documentStats);
            }
        }
        return sortedDocumentsStatsList;
    }


    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private List<Result> sendTasksToWorkers(List<String> workers, List<Task> tasks){
        CompletableFuture<Result>[] futures = new CompletableFuture[workers.size()];
        for(int i=0;i<workers.size();i++){
            String worker=workers.get(i);
            Task task=tasks.get(i);
            byte[] payload= SerializationUtils.serialize(task);
            futures[i]=client.sendTask(worker, payload);
        }
        List<Result> results=new ArrayList<>();
        for(CompletableFuture<Result> future: futures){
            try{
                Result result= future.get();
                results.add(result);
            }catch (InterruptedException| ExecutionException e){
                e.printStackTrace();
            }
        }
        System.out.println(String.format("Received %d/%d results", results.size(), tasks.size()));
        return results;
    }

    private List<Task> createTasks(int numberOfWorkers, List<String> searchTerms){
        List<List<String>> workerDocuments=splitDocumentList(numberOfWorkers,documents);
        List<Task> tasks=new ArrayList<>();

        for(List<String> documentsForWorker: workerDocuments){
            Task task = new Task(searchTerms, documentsForWorker);
            tasks.add(task);
        }
        return tasks;
    }
    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> documents){
        int numberOfDocumentsPerWorker=(documents.size()+numberOfWorkers-1)/numberOfWorkers;
        List<List<String>> workerDocuments= new ArrayList<>();

        for(int i=0;i<numberOfWorkers;i++){
            int firstDocumentIndex = i*numberOfDocumentsPerWorker;
            int lastDocumentIndexExclusive=Math.min(firstDocumentIndex+numberOfDocumentsPerWorker,documents.size());

            if(firstDocumentIndex>=lastDocumentIndexExclusive){
                break;
            }
            List<String> currentWorkerDocuments = new ArrayList<>(documents.subList(firstDocumentIndex,lastDocumentIndexExclusive));
            workerDocuments.add(currentWorkerDocuments);
        }
        return workerDocuments;
    }

    private static List<String> readDocumentList(){
        File documentsDirectory=new File(BOOKS_DIRECTORY);
        return Arrays.asList(documentsDirectory.list())
                .stream()
                .map(documentName->BOOKS_DIRECTORY+"/"+documentName)
                .collect(Collectors.toList());
    }

}
