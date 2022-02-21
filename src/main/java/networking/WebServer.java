package networking;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String TASK_ENDPOINT="/task";
    private static final String STATUS_ENDPOINT="/status";

    private final int port;
    private HttpServer server;
    private final OnRequestCallback onRequestCallback;

    public WebServer(int port, OnRequestCallback onRequestCallback){
        this.port=port;
        this.onRequestCallback = onRequestCallback;
    }

    public void startServer(){
        try{
            this.server=HttpServer.create(new InetSocketAddress(port), 0);
        }catch (Exception e){
            e.printStackTrace();
            return;
        }
        HttpContext statusContext = server.createContext(STATUS_ENDPOINT);
        HttpContext taskContext = server.createContext(onRequestCallback.getEndpoint());

        statusContext.setHandler(this::handleStatusCheckRequest);
        taskContext.setHandler(this::handleTaskCheckRequest);

        server.setExecutor(Executors.newFixedThreadPool(0));
        server.start();
    }

    public void stop(){
        server.stop(0);
    }
    private void handleTaskCheckRequest(HttpExchange exchange) throws IOException{
        if(!exchange.getRequestMethod().equalsIgnoreCase("post")){
            exchange.close();
            return;
        }
        Headers headers = exchange.getRequestHeaders();
        if (headers.containsKey("X-test") && headers.get("X-test").get(0).equalsIgnoreCase("true")) {
            String dummyresponse="123\n";
            return;
        }
        boolean isDebug=false;
        if (headers.containsKey("X-debug") && headers.get("X-debug").get(0).equalsIgnoreCase("true")) {
            isDebug=true;
            return;
        }
        long startTime=System.nanoTime();
        byte[] requestbytes=exchange.getRequestBody().readAllBytes();
        byte[] responseBytes = onRequestCallback.handleRequest(exchange.getRequestBody().readAllBytes());//calculateResponse(requestbytes);
        long endTime = System.nanoTime();
        if(isDebug){
            String debugMessage=String.format("Operation took %d ns\n",endTime-startTime);
            exchange.getResponseHeaders().put("X-Debug-Info", Arrays.asList(debugMessage));
        }
        sendResponse(responseBytes, exchange);
    }

    private byte[] calculateResponse(byte[] requestBytes){
        String bodyString = new String(requestBytes);
        String[] stringNumbers = bodyString.split(",");
        BigInteger result = BigInteger.ONE;
        for(String number: stringNumbers){
            BigInteger bigInteger=new BigInteger(number);
            result = result.multiply(bigInteger);
        }
        return String.format("result of the operation is %s\n", result).getBytes();
    }

    private void handleStatusCheckRequest(HttpExchange exchange) throws IOException{
        if(!exchange.getRequestMethod().equalsIgnoreCase("get")){
            exchange.close();
            return;
        }
        String responseMessage = "Server is alive";
        sendResponse(responseMessage.getBytes(), exchange);
    }
    private void sendResponse(byte[] responseBytes, HttpExchange exchange) throws IOException{
        exchange.sendResponseHeaders(200,responseBytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(responseBytes);
        os.flush();
        os.close();
    }
}
