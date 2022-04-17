import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AssignmentServer implements Runnable{



    private final int port;
    private final Server server;

    public AssignmentServer(int port) throws IOException {
        this(ServerBuilder.forPort(port), port);
    }

    public  AssignmentServer(ServerBuilder<?> serverBuilder, int port) {
        this.port = port;
        this.server = serverBuilder.addService(new AssignmentService()).build();
    }

    public void start() throws IOException {


        System.out.println("Server Started");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                AssignmentServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }




    @Override
    public void run() {

        try {
            start();
            blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }


    }


    public static class AssignmentService extends AssignmentServiceGrpc.AssignmentServiceImplBase {
        @Override
        public void getAssignment(AssignmentRequest request, StreamObserver<AssignmentResponse> responseObserver) {
            System.out.println(request.getRequest());


            List<PartitionGrpc> partitions = new ArrayList<>();
            PartitionGrpc p1 = PartitionGrpc.newBuilder().setArrivalRate(10).setId(1).setLag(500).build();
            PartitionGrpc p2 = PartitionGrpc.newBuilder().setArrivalRate(5.0).setId(2).setLag(250).build();
            PartitionGrpc p3 = PartitionGrpc.newBuilder().setArrivalRate(6.2).setId(3).setLag(490).build();

            partitions.add(p1);
            partitions.add(p2);
            partitions.add(p3);


            //Consumer c1= Consumer.newBuilder().setId(1).

            ConsumerGrpc c1= ConsumerGrpc.newBuilder().setId(1).addAssignedPartitions(p1).addAssignedPartitions(p2).build();
            ConsumerGrpc c2= ConsumerGrpc.newBuilder().setId(2).addAssignedPartitions(p3).build();
            responseObserver.onNext(AssignmentResponse.newBuilder().addConsumers(c1).addConsumers(c2).build());
            responseObserver.onCompleted();

            System.out.println("Sent Assignment to client");


        }



    }
}