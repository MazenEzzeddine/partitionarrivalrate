import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AssignmentServer implements Runnable{

    private final int port;
    private final Server server;

    private static final Logger log = LogManager.getLogger(AssignmentServer.class);


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

            List<Consumer> assignment = Controller.assignment;

            log.info("The assignment is {}", assignment);



            List<ConsumerGrpc> assignmentReply = new ArrayList<>(assignment.size());

            for (Consumer cons : assignment) {
                List<PartitionGrpc> pgrpclist = new ArrayList<>();
                for (Partition p : cons.getAssignedPartitions()) {
                    log.info("partition {} is assigned to consumer {}", p.getId(), cons.getId());
                   PartitionGrpc pgrpc =  PartitionGrpc.newBuilder().setId(p.getId()).build();
                    pgrpclist.add(pgrpc);
                }

                ConsumerGrpc consg  =  ConsumerGrpc.newBuilder().setId(cons.getId()).addAllAssignedPartitions(pgrpclist).build();

                assignmentReply.add(consg);
            }

            for(ConsumerGrpc cons : assignmentReply){
                log.info("Consumer {} has the following partitions", cons.getId());
                for(PartitionGrpc part : cons.getAssignedPartitionsList()){
                    log.info("partition {}", part.getId());
                }

            }

            responseObserver.onNext(AssignmentResponse.newBuilder().addAllConsumers(assignmentReply).build());
            responseObserver.onCompleted();
            System.out.println("Sent Assignment to client");

         /*   List<PartitionGrpc> partitions = new ArrayList<>();
            PartitionGrpc p1 = PartitionGrpc.newBuilder().setArrivalRate(10).setId(0).setLag(500).build();
            PartitionGrpc p2 = PartitionGrpc.newBuilder().setArrivalRate(5.0).setId(1).setLag(250).build();
            PartitionGrpc p3 = PartitionGrpc.newBuilder().setArrivalRate(6.2).setId(2).setLag(490).build();
            PartitionGrpc p4 = PartitionGrpc.newBuilder().setArrivalRate(6.8).setId(3).setLag(240).build();
            PartitionGrpc p5 = PartitionGrpc.newBuilder().setArrivalRate(7.1).setId(4).setLag(243).build();

            partitions.add(p1);
            partitions.add(p2);
            partitions.add(p3);
            partitions.add(p3);
            partitions.add(p4);
            partitions.add(p5);




            //Consumer c1= Consumer.newBuilder().setId(1).
            ConsumerGrpc c1= ConsumerGrpc.newBuilder().setId(1).addAssignedPartitions(p1).addAssignedPartitions(p3).
                    addAssignedPartitions(p4).build();
            ConsumerGrpc c2= ConsumerGrpc.newBuilder().setId(2).addAssignedPartitions(p5).
            addAssignedPartitions(p2).build();
            responseObserver.onNext(AssignmentResponse.newBuilder().addConsumers(c1).addConsumers(c2).build());
            responseObserver.onCompleted();*/
            //System.out.println("Sent Assignment to client");
        }
    }

    public static class AssignmentServiceDynamic extends AssignmentServiceGrpc.AssignmentServiceImplBase {
        @Override
        public void getAssignment(AssignmentRequest request, StreamObserver<AssignmentResponse> responseObserver) {

            List<Partition> partitions1 = new ArrayList<Partition>();
            partitions1.add(new Partition(0,100, 20.0));
            partitions1.add(new Partition(1,100, 50.0));

            List<Partition> partitions2 = new ArrayList<>();
            partitions2.add(new Partition(2,50, 10.0));
            partitions2.add(new Partition(3,50, 500.0));
            partitions2.add(new Partition(4,300, 30));



            Consumer c1 = new Consumer(0,500L, 100);
                    c1.setAssignedPartitions(partitions1);

            Consumer c2 = new Consumer(1, 500L, 100);
            c2.setAssignedPartitions(partitions1);


            BinPackScaler.assignment.add(c1);
            BinPackScaler.assignment.add(c2);


            List<Consumer> assignment = BinPackScaler.assignment;
            List<ConsumerGrpc> assignmentReply = new ArrayList<>(assignment.size());

            for (Consumer cons : assignment) {
                ConsumerGrpc consg  =  ConsumerGrpc.newBuilder().build();
                for (Partition p : cons.getAssignedPartitions()) {
                    consg.toBuilder().addAssignedPartitions(
                            PartitionGrpc.newBuilder().setArrivalRate(p.getArrivalRate()).setId(p.getId()).setLag(p.getLag()).build());
                }
            }

            responseObserver.onNext(AssignmentResponse.newBuilder().addAllConsumers(assignmentReply).build());
            responseObserver.onCompleted();
            System.out.println("Sent Assignment to client");
        }
    }
}