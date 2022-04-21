
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hps.RateRequest;
import org.hps.RateResponse;
import org.hps.RateServiceGrpc;


import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Controller implements Runnable {


    private static final Logger log = LogManager.getLogger(Controller.class);

    public static String CONSUMER_GROUP;
    public static int numberOfPartitions;
    public static AdminClient admin = null;


    static Long sleep;
    static double doublesleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;



    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    ///////////////////////////////////////////////////////////////////////////


    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static boolean firstIteration = true;

    static TopicDescription td;
    static DescribeTopicsResult tdr;
    static ArrayList<Partition> partitions = new ArrayList<>();


    static double currenttotalArrivalRate = 0.0;
    static double previoustotalArrivalRate = 0.0;
    static double dynamicTotalMaxConsumptionRate =0.0;
    static double dynamicAverageMaxConsumptionRate = 0.0;


    private static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        tdr = admin.describeTopics(Collections.singletonList(topic));
        td = tdr.values().get(topic).get();


        for (TopicPartitionInfo p : td.partitions()) {
            partitions.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic has the following partitions {}", td.partitions().size());
    }


    private static void queryConsumerGroup() throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList(Controller.CONSUMER_GROUP));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();

        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

        dynamicTotalMaxConsumptionRate =0.0;
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members()) {
            log.info("Calling the consumer {} for its consumption rate ", memberDescription.host());
           float rate = callForConsumptionRate(memberDescription.host());
            dynamicTotalMaxConsumptionRate += rate;
        }
    }


    private static float callForConsumptionRate(String host) {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(host.substring(1), 5002)
                .usePlaintext()
                .build();
        RateServiceGrpc.RateServiceBlockingStub rateServiceBlockingStub
                = RateServiceGrpc.newBlockingStub(managedChannel);
        RateRequest rateRequest = RateRequest.newBuilder().setRate("Give me your rate")
                .build();
        log.info("connected to server {}", host);
        RateResponse rateResponse = rateServiceBlockingStub.consumptionRate(rateRequest);
        log.info("Received response on the rate: " + rateResponse.getRate());
        managedChannel.shutdown();
        return rateResponse.getRate();
    }


    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for (TopicPartitionInfo p : td.partitions()) {
            requestLatestOffsets.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();

        for (TopicPartitionInfo p : td.partitions()) {
            TopicPartition t = new TopicPartition(topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long committedoffset = committedOffsets.get(t).offset();

            partitions.get(p.partition()).setPreviousLastOffset(partitions.get(p.partition()).getCurrentLastOffset());
            partitions.get(p.partition()).setCurrentLastOffset(latestOffset);
            partitions.get(p.partition()).setLag(latestOffset - committedoffset);
        }
        if (!firstIteration) {
            computeTotalArrivalRate();
        } else {
            firstIteration = false;
        }
    }


    private static void computeTotalArrivalRate() throws ExecutionException, InterruptedException {

        double totalArrivalRate = 0;
        long totallag = 0;

        for (Partition p : partitions) {
            log.info(p.toString());
            totalArrivalRate += (p.getCurrentLastOffset() - p.getPreviousLastOffset()) / doublesleep;
            totallag += p.getLag();
        }

        log.info("current totalArrivalRate from this iteration/sampling {}", totalArrivalRate);
        log.info("totallag {}", totallag);

        /////////////check sampling issues//////////////
        log.info("current total arrival rate from previous sampling {}", currenttotalArrivalRate);
        log.info("previous total arrival rate from previous sampling {}", previoustotalArrivalRate);
        log.info("Math.abs((totalArrivalRate - currenttotalArrivalRate)) / doublesleep {}",
                (((totalArrivalRate - currenttotalArrivalRate)) / doublesleep));


        if (Math.abs((totalArrivalRate - currenttotalArrivalRate)) / doublesleep > 20.0) {
            log.info("Looks like sampling boundary issue");
            log.info("ignoring this sample");
        } else {
            previoustotalArrivalRate = currenttotalArrivalRate;
            currenttotalArrivalRate = totalArrivalRate;
            for (Partition p : partitions) {
                p.setPreviousArrivalRate(p.getArrivalRate());
                p.setArrivalRate((double) (p.getCurrentLastOffset() - p.getPreviousLastOffset()) / doublesleep);
            }
        }

        //queryConsumerGroup();
        //youMightWanttoScaleDynamically(totalArrivalRate);

         //youMightWanttoScale();
    }


    private static void youMightWanttoScaleUsingBinPack(){

        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();

        dynamicAverageMaxConsumptionRate = dynamicTotalMaxConsumptionRate / (double)(size);
        BinPackScaler bpscaler =new BinPackScaler(dynamicTotalMaxConsumptionRate,dynamicAverageMaxConsumptionRate,
                partitions, size);

        bpscaler.scaleAsPerBinPack();

    }





    private static void youMightWanttoScaleDynamically (double totalArrivalRate) throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        log.info("current group size is {}", size);

        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 15 ) {
            log.info("Upscale logic, Up scale cool down has ended");

            if (upScaleLogicDynamic(totalArrivalRate, size)) {
                return;
            }
        } else {
            log.info("Not checking  upscale logic, Up scale cool down has not ended yet");
        }


        if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 15 ) {
            log.info("DownScaling logic, Down scale cool down has ended");
            downScaleLogicDynamic(totalArrivalRate, size);
        }else {
            log.info("Not checking  down scale logic, down scale cool down has not ended yet");
        }
    }



    private static boolean upScaleLogicDynamic(double totalArrivalRate, int size) {

        dynamicAverageMaxConsumptionRate = dynamicTotalMaxConsumptionRate / (double)(size);
        log.info("dynamicAverageMaxConsumptionRate {}", dynamicAverageMaxConsumptionRate);
        if (totalArrivalRate > dynamicAverageMaxConsumptionRate) {
            // log.info("Consumers are less than nb partition we can scale");

            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);

                log.info("Since  arrival rate {} is greater than  maximum consumption rate " +
                        "{} ,  I up scaled  by one ", totalArrivalRate , dynamicAverageMaxConsumptionRate);

                lastDownScaleDecision = Instant.now();
                lastUpScaleDecision = Instant.now();
                return true;
            }
        }
        return false;
    }




    private static void downScaleLogicDynamic(double totalArrivalRate, int size) {

        if(size == 1) return;
        dynamicAverageMaxConsumptionRate = dynamicTotalMaxConsumptionRate / (double)(size);

        log.info("dynamicAverageMaxConsumptionRate {}", dynamicAverageMaxConsumptionRate);
        if (totalArrivalRate  < dynamicAverageMaxConsumptionRate) {

            log.info("since  arrival rate {} is lower than maximum consumption rate " +
                            " with size - 1  I down scaled  by one {}",
                    totalArrivalRate , dynamicAverageMaxConsumptionRate);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    lastDownScaleDecision = Instant.now();
                    lastUpScaleDecision = Instant.now();
                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
    }


    private static void youMightWanttoScale() throws ExecutionException, InterruptedException {
        log.info("Inside you youMightWanttoScale");


        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 15 &&
                Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 15) {
            queryConsumerGroup();

        }


        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 15 ) {
            log.info("Upscale logic, Up scale cool down has ended");


            upScaleLogic();
            //why not returning
        } else {
            log.info("Not checking  upscale logic, Up scale cool down has not ended yet");
        }


        if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 15 ) {
            log.info("DownScaling logic, Down scale cool down has ended");
            downScaleLogic();
        }else {
            log.info("Not checking  down scale logic, down scale cool down has not ended yet");
        }
    }


    private static void upScaleLogic() throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        log.info("curent group size is {}", size);

        if (currenttotalArrivalRate  > size *poll) {
            log.info("Consumers are less than nb partition we can scale");

            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);


                log.info("Since  arrival rate {} is greater than  maximum consumption rate " +
                        "{} ,  I up scaled  by one ", currenttotalArrivalRate , size * poll);
            }

            lastUpScaleDecision = Instant.now();
            lastDownScaleDecision = Instant.now();
        }
    }




    private static void downScaleLogic() throws ExecutionException, InterruptedException {
        int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        if ((currenttotalArrivalRate) < (size - 1) * poll) {

            log.info("since  arrival rate {} is lower than maximum consumption rate " +
                            " with size - 1  I down scaled  by one {}",
                    currenttotalArrivalRate , size * poll);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {

                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    lastDownScaleDecision = Instant.now();
                    lastUpScaleDecision = Instant.now();

                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            readEnvAndCrateAdminClient();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();

        doublesleep = (double) sleep / 1000.0;


        while (true) {
            log.info("New Iteration:");
            try {
                getCommittedLatestOffsetsAndLag();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Sleeping for {} seconds", sleep / 1000.0);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            log.info("End Iteration;");
            log.info("=============================================");
        }
    }
}




