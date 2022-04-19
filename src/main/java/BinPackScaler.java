
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPackScaler {

    private double dynamicTotalMaxConsumptionRate;
    private double dynamicAverageMaxConsumptionRate;

    private final List<Partition> partitions;
    private final int currentCGsize;

    Instant lastScaleUpDecision;
    Instant lastScaleDownDecision;


    public BinPackScaler(double dynamicTotalMaxConsumptionRate, double dynamicAverageMaxConsumptionRate, List<Partition> partitions, int currentCGsize) {
        this.dynamicTotalMaxConsumptionRate = dynamicTotalMaxConsumptionRate;
        this.dynamicAverageMaxConsumptionRate = dynamicAverageMaxConsumptionRate;
        this.partitions = partitions;
        this.currentCGsize = currentCGsize;
    }

    private static final Logger log = LogManager.getLogger(Controller.class);


    private List<Consumer> binPackAndScale() {

        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 0;

         long maxLagCapacity;

        maxLagCapacity = (long) (dynamicAverageMaxConsumptionRate * 5.0);


        consumers.add(new Consumer(maxLagCapacity, dynamicAverageMaxConsumptionRate));

        //if a certain partition has a lag higher than R Wmax set its lag to R*Wmax
        for (Partition partition : partitions) {
            log.info("partition {} has the following lag {}", partition.getId(), partition.getLag());
            if (partition.getLag() > maxLagCapacity ) {
                log.info("Since partition {} has lag {} higher than consumer capacity {}" +
                        " we are truncating its lag", partition.getId(), partition.getLag(), maxLagCapacity);
                partition.setLag(maxLagCapacity);
            }
        }

        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        for (Partition partition : partitions) {
            log.info("partition {} has the following lag {}", partition.getId(), partition.getLag());
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate ) {
                log.info("Since partition {} has lag {} higher than consumer capacity {}" +
                        " we are truncating its lag", partition.getId(), partition.getArrivalRate(), dynamicAverageMaxConsumptionRate);
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate);
            }
        }





        //start the bin pack FFD with sort
        Collections.sort(partitions, Collections.reverseOrder());




        Consumer consumer = null;
        for (Partition partition : partitions) {
            for (Consumer cons : consumers) {
                if (cons.getRemainingLagCapacity() >= partition.getLag() &&
                        cons.getRemainingArrivalCapacity()> partition.getArrivalRate()) {
                    cons.assignPartition(partition);
                    // we are done with this partition, go to next
                    break;
                }
                //we have iterated over all the consumers hoping to fit that partition, but nope
                //we shall create a new consumer i.e., scale up
                if (cons == consumers.get(consumers.size() - 1)) {
                    consumerCount++;
                    consumer = new Consumer((long) (dynamicAverageMaxConsumptionRate * 5.0), dynamicAverageMaxConsumptionRate);
                    consumer.assignPartition(partition);
                }
            }
            if (consumer != null) {
                consumers.add(consumer);
                consumer = null;
            }
        }

        return consumers;

    }


    public void scaleAsPerBinPack() {
        //same number of consumers but different different assignment
        int currentsize = currentCGsize;
        log.info("Currently we have this number of consumers {}", currentsize);
        int neededsize = binPackAndScale().size();
        log.info("We currently need the following consumers (as per the bin pack) {}", neededsize);


        int replicasForscale = neededsize - currentsize;
        // but is the assignmenet the same
        if (replicasForscale == 0) {
            log.info("No need to autoscale");
            /*if(!doesTheCurrentAssigmentViolateTheSLA()) {
                //with the same number of consumers if the current assignment does not violate the SLA
                return;
            } else {
                log.info("We have to enforce rebalance");
                //TODO skipping it for now. (enforce rebalance)
            }*/
        } else if (replicasForscale > 0) {
            //checking for scale up coooldown
            if (Duration.between(lastScaleUpDecision, Instant.now()).toSeconds() < 30) {
                log.info("Scale up cooldown period has not elapsed yet not taking decisions");
                return;
            } else {
                log.info("We have to upscale by {}", replicasForscale);
                log.info("Upscaling");
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                    log.info("I have upscaled you should have {}", neededsize);
                }
            }
            lastScaleUpDecision = Instant.now();
            lastScaleDownDecision = Instant.now();

        } else {

            if (Duration.between(lastScaleDownDecision, Instant.now()).toSeconds() < 60) {
                log.info("Scale down cooldown period has not elapsed yet not taking scale down decisions");
                return;
            } else {

                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(neededsize);
                    log.info("I have upscaled you should have {}", neededsize);
                }

            }
        }
    }

}










