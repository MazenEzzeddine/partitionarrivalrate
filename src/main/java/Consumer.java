import java.util.ArrayList;

public class Consumer {
    private final Long lagCapacity;

    public Consumer(Long lagCapacity, double arrivalCapacity) {
        this.lagCapacity = lagCapacity;
        this.arrivalCapacity = arrivalCapacity;

        this.remainingLagCapacity = lagCapacity;
        this.remainingArrivalCapacity = arrivalCapacity;
        assignedPartitions = new ArrayList<>();
    }

    private double remainingArrivalCapacity;
    private ArrayList<Partition> assignedPartitions;
    private double arrivalCapacity;

    public Long getRemainingLagCapacity() {
        return remainingLagCapacity;
    }

    public void setRemainingLagCapacity(Long remaininglagcapacity) {
        this.remainingLagCapacity = remaininglagcapacity;
    }
    public ArrayList<Partition> getAssignedPartitions() {
        return assignedPartitions;
    }

    public void setAssignedPartitions(ArrayList<Partition> assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }

    private Long remainingLagCapacity;

    public double getRemainingArrivalCapacity() {
        return remainingArrivalCapacity;
    }

    public void setRemainingArrivalCapacity(double remainingArrivalCapacity) {
        this.remainingArrivalCapacity = remainingArrivalCapacity;
    }

    public void  assignPartition(Partition partition) {
        assignedPartitions.add(partition);
        remainingLagCapacity -= partition.getLag();
        remainingArrivalCapacity -= partition.getArrivalRate();
    }

    @Override
    public String toString() {
        return "Consumer{" +
                "lagCapacity=" + lagCapacity +
                ", remainingArrivalCapacity=" + remainingArrivalCapacity +
                ", assignedPartitions=" + assignedPartitions +
                ", arrivalCapacity=" + arrivalCapacity +
                ", remainingLagCapacity=" + remainingLagCapacity +
                '}';
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = lagCapacity != null ? lagCapacity.hashCode() : 0;
        temp = Double.doubleToLongBits(remainingArrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (assignedPartitions != null ? assignedPartitions.hashCode() : 0);
        temp = Double.doubleToLongBits(arrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (remainingLagCapacity != null ? remainingLagCapacity.hashCode() : 0);
        return result;
    }
}
