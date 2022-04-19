import java.util.ArrayList;

public class Consumer {
    private Long capacity;

    public Long getRemainingSize() {
        return remainingSize;
    }

    public void setRemainingSize(Long remainingSize) {
        this.remainingSize = remainingSize;
    }

    public ArrayList<Partition> getAssignedPartitions() {
        return assignedPartitions;
    }

    public void setAssignedPartitions(ArrayList<Partition> assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }

    private Long remainingSize;
    private ArrayList<Partition> assignedPartitions;

    public Consumer(Long capacity) {
        this.capacity = capacity;
        this.remainingSize = capacity;
        assignedPartitions = new ArrayList<>();
    }


    public Long getCapacity() {
        return capacity;
    }


    public void  assignPartition(Partition partition) {
        assignedPartitions.add(partition);
        remainingSize -= partition.getLag();
    }

    public void setCapacity(Long capacity) {
        this.capacity = capacity;
    }

    @Override
    public String toString() {
        return "Consumer{" +
                "capacity=" + capacity +
                ", remainingSize=" + remainingSize +
                ", assignedPartitions=" + assignedPartitions +
                '}' + "\n";
    }
}
