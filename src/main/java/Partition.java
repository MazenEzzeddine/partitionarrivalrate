public class Partition {

    private int id;
    private long lag;
    private double arrivalRate;

    private Long currentLastOffset;
    private Long previousLastOffset;





    //private Long[] offsetWindow = new Long[4] ;
    private double[] arrivalRateWindow = new double[4];
    private Long[] lagWindow = new Long[4];

    public Long getCurrentLastOffset() {
        return currentLastOffset;
    }

    public void setCurrentLastOffset(Long currentLastOffset) {
        this.currentLastOffset = currentLastOffset;
    }

    public Long getPreviousLastOffset() {
        return previousLastOffset;
    }

    public void setPreviousLastOffset(Long previousLastOffset) {
        this.previousLastOffset = previousLastOffset;
    }

    public Partition(int id, long lag, double arrivalRate) {
        this.id = id;
        this.lag = lag;
        this.arrivalRate = arrivalRate;
        this.currentLastOffset =0L;
        this.previousLastOffset =0L;

        for(int i=0; i<4; i++){
            //offsetWindow[i] = 0L;
            arrivalRateWindow[i] = 0.0;
            lagWindow[i]=0L;
        }
    }


    public double getAverageArrivalRate(){
        double averageArrivalRate =0.0;
        for(int i=0; i<4; i++) {
            //offsetWindow[i] = 0L;
            averageArrivalRate += arrivalRateWindow[i];
        }
        return averageArrivalRate/4.0;
    }

    public double getAverageLag(){
        Long averageLag =0L;
        for(int i=0; i<4; i++) {
            //offsetWindow[i] = 0L;
            averageLag += lagWindow[i];
        }
        return (double)averageLag/4.0;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getLag() {
        return lag;
    }

    public void setLag(long lag) {
        this.lag = lag;

        for(int i=2; i>=0; i--){
            lagWindow[i+1] = lagWindow[i];
        }

        lagWindow[0] = lag;
    }

    public double getArrivalRate() {
        return arrivalRate;
    }

    public void setArrivalRate(double arrivalRate) {
        this.arrivalRate = arrivalRate;

        for(int i=2; i>=0; i--){
            arrivalRateWindow[i+1] = arrivalRateWindow[i];
        }

        arrivalRateWindow[0] = arrivalRate;
    }


    @Override
    public String toString() {
        return "Partition{" +
                "id=" + id +
                ", lag=" + lag +
                ", arrivalRate=" + arrivalRate +
                ", averageArrivalRate=" + getAverageArrivalRate() +
                ", averageLag=" + getAverageLag() +
        '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partition partition = (Partition) o;

        if (id != partition.id) return false;
        if (lag != partition.lag) return false;
        return Double.compare(partition.arrivalRate, arrivalRate) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id;
        result = 31 * result + (int) (lag ^ (lag >>> 32));
        temp = Double.doubleToLongBits(arrivalRate);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }


}
