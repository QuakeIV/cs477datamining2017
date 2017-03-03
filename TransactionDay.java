public class TransactionDay
{
    public enum Situation { INCREASING, STABLE, DECREASING }

    public String industry;
    public String date;
    public double capital;
    public double movingAverage;
    public Situation situation;
    
    
    public String toString()
    {
        return industry + " " + date + " $" + capital + "\n";
    }
} 
