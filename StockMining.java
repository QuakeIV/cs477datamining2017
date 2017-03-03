/*
 * StockMining()
 *
 * Matthew Adler, Isaias Mondar, Ethan Voon
 *
 * CSCI 477 Data Mining - Winter 2017
 *
 */

import java.sql.*;
import java.util.*;
import java.io.*;

public class StockMining
{
    static Connection conn;
   
    private static HashMap<String, Integer> industries;
    private static ArrayList<double []> sectorMatrix;
    private static int movingAverageWindow = 10; //in days
    
    
    public static void main(String [] args) throws Exception
    {
        String readParamsFile = "readerparams.txt";

        Properties readProps = new Properties();
        readProps.load(new FileInputStream(readParamsFile));

        try{
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = readProps.getProperty("dburl");
            conn = DriverManager.getConnection(dburl, readProps);

            System.out.printf("Database connection %s %s established.%n", dburl, readProps.getProperty("user"));
            doAnalysis();
            conn.close();
        }
        catch(SQLException e)
        {
            System.out.printf("SQLException: %s%nVendorError: %s%n", e.getMessage(), e.getSQLState(),	e.getErrorCode());
        }
    }//end main
    
    // Print shortcuts
    private static void println(String s){System.out.println(s);}
    private static void print(String s){System.out.print(s);}
      
    // Driving function
    private static void doAnalysis() throws SQLException, Exception
    {
            ArrayList<String> industries = getIndustries();
            
            HashMap<String, ArrayList<TransactionDay>> allTransactions = new HashMap<String, ArrayList<TransactionDay>>();
            
            String[] dates = getStartEndDates(industries);
            println("Start Date: " + dates[0] + "... End Date: " + dates[1]);
             
            try
            {
                for(String industry: industries)
                {
                    println("Reading data for: " + industry);
                    allTransactions.put(industry, getIndustryData(industry, dates[0], dates[1]));
                }
            }
            catch(SQLException e)
            {
                System.out.printf("SQLException: %s%nVendorError: %s%n", e.getMessage(), e.getSQLState(), e.getErrorCode());
            }
            
            
            //align data (discard any dates that dont exist for ALL industries)
            alignData(allTransactions.values());
            
            //set situation values in transaction days
            setupSituations(allTransactions.values());
            
            //determine relationships
            analyzeData(allTransactions);
    }
    
    private static void analyzeData(HashMap<String, ArrayList<TransactionDay>> allTransactions)
    {
    }
    

    private static void setupSituations(Collection<ArrayList<TransactionDay>> industries)
    {                        
        for(ArrayList<TransactionDay> list: industries){
            for(TransactionDay t: list){
                    if(t.capital > 1.25*t.movingAverage){
                        t.situation = TransactionDay.Situation.INCREASING;
                        println(t.toString() + t.situation);
                    }
                    if(t.capital < .75*t.movingAverage){
                        t.situation = TransactionDay.Situation.DECREASING;
                        println(t.toString() + t.situation);
                    }
                    else{
                        t.situation = TransactionDay.Situation.STABLE;
                        println(t.toString() + t.situation);
                    }
            }
        }        
    }
    
    private static void alignData(Collection<ArrayList<TransactionDay>> industries)
    {
        ArrayList<Iterator<TransactionDay>> industryIterators = new ArrayList<Iterator<TransactionDay>>();
    
        //get iterators for all of the lists
        for (ArrayList<TransactionDay> transactionList : industries)
        {
            industryIterators.add(transactionList.iterator());
        }
        
        //begin aligning the data
        
        
        Iterator<TransactionDay> reference = industryIterators.remove(industryIterators.size()-1);
        String currentDate = reference.next().date;
        
        boolean allHaveNext = true;
        
        while(allHaveNext)
        {
            boolean allequal = true;
            for(Iterator<TransactionDay> iter : industryIterators)
            {
                String str = iter.next().date;
                
                while(str.compareTo(currentDate) > 0)
                {
                    reference.remove();
                    currentDate = reference.next().date;
                }
                
                while (str.compareTo(currentDate) < 0)
                {
                    iter.remove();
                    iter.next();
                }
            }
            
            currentDate = reference.next().date;
        }
        
        //at this point any tailing elements should be removed because at least one list ran out of elements, meaning there is no match for said tailing elements
    }
   
   
   
    private static ArrayList<String> getIndustries() throws SQLException
    {
        //String array to return
        ArrayList<String> industries = new ArrayList<String>();
        // Format and execute the query
        PreparedStatement	pstmt	= conn.prepareStatement("select Industry from Company natural join PriceVolume " +
                                                "group by Industry;");
        ResultSet result = pstmt.executeQuery();
        // Check for query result
        if (result.next())
        {
            // Get first result info
            String currIndustry = result.getString("Industry");
            industries.add(currIndustry);

            // Iterate through remaining results
            while (result.next())
            {
                currIndustry = result.getString("Industry");
                industries.add(currIndustry);
            }
        }
        else
        {
            System.out.printf("Error finding industries.%n");
        }
            
        pstmt.close();

        return industries;
    }//end getIndustries    
    
    
    private static String[] getStartEndDates(ArrayList<String> industries)
    throws SQLException
    {
        // Placeholder until actual dates calculated, to return
        String[] dates = new String[]{"start", "end"};
        // Find start/end for each industry first
        ArrayList<String[]> allDates = new ArrayList<String[]>();
        
        for (String industry : industries)
        {
            PreparedStatement minDateStatement = conn.prepareStatement("select Ticker, TransDate from Company natural join PriceVolume where Industry = ? order by TransDate asc limit 1;");
            minDateStatement.setString(1, industry);
            ResultSet result = minDateStatement.executeQuery();
            
            String currStart = "";
            
            // Check for query result
            if (result.next())
            {
                // Set the current start/end to first transaction
                currStart = result.getString("TransDate");
                
            }
            else
            {
                System.out.printf("No data. No analysis.%n");
            }
        
        
        
            PreparedStatement maxDateStatement = conn.prepareStatement("select Ticker, TransDate from Company natural join PriceVolume where Industry = ? order by TransDate desc limit 1;");
            maxDateStatement.setString(1, industry);
            result = maxDateStatement.executeQuery();
            
            String currEnd = "";
            
            // Check for query result
            if (result.next())
            {
                // Set the current start/end to first transaction
                currEnd = result.getString("TransDate");
                
            }
            else
            {
                System.out.printf("No data. No analysis.%n");
            }
            
            maxDateStatement.close();
            
            
            // Add Start/End for this industry to allDates
            allDates.add(new String[]{currStart, currEnd});
            
            minDateStatement.close();
            
        }// end for each industry
        
        if (!allDates.isEmpty())
        {
            String[] currDates = allDates.get(0);
            
            for (String[] date : allDates)
            {
            if ((date[0].compareTo(currDates[0])) > 0)
            {
                currDates[0] = date[0];
            }
            if ((date[1].compareTo(currDates[1])) < 0)
            {
                currDates[1] = date[1];
            }
            }
            
            dates[0] = currDates[0];
            dates[1] = currDates[1];
        }

        return dates;
        
    }// end getStartEndDates\

    
    private static ArrayList<TransactionDay> getIndustryData(String industry, String startDate, String endDate)
    throws SQLException
    {
        // To return
        ArrayList<TransactionDay> results = new ArrayList<TransactionDay>();
        
        ArrayList<Double> movingAvgList = new ArrayList<Double>();
        
        PreparedStatement pstmtIndustryData;

        pstmtIndustryData = conn.prepareStatement(
                " select P.TransDate, P.ClosePrice, P.Volume"
                +" from PriceVolume P natural join Company "
                +" where industry =  ? and TransDate between ? and ? order by TransDate DESC");
        //[date, openPrice, closePrice, highPrice, lowPrice, volume]
        pstmtIndustryData.setString(1, industry);
                    pstmtIndustryData.setString(2, startDate);
                    pstmtIndustryData.setString(3, endDate);                                                            
        ResultSet querySet = pstmtIndustryData.executeQuery();
        
        // Set up first day for do loop
        if (querySet.next())
        {
        
            TransactionDay day = new TransactionDay();
            day.industry = industry;
            day.date = querySet.getString(1);

            do
            {                           
                String date = querySet.getString("P.TransDate");
                double closingPrice = querySet.getDouble("P.ClosePrice");
                double volume = querySet.getDouble("P.Volume");

                day.capital += closingPrice * volume;

                if(!date.equals(day.date))
                {
                    movingAvgList.add(day.capital);
                    
                    
                    //if the moving average list is fully populated, set the moving average for that day
                    //if not set it to -1 because no moving average is possible for that day
                    if(movingAvgList.size() >= movingAverageWindow)
                    {
                        movingAvgList = new ArrayList<Double>(movingAvgList.subList(movingAvgList.size() - movingAverageWindow, movingAvgList.size()));
                        
                        double average = 0.0d;
                        
                        for(Double val : movingAvgList)
                        {
                            average+= val;
                        }
                        
                        average = average/movingAverageWindow;
                        
                        day.movingAverage = average;
                    }
                    else
                    {
                        day.movingAverage = -1.0d;
                    }
                
                
                    results.add(day);
                    
                    day = new TransactionDay();
                    day.industry = industry;
                    day.date = date;
                }
                
            } while(querySet.next());
            
        } else
        {
            print("Error finding transaction data.");
        }
        
        pstmtIndustryData.close();
        
        //truncate results to remove days that dont have a moving average value
        
        results = new ArrayList<TransactionDay>(results.subList(movingAverageWindow - 1, results.size()));
        
        return results;
    }//end getIndustryData    
    
    
}//end class
 
