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

public class CompanyMining
{
    static Connection conn;
   
    private static HashMap<String, Integer> companyToInt;
    private static HashMap<Integer, String> intToCompany;
    private static ArrayList<double []> sectorMatrix;
    private static int movingAverageWindow = 10; //in days
    
    
    public static void main(String [] args) throws Exception
    {
        companyToInt = new HashMap<String, Integer>();
        intToCompany = new HashMap<Integer, String>();
    
        String readParamsFile = "readerparams.txt";

        Properties readProps = new Properties();
        readProps.load(new FileInputStream(readParamsFile));

        try{
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = readProps.getProperty("dburl");
            conn = DriverManager.getConnection(dburl, readProps);

            System.out.printf("Database connection %s %s established.%n", dburl, readProps.getProperty("user"));
            doCompanyAnalysis();
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
    private static void doCompanyAnalysis() throws SQLException, Exception
    {
        ArrayList<String> companies = getCompanies();
        
        HashMap<String, ArrayList<TransactionDay>> allTransactions = new HashMap<String, ArrayList<TransactionDay>>();
        
        println("Getting start and end dates.");
        String[] dates = getStartEndDates();
        println("Start Date: " + dates[0] + "... End Date: " + dates[1]);
            
        try
        {
            for(String company: companies)
            {
                println("Reading data for: " + company);
                ArrayList<TransactionDay> companyData = getCompanyData(company, dates[0], dates[1]);
                if(companyData != null)
                    allTransactions.put(company, companyData);
            }
        }
        catch(SQLException e)
        {
            System.out.printf("SQLException: %s%nVendorError: %s%n", e.getMessage(), e.getSQLState(), e.getErrorCode());
        }
        
        
        //align data (discard any dates that dont exist for ALL companies)
        println("Aligning data");
        alignData(allTransactions.values());
        
        //set situation values in transaction days
        println("Categorizing transactions");
        setupSituations(allTransactions.values());
        
        //determine relationships
        println("Creating Itemsets");
        List<LinkedList<TransactionDay>> allItemsets = createItemSets(allTransactions.values());
        
        //mine for frequent closed itemsets
        println("Activating external library to mine for closed itemsets");
        outputItemsets(allItemsets);
        Runtime.getRuntime().exec("java -jar spmf.jar run FPClose data.dat output.dat 10%");
        
        //print analysis of output
        translateOutput("output.dat");
    }
    
    private static void translateOutput(String file)
    throws FileNotFoundException
    {
        Scanner sc = new Scanner(new File(file));

        while(sc.hasNextLine())
        {
            String raw = sc.nextLine();
            String[] rawSplit = raw.split(" #SUP: ");
            String support = rawSplit[1];
            String[] itemset = rawSplit[0].split(" ");
            
            if(itemset.length > 1)
            {
                print("Itemset: ");
                
                int len = itemset.length;
                
                print(intToCompany.get(Integer.valueOf(itemset[0])) + ", ");
                
                for (int i = 1; i < len; i++)
                {
                    print(intToCompany.get(Integer.valueOf(itemset[i])));
                }
                
                println(". Support: " + support);
            }
        }
    }
    
    private static void outputItemsets(List<LinkedList<TransactionDay>> allItemsets)
    throws FileNotFoundException, UnsupportedEncodingException
    {
        PrintWriter writer = new PrintWriter("data.dat", "UTF-8");
        
        for(List<TransactionDay> itemset : allItemsets)
        {
            String line = "";
            
            for (TransactionDay item : itemset)
            {
                String name = item.name;
                
                Integer companyInt = companyToInt.get(name);
                
                line += (companyToInt.get(item.name) + " ");
            }
            
            line = line.substring(0, line.length() - 1);
            
            writer.println(line);
        }
        
        writer.close();
    }
    
    private static List<LinkedList<TransactionDay>> createItemSets(Collection<ArrayList<TransactionDay>> allTransactions)
    {
        ArrayList<Iterator<TransactionDay>> companyIterators = new ArrayList<Iterator<TransactionDay>>();
    
        for (ArrayList<TransactionDay> company : allTransactions)
        {
            if(company.iterator().hasNext())
                companyIterators.add(company.iterator());
        }
        
        boolean daysLeft = true;
        
        LinkedList<LinkedList<TransactionDay>> allItemsets = new LinkedList<LinkedList<TransactionDay>>();
        
        while(daysLeft)
        {
            LinkedList<TransactionDay> increasing = new LinkedList<TransactionDay>();
            LinkedList<TransactionDay> decreasing = new LinkedList<TransactionDay>();
            
            for(Iterator<TransactionDay> iter : companyIterators)
            {
                TransactionDay day = iter.next();
                if(day.situation == TransactionDay.Situation.INCREASING)
                {
                    increasing.add(day);
                }
                else if (day.situation == TransactionDay.Situation.DECREASING)
                {
                    decreasing.add(day);
                }
                
                if(!iter.hasNext())
                    daysLeft = false;
            }
            
            if(!increasing.isEmpty())
            {
                allItemsets.add(increasing);
            }
            
            if(!decreasing.isEmpty())
            {
                allItemsets.add(decreasing);
            }
        }
        
        return allItemsets;
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
    
    private static void alignData(Collection<ArrayList<TransactionDay>> companies)
    {
        ArrayList<HashSet<String>> dateSetList = new ArrayList<HashSet<String>>();
        
        //set up the date set, to enable quick lookups of the existence of a date
        for (ArrayList<TransactionDay> transactionList : companies)
        {
            HashSet<String> dateSet = new HashSet<String>();
            for(TransactionDay day : transactionList)
            {
                dateSet.add(day.date);
            }
            dateSetList.add(dateSet);
        }
        
        //begin aligning the data
        for (ArrayList<TransactionDay> company : companies)
        {
            Iterator<TransactionDay> iter = company.iterator();
            while(iter.hasNext())
            {
                TransactionDay day = iter.next();
                
                boolean existsInAll = true;
                for(HashSet<String> dateSet : dateSetList)
                {
                    if (!dateSet.contains(day.date))
                    {
                        existsInAll = false;
                        break;
                    }
                }
                
                if(!existsInAll)
                    iter.remove();
            }
        }
    }

   
    private static ArrayList<String> getCompanies() throws SQLException
    {
        //String array to return
        ArrayList<String> companies = new ArrayList<String>();
        // Format and execute the query
        PreparedStatement pstmt = conn.prepareStatement("select Ticker from Company;");
        ResultSet result = pstmt.executeQuery();
        // Check for query result
        if (result.next())
        {
            // Get first result info
            String currIndustry = result.getString("Ticker");
            companies.add(currIndustry);
            
            companyToInt.put(currIndustry, 0);
            intToCompany.put(0, currIndustry);

            int i = 1;
            // Iterate through remaining results
            while (result.next())
            {
                currIndustry = result.getString("Ticker");
                
                companyToInt.put(currIndustry, i);
                intToCompany.put(i, currIndustry);
                
                companies.add(currIndustry);
                i++;
            }
        }
        else
        {
            System.out.printf("Error finding companies.%n");
        }
            
        pstmt.close();

        return companies;
    }//end getIndustries    
    
    
    private static String[] getStartEndDates()
    throws SQLException
    {
        // Placeholder until actual dates calculated, to return
        String[] dates = new String[]{"start", "end"};
        // Find start/end for each company first
        ArrayList<String[]> allDates = new ArrayList<String[]>();
        
        PreparedStatement minDateStatement = conn.prepareStatement("select TransDate from PriceVolume order by TransDate asc limit 1;");
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
    
    
    
        PreparedStatement maxDateStatement = conn.prepareStatement("select TransDate from PriceVolume order by TransDate desc limit 1;");
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

    
    private static ArrayList<TransactionDay> getCompanyData(String company, String startDate, String endDate)
    throws SQLException
    {
        // To return
        ArrayList<TransactionDay> results = new ArrayList<TransactionDay>();
        
        ArrayList<Double> movingAvgList = new ArrayList<Double>();
        
        PreparedStatement pstmtCompanyData;

        pstmtCompanyData = conn.prepareStatement(
                " select P.TransDate, P.ClosePrice, P.Volume"
                +" from PriceVolume P natural join Company "
                +" where Ticker =  ? and TransDate between ? and ? order by TransDate DESC");
        //[date, openPrice, closePrice, highPrice, lowPrice, volume]
        pstmtCompanyData.setString(1, company);
                    pstmtCompanyData.setString(2, startDate);
                    pstmtCompanyData.setString(3, endDate);                                                            
        ResultSet querySet = pstmtCompanyData.executeQuery();
        
        // Set up first day for do loop
        if (querySet.next())
        {
        
            TransactionDay day = new TransactionDay();
            day.name = company;
            day.date = querySet.getString("P.TransDate");

            do
            {                           
                String date = querySet.getString("P.TransDate");
                double closingPrice = querySet.getDouble("P.ClosePrice");
                double volume = querySet.getDouble("P.Volume");

                day.capital = closingPrice * volume;

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
                day.name = company;
                day.date = date;
                
            } while(querySet.next());
            
        }
        else
        {
            println("Error finding transaction data for " + company);
        }
        
        pstmtCompanyData.close();
        
        //truncate results to remove days that dont have a moving average value
        
        
        if(results.size() > movingAverageWindow)
            results = new ArrayList<TransactionDay>(results.subList(movingAverageWindow - 1, results.size()));
            
        if(results.size() == 0)
            return null;
        
        return results;
    }//end getIndustryData    
    
    
}//end class
 
