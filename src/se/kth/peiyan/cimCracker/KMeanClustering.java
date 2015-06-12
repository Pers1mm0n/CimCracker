package se.kth.peiyan.cimCracker;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

/**
 * A class storing some static method for K mean clustering
 * 
 * @author peiyanli
 * @version 0.1, June 12, 2015
 */
public class KMeanClustering
{
    private static final int REPETITION = 50;
    
    /**
     * read data from 'analog_meas' table and restructure them into data object with selected attributes
     * 
     * @param state database statement
     * @param selectedAttributes selected attributes for developing data objects
     * @param tableName it should be 'analog_meas' or any table similar to 'analog_meas'
     * @return a list of data object - an Java inner data structure - for analyzing
     */
    public static ArrayList<DataObject> prepareDataObject(
            Statement state,
            ArrayList<String> selectedAttributes,
            String tableName)
    {
        ArrayList<DataObject> dataObjects = new ArrayList<>();
        try{
            // create table in database
            StringBuilder dataObjectTableState = new StringBuilder("CREATE TABLE dataObjects");
            dataObjectTableState.append(" (time DOUBLE KEY,");
            selectedAttributes.stream().forEach((String attribute) ->
            {
                dataObjectTableState.append(attribute).append("_VALUE DOUBLE,");
            });
            dataObjectTableState.deleteCharAt(dataObjectTableState.length() - 1);
            dataObjectTableState.append(");");
            state.execute(dataObjectTableState.toString());
                
            // populate entities into dataObject tables
            // get time list
            ResultSet resultSet = state.executeQuery("SELECT time FROM " + tableName + " GROUP BY time;");
            ArrayList<String> timeList = new ArrayList<>();
            while (resultSet.next())
            {
                timeList.add(resultSet.getString(1));
            }
            // prepare values for each time instance
            for (String time : timeList)
            {
                StringBuilder rowInsertState = new StringBuilder("INSERT dataObjects");
                rowInsertState.append(" VALUES (");
                rowInsertState.append(time).append(",");
                for (String attribute : selectedAttributes)
                {
                    ResultSet resultSet1 = state.executeQuery("SELECT value FROM " + tableName + " WHERE time=" + time + " AND name='" + attribute + "';");
                    resultSet1.next();
                    String value = resultSet1.getString(1);
                    rowInsertState.append(value).append(",");
                }
                rowInsertState.deleteCharAt(rowInsertState.length() - 1);
                rowInsertState.append(");");
                //System.out.println(rowInsertState.toString());
                state.executeUpdate(rowInsertState.toString());
            }
            
            ResultSet resultSet2 = state.executeQuery("SELECT * FROM dataObjects;");
            while (resultSet2.next())
            {
                int identifer = resultSet2.getInt(1);
                String id = resultSet2.getString(1);
                
                ArrayList<Double> position = new ArrayList<>();
                int numColumns = resultSet2.getMetaData().getColumnCount();
                for (int i = 2; i <= numColumns; i++)
                {
                    position.add(resultSet2.getDouble(i));
                }
                dataObjects.add(new DataObject(identifer, position, id));
            }
        } catch (SQLException ex)
        {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return dataObjects;
    }
    
    /**
     * clustering the data
     * 
     * @param state database statement
     * @param dataObjects a list of data objects - an Java inner data structure - for analyzing
     * @param kNumber K
     * @param isRandomInitializationNeeded if the 'random initialization option' is selected
     * @return 
     */
    public static ArrayList<Centroid> clustering(
            Statement state,
            ArrayList<DataObject> dataObjects,
            int kNumber,
            boolean isRandomInitializationNeeded)
    {
        ArrayList<Centroid> returnedList = new ArrayList<>();
        // clustering data internally
        if (isRandomInitializationNeeded)
        {
            ArrayList<ArrayList<Centroid>> centroidSelections = new ArrayList<>(REPETITION);
            double leastMSE = Double.MAX_VALUE;
            int leastIndex = 0;
            for (int i = 0; i < REPETITION; i++)
            {
                centroidSelections.add(simpleClustering(dataObjects, kNumber, isRandomInitializationNeeded));
                double meanSquareError = dataObjects.stream().mapToDouble((DataObject data) -> 
                {
                    double distance = data.calculateDistance(data.getCentroid());
                    return distance * distance;
                }).sum() / dataObjects.size();
                
                if (meanSquareError < leastMSE)
                {
                    leastMSE = meanSquareError;
                    leastIndex = i;
                }
            }
            returnedList = centroidSelections.get(leastIndex);
            for (DataObject object : dataObjects)
            {
                object.centroidAssignment(returnedList);
            }
        } else
            returnedList = simpleClustering(dataObjects, kNumber, isRandomInitializationNeeded);
        
        // export clusters into database
        try {
            // create table for each cluster
            for (int i = 0; i < kNumber; i++)
            {
                StringBuilder clusterTableState = new StringBuilder("CREATE TABLE cluster_" + i);
                clusterTableState.append(" LIKE dataObjects;");
                state.execute(clusterTableState.toString());
            }
            
            // populate entities into dataObject tables
            for (DataObject dataObject : dataObjects)
            {
                int identifier = dataObject.getCentroid().getIdentifier();
                String id = dataObject.getId();
                double[] attributes = dataObject.getPosition();
                StringBuilder rowInsertState = new StringBuilder("INSERT cluster_" + identifier);
                rowInsertState.append(" VALUES (").append(id).append(",");
                for (int i = 0; i < attributes.length; i++)
                {
                    rowInsertState.append(attributes[i]).append(",");
                }
                rowInsertState.deleteCharAt(rowInsertState.length() - 1);
                rowInsertState.append(");");
                state.executeUpdate(rowInsertState.toString());
            }
        } catch (SQLException ex)
        {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        
        return  returnedList;
    }
    
    private static ArrayList<Centroid> simpleClustering(
            ArrayList<DataObject> dataObjects,
            int kNumber,
            boolean isRandomInitializationNeeded)
    {
        ArrayList<Centroid> centroidList = new ArrayList<>();
        
        // random intialization
        Random rand = new Random();
        ArrayList<Integer> indices = new ArrayList<>(kNumber);
        
        for (int i = 0; i < kNumber; i++)
        {
            int index = rand.nextInt(dataObjects.size());
            if (indices.contains(index))
            {
                i--;
                continue;
            }
            indices.add(index);
            DataObject pseudoCentroid = dataObjects.get(index);
            Centroid centroid = new Centroid(i, new ArrayList<>());
            centroid.setPosition(pseudoCentroid.getPosition());
            centroidList.add(centroid);
        }
        
        boolean flag = true;
        while (flag)
        {
            for (DataObject object : dataObjects)
            {
                object.centroidAssignment(centroidList);
            }
            
            flag = false;
            for (Centroid centroid : centroidList)
            {
                flag |= (centroid.moveCentroid(dataObjects));
            }
        }
        return centroidList;
    }
}
