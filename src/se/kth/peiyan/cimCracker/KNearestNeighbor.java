package se.kth.peiyan.cimCracker;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;

/**
 * A class storing some static method for K nearest neighbor classification
 * 
 * @author peiyanli
 * @version 0.1 June 12, 2015
 */
public class KNearestNeighbor
{
    
    /**
     * read data from 'analog_meas' table and restructure them into data object with selected attributes
     * <p>this method is different from the one with the the same name in KMeanClustering class</p>
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
            dataObjectTableState.append(" (time DOUBLE KEY, tag INT,");
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
                rowInsertState.append(time).append(",-1,");
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
                for (int i = 3; i <= numColumns; i++)
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
     * K nearest neighbor classification. Find suitable cluster for
     * each data object and tag the cluster identifier in the database
     * 
     * @param statement database statement
     * @param selectedAttributes selected attributes for developing data objects
     * @param tableName it should be 'analog_meas' or any table similar to 'analog_meas'
     * @param dataObjects a list of remembered data object - an Java inner data structure - for analyzing
     * @param kNumber K
     */
    public static void tagState(
            Statement statement,
            ArrayList<String> selectedAttributes,
            String tableName,
            ArrayList<DataObject> dataObjects,
            int kNumber)
    {
        // get test data object
        ArrayList<DataObject> testObjects = prepareDataObject(statement, selectedAttributes, tableName);
        
        testObjects.stream().forEach((test) ->
        {
            ArrayList<Distance> distances = new ArrayList<>(dataObjects.size());
            
            dataObjects.stream().forEach((data) ->
            {
                double diff = data.calculateDistance(test);
                distances.add(new Distance(data, diff));
            });
            
            Collections.sort(distances);
            
            ArrayList<Integer> clusterIdentifiers = new ArrayList<>();
            
            for (int i = 0; i < kNumber; i++)
            {
                int identifier = distances.get(i).dataObject.getCentroid().getIdentifier();
                clusterIdentifiers.add(identifier);
            }
            
            int largestIdentifier = clusterIdentifiers.stream().mapToInt((identifier) -> {return identifier;}).max().getAsInt();
            
            int[] frequencies = new int[largestIdentifier + 1];
            for (int i = 0; i <= largestIdentifier; i++)
            {
                frequencies[i] = Collections.frequency(clusterIdentifiers, i);
            }
            
            int state = -1;
            {
                int max = Integer.MIN_VALUE;
                for (int i = 0; i < frequencies.length; i++)
                {
                    if (frequencies[i] > max)
                    {
                        max = frequencies[i];
                        state = i;
                    }
                }
            }
            
            // populate into database
            try {
                StringBuilder rowInsertState = new StringBuilder("UPDATE dataObjects SET tag=");
                rowInsertState.append(state).append(" WHERE time=").append(test.getId()).append(";");
                statement.executeUpdate(rowInsertState.toString());
            } catch (SQLException ex)
            {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        });
    }
    
    /**
     * a private class represent the distance from a test data object to remembered data object
     */
    private static class Distance implements Comparable<Distance>
    {
        private DataObject dataObject;
        private double distance;

        /**
         * constructor for Distance instance
         * @param dataObject a list of remembered data object - an Java inner data structure - for analyzing
         * @param distance distance
         */
        public Distance(DataObject dataObject, double distance)
        {
            this.dataObject = dataObject;
            this.distance = distance;
        }

        /**
         * @return the referenced data object in memory, to which the distance is measured
         */
        public DataObject getDataObject()
        {
            return dataObject;
        }

        @Override
        public int compareTo(Distance o)
        {
            double result = this.distance - o.distance;
            if (result < -0.0001)
                return -1;
            else if (result > 0.0001)
                return 1;
            else
                return 0;
        }
    }
}
