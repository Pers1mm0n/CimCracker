package se.kth.peiyan.cimCracker;

import java.util.ArrayList;

/**
 * A class represent data objects
 * 
 * @author peiyanli
 * @version 0.1, June 12, 2015
 */
public class DataObject extends DataPoint
{
    private final String id;
    private Centroid centroid;
    
    /**
     * construct a Centroid instance
     * 
     * @param identifier identifier --- should be unique for different cluster centroid
     * @param position the location of cluster centroid
     * @param time the 'time' attribute corresponding to the 'time' in database
     */
    public DataObject(int identifier,
            ArrayList<Double> position,
            String time)
    {
        super(identifier, position);
        id = time;
    }

    /**
     * 
     * @return id
     */
    public String getId()
    {
        return id;
    }

    /**
     * 
     * @return centroid
     */
    public Centroid getCentroid()
    {
        return centroid;
    }
    
    /**
     * calculate the distance to another DataPoint
     * 
     * @param otherPoint
     * @return 
     */
    public double calculateDistance(DataPoint otherPoint)
    {
        double distance = -1;
        
        double[] otherPosition = otherPoint.getPosition();
        double[] thisPosition = this.getPosition();
        double summation = 0.0;
        
        for (int i = 0; i < otherPosition.length; i++)
        {
            double diff = otherPosition[i] - thisPosition[i];
            summation += diff * diff;
        }
        
        return Math.sqrt(summation);
    }
    
    /**
     * assignment cluster centroid from a candidate list
     * 
     * @param centroids a candidate list of cluster centroids
     */
    public void centroidAssignment(ArrayList<Centroid> centroids)
    {
        ArrayList<Double> distances = new ArrayList<>();
        double minDistance = centroids.stream().mapToDouble(c ->
        {
            double distance = this.calculateDistance(c);
            distances.add(distance);
            return distance;
        }).min().getAsDouble();
        
        int index = distances.indexOf(minDistance);
        centroid = centroids.get(index);
    }
}
