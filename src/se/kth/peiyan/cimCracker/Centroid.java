package se.kth.peiyan.cimCracker;

import java.util.ArrayList;

/**
 * A class represent cluster centroid
 * 
 * @author peiyanli
 * @version 0.1, June 12, 2015
 */
public class Centroid extends DataPoint
{
    private ArrayList<DataObject> members = new ArrayList<>();
    
    /**
     * construct a Centroid instance
     * 
     * @param identifier identifier --- should be unique for different cluster centroid
     * @param position the location of cluster centroid
     */
    public Centroid(int identifier, ArrayList<Double> position)
    {
        super(identifier, position);
    }
    
    // get all the data object clustered into this centroid
    private void setMembers(ArrayList<DataObject> dataObjects)
    {
        dataObjects.forEach(d -> 
        {
            if (d.getCentroid() == this)
                members.add(d);
        });
    }
    
    /**
     * move the centroid to the mean focus of this cluster
     * 
     * @param dataObjects
     * @return if the centroid moved
     */
    public boolean moveCentroid(ArrayList<DataObject> dataObjects)
    {
        setMembers(dataObjects);
        double[] oldPosition = this.getPosition();
        double[] newPosition = new double[oldPosition.length];
        int memberSize = members.size();
        
        members.stream().map((member) -> member.getPosition()).forEach((position) ->
        {
            for (int i = 0; i < oldPosition.length; i++)
            {
                newPosition[i] += position[i];
            }
        });
        
        for (int i = 0; i < oldPosition.length; i++)
        {
            newPosition[i] = newPosition[i] / memberSize;
        }
        setPosition(newPosition);
        
        boolean flag = false;
        for (int i = 0; i < oldPosition.length; i++)
        {
            if (newPosition[i] - oldPosition[i] > 0.0001)
            {
                flag |= true;
            }
        }
        
        return (flag);
    }
}