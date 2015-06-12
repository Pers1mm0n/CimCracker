package se.kth.peiyan.cimCracker;

import java.util.ArrayList;

/**
 * represent an abstract point
 * 
 * @author peiyanli
 * @version 0.1, June 12, 2015
 */
public class DataPoint
{
    private final int identifier;
    private ArrayList<Double> position = new ArrayList<>();

    /**
     * constructor for a DataPoint instance
     * 
     * @param identifer
     * @param position 
     */
    public DataPoint(int identifer, ArrayList<Double> position)
    {
        this.identifier = identifer;
        this.position = position;
    }

    /**
     * 
     * @return identifier
     */
    public int getIdentifier()
    {
        return identifier;
    }
    
    /**
     * 
     * @return position
     */
    public double[] getPosition()
    {
        double[] position = new double[this.position.size()];
        int index = 0;
        for (double entity : this.position)
        {
            position[index] = entity;
            index++;
        }
        
        return position;
    }
    
    /**
     * move the dataPoint to a new position
     * 
     * @param newPosition 
     */
    public void setPosition(double[] newPosition)
    {
        position.clear();
        for (int i = 0; i < newPosition.length; i++)
        {
            position.add(newPosition[i]);
        }
    }
}
