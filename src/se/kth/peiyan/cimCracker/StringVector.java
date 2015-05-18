package se.kth.peiyan.cimCracker;

import java.util.Vector;

/**
 * A enhanced Vector\<String\> structure to store a series of string
 * 
 * @author peiyanli
 * @version 0.1, May 16, 2015
 */
public class StringVector extends Vector<String>
{
    private transient int index = -1;
    
    @Override
    public boolean contains(Object o)
    {
	if (!(o instanceof String))
            return false;
			
        String other = (String) o;
        for (int i = 0; i < this.size(); i++)
        {
            if (this.get(i).equals(other))
            {
                index = i;
                return true;
            }
        }
	return false;
    }
    
    /*
     * get the index of specific item
     * return -1 if this item not contained in the ColumnPath
     */
    public int getIndex(Object o)
    {
        if (!(o instanceof String))
            return -1;
			
        String other = (String) o;
        for (int i = 0; i < this.size(); i++)
        {
            if (this.get(i).equals(other))
            {
                return i;
            }
        }
        return -1;
    }
}
