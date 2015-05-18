package se.kth.peiyan.cimCracker;

import java.util.Vector;

/**
 * This is a data structure based on Vector<String[]> specifying for registering 
 * the hierarchic structure of cim XML.
 * 
 * Example: if I make choose on the "Export Data" panel, the selection information 
 * will be stored like:[[cim:GeographicalRegion, cim:identifiedObject.Name],
 *                      [cim:GeographicalRegion, cim:identifiedObject.localName],
 *                      [cim:SubGeographicalRegion, cim:SubGeographicalRegion.Region]]
 * 
 * Root--
 *      |---cim:GeographicalRegion
 *      | |---[✔]cim:identifiedObject.Name
 *      | |---[✔]cim:identifiedObject.localName
 *      |
 *      |---cim:SubGeographicalRegion
 *      | |---[ ]cim:identifiedObject.Name
 *      | |---[ ]cim:identifiedObject.localName
 *      | |---[✔]cim:SubGeographicalRegion.Region
 *      |
 * 
 * @author peiyanli
 * @version 0.1, May 16, 2015
 */
class ColumnPath extends Vector<String[]>
{
    private transient int index = -1;
	
    /**
     * return if "one selection" is stored.
     * 
     * @param o Other object
     */
    @Override
    public boolean contains(Object o)
    {
	if (!(o instanceof String[]))
            return false;
	
	String[] other = (String[]) o;
	for (int i = 0; i < this.size(); i++)
	{
            String[] storedPath = this.get(i);
            if (storedPath[1].equals(other[1]) && storedPath[0].equals(other[0]))
            {
		index = i;
		return true;
            }
	}
        return false;
    }
	
    /**
     * get the index of specific item
     * return -1 if this item not contained in the ColumnPath
     * 
     * @param o Other object
     */
    public int getIndex(Object o)
    {
        if (!(o instanceof String[]))
            return -1;
	
	String[] other = (String[]) o;
	for (int i = 0; i < this.size(); i++)
	{
            String[] storedPath = this.get(i);
            if (storedPath[1].equals(other[1]) && storedPath[0].equals(other[0]))
            {
		return i;
            }
	}
        return -1;
    }
}
