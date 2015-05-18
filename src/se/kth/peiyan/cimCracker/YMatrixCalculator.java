package se.kth.peiyan.cimCracker;

import java.util.ArrayList;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.math3.complex.Complex;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * Class responsible for Y Matrix calculation
 * 
 * @author peiyanli
 * @version 0.1, May 16, 2015
 */
public class YMatrixCalculator
{
    private Document doc;
    private StringVector lineIDs;
    private Vector<String[]> connections;
    private StringVector buses;
    Vector data;
    Vector dataPrepare;

    public YMatrixCalculator(Document doc)
    {
        this.doc = doc;
        
        Element root = doc.getDocumentElement();
        NodeList list = root.getElementsByTagName("cim:Line");
	Pattern pattern = Pattern.compile("[a-zA-Z0-9]+ +to +[a-zA-Z0-9]+");// using regular expression find out which two buses are connected through a line
		
	lineIDs = new StringVector();
	connections = new Vector<>();
	for (int i = 0; i < list.getLength(); i++)
	{
            Element element = (Element) list.item(i);
            String rdf_id = element.getAttribute("rdf:ID");
            String name = ((Text) (((Element) element.getElementsByTagName("cim:IdentifiedObject.name").item(0)).getFirstChild())).getData().trim();
            if (!lineIDs.contains(rdf_id))
            {
		lineIDs.add(rdf_id);
		Matcher matched = pattern.matcher(name);
		if (matched.find())
		{
                    connections.add(matched.group(0).toUpperCase().split(" TO "));
		}
            }
	}
		
        buses = new StringVector();
        connections.stream().forEach((connection) ->
        {
            for (String bus : connection)
            {
                if (!buses.contains(bus))
                {
                    buses.add(bus);
                }
            }
        });
		
        NodeList lineSegList = root.getElementsByTagName("cim:ACLineSegment");
		
        Complex[] diagonalEntities = new Complex[buses.size()];
		
        // calculate the diagonal entity
        for (int k = 0; k < buses.size(); k++)
	{
            String bus = buses.get(k);
            diagonalEntities[k] = new Complex(0);
			
            ArrayList<Integer> connectedLine = getWhichLineConnected(bus);
            // get the line data
            for (int i : connectedLine)
            {
		String lineID = lineIDs.get(i);
		for (int j = 0; j < lineSegList.getLength(); j++)
		{
                    Element lineSegElement = ((Element) lineSegList.item(j));
                    Element lineSegContainer = (Element) lineSegElement.getElementsByTagName("cim:Equipment.MemberOf_EquipmentContainer").item(0);
                            
                    if (lineSegContainer.getAttribute("rdf:resource").trim().replaceAll("#", "").equals(lineID))
                    {
			double x_l = Double.parseDouble(((Text) ((Element) lineSegElement.getElementsByTagName("cim:ACLineSegment.x").item(0)).getFirstChild()).getData().trim());
			double r_l = Double.parseDouble(((Text) ((Element) lineSegElement.getElementsByTagName("cim:ACLineSegment.r").item(0)).getFirstChild()).getData().trim());
			diagonalEntities[k] = diagonalEntities[k].add(new Complex(1).divide(new Complex(r_l, x_l)));
                    }
		}
            }
	}
		
	// prepare data for table model, the data structure is lower triangular matrix
	dataPrepare = new Vector();
	for (int j = 0; j < buses.size(); j++)
	{
            Vector row = new Vector();
            for (int i = 0; i < (j + 1); i++)
            {
            if (i == j)
                row.add(diagonalEntities[i]);
                else
		{
                    int connectedLine = getConnectedLine(j, i);
                    String lineID = lineIDs.get(connectedLine);
		
                    for (int j1 = 0; j1 < lineSegList.getLength(); j1++)
                    {
			Element lineSegElement = ((Element) lineSegList.item(j1));
			Element lineSegContainer = (Element) lineSegElement.getElementsByTagName("cim:Equipment.MemberOf_EquipmentContainer").item(0);
			
			if (lineSegContainer.getAttribute("rdf:resource").trim().replaceAll("#", "").equals(lineID))
                        {
                            double x_l = Double.parseDouble(((Text) ((Element) lineSegElement.getElementsByTagName("cim:ACLineSegment.x").item(0)).getFirstChild()).getData().trim());
                            double r_l = Double.parseDouble(((Text) ((Element) lineSegElement.getElementsByTagName("cim:ACLineSegment.r").item(0)).getFirstChild()).getData().trim());
                            Complex z = new Complex(r_l, x_l);
                            Complex y = new Complex(1).divide(z).multiply(-1);
                            row.add(y);
			}
                    }
		}
            }
            dataPrepare.add(row);
	}
		
	data = new Vector();
	for (int j = 0; j < buses.size(); j++)
	{
            Vector row = new Vector();
            for (int i = 0; i < buses.size(); i++)
            {
                if (i < (j + 1))
                    row.add((Complex) ((Vector) dataPrepare.get(j)).get(i));
		else
                    row.add((Complex) ((Vector) dataPrepare.get(i)).get(j));
            }
            data.add(row);
	}	
    }
    
    private int getConnectedLine(int j, int i)
    {
	int returnValue = -1;
	String bus = buses.get(j);
	ArrayList<Integer> connectedLines = getWhichLineConnected(bus);
	ArrayList<Integer> connectedBuses = new ArrayList<Integer>();
	for (int k : connectedLines)
	{
            String[] connectedBusNames= connections.get(k);
            for (String connectedBusName : connectedBusNames)
            {
		if (connectedBusName.equals(buses.get(i)))
		return k;
            }
	}
	
	return returnValue;
	}
	
    private ArrayList<Integer> getWhichLineConnected(String bus)
    {
        ArrayList<Integer> returnValue = new ArrayList<>();
        for (int i = 0; i < connections.size(); i++)
        {
            for (String each : connections.get(i))
            {
                if (bus.equals(each))
                returnValue.add(i);
            }
        }
        return returnValue;
    }

    public StringVector getBuses()
    {
        return buses;
    }

    public Vector getData()
    {
        return data;
    }
}
