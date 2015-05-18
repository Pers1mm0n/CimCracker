package se.kth.peiyan.cimCracker;

import java.util.Vector;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * A class responsible for preparing cim Object types in a XML file.
 *  
 * @author peiyanli
 * @version 0.1, May 16, 2015
 */
public class XMLExporter
{
    private Element root;
    private StringVector cimObjectNames;
    Vector<Vector<String>> cimObjects;

    public XMLExporter(Document doc)
    {
        this.root = doc.getDocumentElement();
        
        // get cim objects
	NodeList list = root.getChildNodes();
        cimObjectNames = new StringVector();
        cimObjects = new Vector<>();
        for (int i = 0; i < list.getLength(); i++)
	{
            StringBuffer cimObjectName = new StringBuffer(list.item(i).getNodeName());
            if (cimObjectName.charAt(0) != '#' && !cimObjectNames.contains(cimObjectName.toString()))
            {
		cimObjectNames.add(list.item(i).getNodeName());
		cimObjects.add(new Vector<String>());
            }
	}
        
        // get childs for every cim objects
	for (int j = 0; j < cimObjectNames.size(); j++)
	{
            String tag = cimObjectNames.get(j);
            Element element = (Element) root.getElementsByTagName(tag).item(0);
            NodeList subList = element.getChildNodes();
            
            for (int i = 0; i < subList.getLength(); i++)
            {
		StringBuffer cimObjectProperties = new StringBuffer(subList.item(i).getNodeName());
		if (cimObjectProperties.charAt(0) != '#' && !cimObjects.get(j).contains(cimObjectProperties.toString()))
                {
                    cimObjects.get(j).add(cimObjectProperties.toString());
                }
            }
	}
    }

    public StringVector getCimObjectNames()
    {
        return cimObjectNames;
    }

    public Vector<Vector<String>> getCimObjects()
    {
        return cimObjects;
    }
}
