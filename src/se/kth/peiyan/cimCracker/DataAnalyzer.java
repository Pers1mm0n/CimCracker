package se.kth.peiyan.cimCracker;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * this class is responsible for:
 * 1) Database connection,
 * 2) read metadata (table, column) and data from database,
 * 3) create new table in a database
 * 3) populate table in database,
 * 
 * @author peiyanli
 * @version 0.1, May 16, 2015
 */
public class DataAnalyzer
{
    private String connPath;
    private Connection conn;
    private Statement statement;
    private boolean isConnSuccess = false;
    private MainFrame parent;
    
    /**
     * Constructor for connecting foreign database
     * 
     * @param connPath the database path for connecting foreign database
     */
    public DataAnalyzer(String connPath, MainFrame parent)
    {
        this.connPath = connPath;
        this.parent = parent;
        databaseConenction();
    }
    
    private void databaseConenction()
    {
        // connecting to database
        try
        {
            // The newInstance() call is a work around for some
            // broken Java implementations

            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) 
        {
            ex.printStackTrace();
            Logger.getLogger(DataAnalyzer.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try
        {   
            conn = DriverManager.getConnection(connPath);
            statement = conn.createStatement();
            isConnSuccess = true;
        } catch (SQLException ex)
        {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    public boolean isConnSuccess() {
        return isConnSuccess;
    }
    
    /**
     * get table name
     * @return table name
     */
    public String[] getTable()
    {
        StringBuilder builder = new StringBuilder(10);
        
        try {
            DatabaseMetaData md = conn.getMetaData();
            ResultSet resultSet = md.getTables(null, null, "%", null);
            while (resultSet.next())
            {
                builder.append(resultSet.getString(3) + ",");
            }
            return builder.toString().split(",");
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
            return new String[] {};
    }
    
    /**
     * get column titles for a specific table
     * 
     * @param tableName table name
     * @return column titles
     */
    public Vector getColumn(String tableName)
    {
        Vector column = new Vector();
        try
        {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName);
            int numColumns = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= numColumns; i++)
                column.add(resultSet.getMetaData().getColumnName(i));
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return column;
    }
    
    /**
     * fetch data specified by table from database
     * 
     * @param tableName
     */
    public Vector getData(String tableName)
    {
        Vector data = new Vector();
        try
        {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName);
            int numColumns = resultSet.getMetaData().getColumnCount();
            while (resultSet.next())
            {
                Vector row = new Vector();
                for (int i = 1; i <= numColumns; i++)
                {
                    row.add(resultSet.getString(i));
                }
                data.add(row);
            }
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return data;
    }
    
    /**
     * get database connection
     */
    public Connection getConn()
    {
        return conn;
    }

    /**
     * create new table
     * 
     * @param columnPath the selected table from export panel
     * @param isInnoDBselected true if InnoDB engine is selected
     */
    protected void createTables(ColumnPath columnPath, boolean isInnoDBselected)
    {
        StringVector tableNames = new StringVector();
        Vector<ArrayList<String>> columnInfo = new Vector<>();
        isInnoDBselected = false;
        for (String[] column : columnPath)
        {
            int index = tableNames.getIndex(column[0]);
            if (index == -1)
            {
                tableNames.add(column[0]);
                ArrayList<String> arrayList = new ArrayList<>();
                arrayList.add(column[1]);
                columnInfo.add(arrayList);
            } else
            {
                ArrayList<String> arrayList = columnInfo.get(index);
                arrayList.add(column[1]);
            }
        }
        if (!isInnoDBselected)
        {
            Element rootelement = parent.getDoc().getDocumentElement();
            try {
                for (int i = 0; i < tableNames.size(); i++)
                {
                    StringBuffer tableCreateStat = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
                    String tablename = tableNames.get(i).replace(':', '_');
                    tableCreateStat.append(tablename).append(" ");
                    
                    StringBuffer colDefinition = new StringBuffer();
                    StringBuffer colNames = new StringBuffer();
                    for (String colName : columnInfo.get(i))
                    {
                        colDefinition.append(colName.replace(':', '_').replace('.', '_'));
                        colDefinition.append(" VARCHAR(128),");
                        colNames.append(colName.replace(':', '_').replace('.', '_')).append(",");
                    }
                    tableCreateStat.append("(rdf_id VARCHAR(128) KEY,");
                    tableCreateStat.append(colDefinition.toString()).append(" FULLTEXT(");
                    
                    colNames.replace(colNames.length()-1, colNames.length(), "");
                    tableCreateStat.append(colNames.toString()).append("))").append(" ENGINE=MyISAM;");
                    statement.execute(tableCreateStat.toString());
                    
                    // Populate rows
                    NodeList elementList = rootelement.getElementsByTagName(tableNames.get(i));
                    for (int j = 0; j < elementList.getLength(); j++)
                    {
                        StringBuffer rowInsertState = new StringBuffer("INSERT INTO ");
                        rowInsertState.append(tablename).append(" (rdf_id,").append(colNames.toString()).append(")").append(" VALUES (");
                        
                        Element element = (Element) elementList.item(j);
                        String rdf_id = element.getAttribute("rdf:ID");
                        StringBuffer values = new StringBuffer();
                        for (String colName : columnInfo.get(i))
                        {
                            Node nameElement = element.getElementsByTagName(colName).item(0);
                            if (nameElement.hasChildNodes())
                            {
                                values.append("'").append(((Text) nameElement.getFirstChild()).getData().trim()).append("'").append(",");
                            } else if (nameElement.hasAttributes())
                            {
                                values.append("'").append(((Attr) ((Element) nameElement).getAttributes().item(0)).getValue().trim()).append("'").append(",");
                            }
                        }
                        values.replace(values.length()-1, values.length(), "");
                    
                        rowInsertState.append("'").append(rdf_id).append("',").append(values).append(");");
                    
                        statement.executeUpdate(rowInsertState.toString());
                    }
                }
            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        }
    }
    
}
