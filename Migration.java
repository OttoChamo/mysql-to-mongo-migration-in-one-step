import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

/*
* @author : Sushil jain
* @Email  : contact@sushiljain.in
* @website: http://www.sushiljain.in/
*
**/

public class Migration {   
    public static void main(String[] args) {
            
        try{

          //Load mysql driver
             Class.forName("com.mysql.jdbc.Driver");
              // Setup the connection with the DB
              Connection connect = DriverManager.getConnection("jdbc:mysql://HOST:PORT/DATABASE_NAME?zeroDateTimeBehavior=convertToNull", "username", "password");

             //get meta object for database meta info
             DatabaseMetaData meta = connect.getMetaData();
              // Result set get the result of the SQL query
             ResultSet resultSet= meta.getTables(null, null, null, new String[]{"TABLE"});
             //Connect to Mongo
             Mongo m = new Mongo("localhost", 27017);
   	     //Select database in mongo	
 	     DB db = m.getDB("MONGO_DATBASE_NAME");
 			           
             while(resultSet.next()){
                
             String tableName = resultSet.getString(3);
             
                Statement stmt = connect.createStatement();
                ResultSet rs = stmt.executeQuery("show columns from "+tableName);
                //Start - Creating mysql query for getting all column names from mysql
                HashMap<String,String> fieldsNameArray = new HashMap<String,String>();
                StringBuilder query = new StringBuilder("select ");
                int totalFields = 0;
                while(rs.next())
                {
                	fieldsNameArray.put(rs.getString("Field"), rs.getString("Type"));
                	query.append("`"+rs.getString("Field")+"`").append(",");
                	totalFields++;
                }
                if(totalFields == 0)
                	continue;
                
                query.deleteCharAt(query.length() -1);
                query.append(" from ").append(tableName).append(";");
                //End creating MySQL Query
                //Now execute query                
                Statement stmt2 = connect.createStatement();
                ResultSet rs2 = stmt2.executeQuery(query.toString());
                Object[]obj = fieldsNameArray.keySet().toArray();
                DBCollection col = db.getCollection(tableName);
    		//For every row create mongo entry and inserting it
                int i = 0;		
                while(rs2.next())
                {
                	BasicDBObject mongoObj = new BasicDBObject();
                	for (int j = 0; j < obj.length; j++)
                	{
				String val = rs2.getString(obj[j].toString());
				mongoObj.append(obj[j].toString(), val);
			}
                	
                	col.insert(mongoObj);
                    	i++;
                        System.out.println("Copying data from table : "+tableName+" -- Row# : "+i);
                }
            }
            //Setting database related objects to null and closing database connections.
            rs2 = null;
            meta = null;
            resultSet = null;
            connect.close();
            m.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
    }
    
    
}
