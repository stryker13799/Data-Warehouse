import java.sql.*;
import java.util.*;
import org.apache.commons.collections4.multimap.*;
import org.apache.commons.collections4.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoField;

public class meshJoin
{
	public static class MySQL	
	{
		private String username=null;
		private String password=null;
		private String schema=null;

		public MySQL()
		{
			readMySQLDetails();
		}

		public final Connection getConnection()
		{
			try
			{
				Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+schema, username,password);
				System.out.println("\nConnected With the database successfully");
				return connection;
			} catch (SQLException e)
			{
				System.out.println("Error while connecting to the database\nExiting...");
				System.exit(-1);
			}
			return null;
		}
		
		public final void readMySQLDetails()
		{
			try (Scanner sc = new Scanner(System.in))
			{
				 System.out.print("Enter your MySQL username (By default, it is: root)\n");
				 username=sc.next();
				 System.out.print("\nEnter your MySQL password\n");
				 password=sc.next();
				 System.out.print("\nEnter schema name where Transactonal and Masterdata is in\n");
				 schema=sc.next();
				 sc.close();
			}
			catch (Exception e)
			{
			    e.printStackTrace();
			}
		}

	}

	
	public static class meshJoinAlgo
	{
		private final int PARTITION_SIZE;
		public meshJoinAlgo(int p_size)
		{
			PARTITION_SIZE=p_size;
		}

		private final void algo(Connection connection) throws SQLException
		{
			int md_l_limit=0,td_l_limit=0;
			final Statement stmt = connection.createStatement();
			ArrayBlockingQueue<List<Map<String,String>>> queue = new ArrayBlockingQueue<List<Map<String,String>>>(PARTITION_SIZE);
			MultiValuedMap<String, Map<String, String>> multiHashMap = new ArrayListValuedHashMap<>();
			boolean flag=false;
			System.out.println("Beginning Mesh Join Algorithm...\n");
			
			while(true)
			{
				final String md_query="select * from masterdata " + "limit " + String.valueOf(md_l_limit) + ", "+String.valueOf(PARTITION_SIZE);
				md_l_limit+=PARTITION_SIZE;
				ResultSet md=stmt.executeQuery(md_query);
				if (!md.isBeforeFirst()) //No rows found i-e reach end of dataset
				{   
				    md_l_limit=0;
				    continue;
				}
				
				List<Map<String, String>> md_dataset = new ArrayList<Map<String, String>>();
				while(md.next())
				{
					Map<String, String> data = new HashMap<String, String>();

					final String pid = md.getString("PRODUCT_ID");
					final String pname = md.getString("PRODUCT_NAME");
					final String sid = md.getString("SUPPLIER_ID");
					final String sname = md.getString("SUPPLIER_NAME");
					final String price = md.getString("PRICE");

					data.put("PRODUCT_ID", pid);
					data.put("PRODUCT_NAME", pname);
					data.put("SUPPLIER_ID", sid);
					data.put("SUPPLIER_NAME", sname);
					data.put("PRICE", price);
	
					md_dataset.add(data);
				}
				
				final String td_query="select * from transactions " + "limit " + String.valueOf(td_l_limit) + ", 50";
				td_l_limit+=50;
				final ResultSet td = stmt.executeQuery(td_query);
				if (!td.isBeforeFirst()) //No rows found i-e reach end of dataset
				{    
					flag=true;
					if(queue.isEmpty())
					{
					    System.out.println("Reached end of transactional dataset and queue is empty\n"
					    		+ "Mesh Join Algorithm completed\nExiting...\n");
					    break;
					}

				}
				
				List<Map<String, String>> td_dataset = new ArrayList<Map<String, String>>();
				while (td.next())
				{
					Map<String, String> data = new HashMap<String, String>();
					final String tid = td.getString("TRANSACTION_ID");
					final String pid = td.getString("PRODUCT_ID");
					final String cid = td.getString("CUSTOMER_ID");
					final String cname = td.getString("CUSTOMER_NAME");
					final String sid = td.getString("STORE_ID");
					final String sname = td.getString("STORE_NAME");
					final String date = td.getString("T_DATE");
					final String quantity = td.getString("QUANTITY");
	
					data.put("TRANSACTION_ID", tid);
					data.put("PRODUCT_ID", pid);
					data.put("CUSTOMER_ID", cid);
					data.put("CUSTOMER_NAME", cname);
					data.put("STORE_ID", sid);
					data.put("STORE_NAME", sname);
					data.put("T_DATE", date);
					data.put("QUANTITY", quantity);
					
					td_dataset.add(data);
				}
				
				for (int j = 0; j < td_dataset.size(); j++)
				{
					Map<String, String> masterdata = td_dataset.get(j);
					multiHashMap.put(masterdata.get("PRODUCT_ID"), masterdata);
				}
				if(queue.size()>=PARTITION_SIZE || flag) //If queue full, or end of transaction data reached
				{
					for(Map<String,String> temp_map:queue.poll())
					{
						multiHashMap.removeMapping(temp_map.get("PRODUCT_ID"),temp_map);
					}
				}
				if(!flag)
				{
					queue.add(td_dataset);
				}

				for(Map<String,String>temp_md:md_dataset)
				{
					String cid=null,cname=null,pid=null,pname=null,pprice=null,sid=null,
							sname=null,supid=null,supname=null,date=null;
					for(Map<String,String> stream:multiHashMap.get(temp_md.get("PRODUCT_ID")))
					{
						try //Inserting into customer dimension
						{
							cid=stream.get("CUSTOMER_ID");
							cname=stream.get("CUSTOMER_NAME");
							final String cust_q="INSERT INTO CUSTOMER VALUES ('" + cid +"', '" + cname+ "');";
							insert_into_table(cust_q,stmt);
						}catch (SQLException e){}
						
						try //Inserting into product dimension
						{
							pid=stream.get("PRODUCT_ID");
							pname=temp_md.get("PRODUCT_NAME");
							pprice=temp_md.get("PRICE");
							final String prod_q="INSERT INTO PRODUCT VALUES ('" + pid+"', '" + pname+"', '"
									+ pprice + "');";
							insert_into_table(prod_q,stmt);
						}catch (SQLException e) {}
						
						try //Inserting into store dimension
						{
							sid=stream.get("STORE_ID");
							sname=stream.get("STORE_NAME");
							final String store_q="INSERT INTO STORE VALUES ('" + sid+"', '" + sname + "');";
							insert_into_table(store_q,stmt);
						}catch (SQLException e) {}
						
						try
						{
							supid=temp_md.get("SUPPLIER_ID");
							supname=temp_md.get("SUPPLIER_NAME");
							final String supp_q="INSERT INTO SUPPLIER VALUES ('" + supid+"', '" + supname + "');";
							insert_into_table(supp_q,stmt);
						}catch (SQLException e)
						{
							try //Handling unique case of suppler name "Cacey's", where SQL treats ' as delimiter for string, so exception is thrown
							{
								supid=temp_md.get("SUPPLIER_ID");
								supname =temp_md.get("SUPPLIER_NAME");
								supname = supname.replaceAll("'", "");
								final String supp_q = "INSERT INTO SUPPLIER VALUES ('" + supid +"', '" + supname + "');";
								insert_into_table(supp_q,stmt);	
							}catch (SQLException d) {}
						}
						
						try //Inserting into dates dimension
						{
							char[]day=new char[2];
							char[]month=new char[2];
							char[]year=new char[4];
							final String weekend;
							int quarter=0;
							
							date = stream.get("T_DATE");
							date.getChars(0, 4, year, 0);
							date.getChars(5,7,month,0);
							date.getChars(8, 10, day, 0);
							quarter=(Integer.parseInt(String.valueOf(month)));
							quarter=((quarter-1)/3)+1;
							if(isWeekend(date))
							{
								weekend="Yes";
							}
							else
							{
								weekend="No";
							}
							
							String date_q="INSERT INTO DATES VALUES ('" + date +  "', '" + String.valueOf(day) + "', '" + String.valueOf(month) +
											"', '" + String.valueOf(quarter) + "', '" + String.valueOf(year)+  "', '" + weekend +"');";
							
							insert_into_table(date_q,stmt);
						}catch (SQLException e) {}
						
						
						//Now inserting into FACT Table
						try
						{
							final String quantity=stream.get("QUANTITY");
							final Float sale_price=Integer.valueOf(quantity)*Float.valueOf(pprice);
					
							final String fact_q="INSERT INTO FACT VALUES ('" + pid +"', '" + quantity +"', '" + cid +"', '"
											+ sid +"', '" + supid +"', '"+ String.valueOf(sale_price)+"', '" + date +"');";
							insert_into_table(fact_q,stmt);
						}catch (SQLException e) {}
					}
				}
			}	
		}
		
		
		private final void insert_into_table(String query,Statement stmt) throws SQLException
		{
			stmt.executeUpdate(query);
		}
		
	    private final static boolean isWeekend(final String date) 
	    {
	    	LocalDate ld=LocalDate.parse(date);
	        DayOfWeek day = DayOfWeek.of(ld.get(ChronoField.DAY_OF_WEEK));
	        return day == DayOfWeek.SUNDAY || day == DayOfWeek.SATURDAY;
	    }
	}
	
	
	public static void main(String[] args) throws SQLException
	{
		MySQL mysql = new MySQL();
		meshJoinAlgo mesh = new meshJoinAlgo(10);

		Connection connection = mysql.getConnection();
		mesh.algo(connection);
		
		return;
	}
}