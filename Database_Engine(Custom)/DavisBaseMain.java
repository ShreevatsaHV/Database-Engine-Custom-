import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class DavisBaseMain {	
	static String prompt = "davisql> ";
	static boolean isExit = false;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static CreateNew randomAccessTableFile;
	static CreateNew randomAccessColFile;
	static String version = "v1.099";
	static int pageSize = 1024;
	static String copyright = "©2017 Shreevatsa H V";	
	static Map<String, BPlusTree> BPlusMap = new HashMap<String, BPlusTree>();
	
	public static void initializeMetaData() throws IOException {
		
		File dir = new File("Data");		

		 if(!dir.exists()) 
		 {
			dir.mkdir();			 
			File dir2 = new File("Data\\catalog");
			dir2.mkdir();
			
			File dir3 = new File("Data\\user_data");
			dir3.mkdir();

			File tablefile = new File("Data\\catalog\\davisbase_tables.tbl");			
			randomAccessTableFile = new CreateNew(tablefile, "rw", 512);
			randomAccessTableFile.seek(1);
			randomAccessTableFile.writeShort(13);
			randomAccessTableFile.setLength(512);
			randomAccessTableFile.insertToDBTables("davisbase_tables");
			randomAccessTableFile.insertToDBTables("davisbase_columns");
						
			File columnfile = new File("Data\\catalog\\davisbase_columns.tbl");
			randomAccessColFile = new CreateNew(columnfile, "rw", 2048);
			randomAccessColFile.seek(1);
			randomAccessColFile.writeShort(13);
			randomAccessColFile.insertToDBColumns("davisbase_tables", "rowid", "BYTE", "PRI", 1, "NO");
			randomAccessColFile.insertToDBColumns("davisbase_tables", "table_name", "TEXT", "", 2, "YES");			
			randomAccessColFile.insertToDBColumns("davisbase_columns", "rowid", "BYTE", "PRI", 1, "NO");
			randomAccessColFile.insertToDBColumns("davisbase_columns", "table_name", "TEXT", "", 2, "YES");
			randomAccessColFile.insertToDBColumns("davisbase_columns", "column_name", "TEXT", "", 3, "YES");
			randomAccessColFile.insertToDBColumns("davisbase_columns", "data_type", "SMALLINT", "", 4, "YES");
			randomAccessColFile.insertToDBColumns("davisbase_columns", "column_key", "TEXT", "", 5, "YES");
			randomAccessColFile.insertToDBColumns("davisbase_columns", "ordinal_position", "TINYINT", "", 6, "YES");
			randomAccessColFile.insertToDBColumns("davisbase_columns", "is_nullable", "TEXT", "", 7, "YES");
							
		 }
		 else
		 {
			File fiTb = new File("Data\\catalog\\davisbase_tables.tbl");			
			randomAccessTableFile = new CreateNew(fiTb, "rw", 512);
			randomAccessTableFile.recordLength = 21;
			randomAccessTableFile.noColumns = 2;
			randomAccessTableFile.seek(2);
			randomAccessTableFile.raTablePos = randomAccessTableFile.readShort();
			randomAccessTableFile.noRecords = randomAccessTableFile.findNumRecs(1);
			randomAccessTableFile.startPosition = 8 + randomAccessTableFile.noRecords*2;
					
			File fileColumn = new File("Data\\catalog\\davisbase_columns.tbl");
			randomAccessColFile = new CreateNew(fileColumn, "rw", 2048);
			System.out.println("Data folder already exists");
			randomAccessColFile.recordLength = 84;
			randomAccessColFile.noColumns = 7;
			randomAccessColFile.seek(2);
			randomAccessColFile.raColumnPos = randomAccessColFile.readShort();		
			randomAccessColFile.noRecords = randomAccessColFile.findNumRecs(1);
			randomAccessColFile.startPosition = 8 + randomAccessColFile.noRecords*2;
			
			randomAccessTableFile.seek(8);
			int start = randomAccessTableFile.readShort() + 1;
			randomAccessTableFile.seek(1);
			int count = randomAccessTableFile.readByte();
			
			while(count != 0) {
				randomAccessTableFile.seek(start);
				String table_name = randomAccessTableFile.readLine().substring(0, 20).trim();
				if(!(table_name.equalsIgnoreCase("davisbase_tables")) && !(table_name.equalsIgnoreCase("davisbase_columns"))) {
					File file = new File("Data\\user_data\\" + table_name + ".tbl");
					CreateNew NeoTable = new CreateNew(file, "rw", pageSize);
					NeoTable.pagesCount = (int) ((NeoTable.length()/512) - 1); 
					NeoTable.noRecords = NeoTable.findNumRecs(NeoTable.pagesCount); 
					int value[] = randomAccessColFile.findNumofCols_RecLen(table_name); 
					NeoTable.noColumns = value[0];
					NeoTable.recordLength = value[1];
					
					if(NeoTable.pagesCount == 1) {						
						NeoTable.seek(1);
						int numberOfRecords = NeoTable.readByte();						
						if(numberOfRecords == 0) {
							NeoTable.pointer = 512;
						} else {
							NeoTable.seek(2);
							NeoTable.pointer = NeoTable.readShort();
						}
						NeoTable.startPosition = 8 + numberOfRecords*2; 
					}
					else 
					{
						NeoTable.seek(1024 + (NeoTable.pagesCount - 2)*512 + 1);
						int numberOfRecords = NeoTable.readByte();
						if(numberOfRecords == 0) {
							NeoTable.pointer = 1024 + (NeoTable.pagesCount-1)*512;
						} else {
							NeoTable.seek(1024 + (NeoTable.pagesCount - 2)*512 + 2);
							NeoTable.pointer = NeoTable.readShort();
						}
						NeoTable.startPosition = (1024 + (NeoTable.pagesCount - 2)*512) + numberOfRecords*2;
					}
										
					BPlusTree bPlusTree = new BPlusTree(NeoTable);
					BPlusMap.put(table_name, bPlusTree);
				}				
				start = start - randomAccessTableFile.recordLength;
				count--;
			}		
		 }	
	}
	
	public static void main(String[] args) throws IOException {
		initializeMetaData();
		splashScreen();
		String userCommand = ""; 
		
		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("C:>");
	}

	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void parseUserCommand (String userCommand) throws IOException {
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
			case "show":
				parseShowString(userCommand);
			break;
			case "create":
				parseCreateString(userCommand);
			break;
			case "insert":
				parseInsertString(userCommand);
			break;
			case "select":
				parseQueryString(userCommand);
			break;
			case "delete":
				parseDeleteString(userCommand);
			break;
			case "drop":
				parseDropString(userCommand);
			break;
			case "update":
				parseUpdateString(userCommand);
			break;
			case "help":
				help();
			break;
			case "version":
				displayVersion();
			break;
			case "quit":
				isExit = true;
			break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tCREATE TABLE table_name (column_name1 INT PRI NO,column_name2 data_type2 [YES/NO],column_name3 data_type3 [YES/NO]);             Create table command: Pri for primary key and is in 1st column only. For nullable, yes or no has to be given for each column");
		System.out.println("\tINSERT INTO TABLE (column_name1,column_name2,column_name3) table_name VALUES (value1,value2,value3);       													Insert data into table.All column names has to be given in column list in order. All values has to be given too and in order");
		System.out.println("\tINSERT INTO TABLE () table_name values (2,aa,aa,aa);                                                                              Insert into table without giving column list. But the brackets are mandatory");
		System.out.println("\tSELECT * FROM table_name;                        																					Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE column_name = value;  																					Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                          																					Remove table data and its schema.");
		System.out.println("\tUPDATE table_name SET column_name = value;      																					Update table data without where condition.");
		System.out.println("\tUPDATE table_name SET column_name = value WHERE column_name = value;      														Update table data with a where condition.");
		System.out.println("\tDELETE FROM TABLE table_name;        																								Delete all records");
		System.out.println("\tDELETE FROM TABLE table_name WHERE column_name = value;       																		Delete records whose rowid is <id>.");
		System.out.println("\tSHOW tables;           																											Display the table names");
		System.out.println("\tVERSION;                                         																					Show the program version.");
		System.out.println("\tHELP;                                            																					Show this help information");
		System.out.println("\tQUIT;                                            																					Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}

	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	private static void parseQueryString(String userCommand) throws IOException {
		/* SELECT * FROM table_name WHERE column_name operator value; */
		ArrayList<String> querySplit = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		String wildCard = querySplit.get(1);
		String checkingColumn = null, operator = null, valueToCompare = null;
		String tableName = querySplit.get(3);
		if(querySplit.size()>4){
            checkingColumn = querySplit.get(5);
            operator = querySplit.get(6);
            valueToCompare = querySplit.get(7);
         }		
		BPlusTree btree = BPlusMap.get(tableName);
		btree.root.queryFromTable(tableName, wildCard, checkingColumn, operator, valueToCompare, randomAccessTableFile, randomAccessColFile);	
	}

	private static void parseDeleteString(String userCommand) throws IOException {
		//DELETE FROM TABLE table_name WHERE row_id = key_value;		
		ArrayList<String> querySplit = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if((querySplit.size()==4) || (querySplit.size()==8)) 
		{
			String tableName = querySplit.get(3);
			String checkingColumn = null, operator = null, valueToCompare = null;
			if(querySplit.size() > 4){
	            checkingColumn = querySplit.get(5);
	            operator = querySplit.get(6);
	            valueToCompare = querySplit.get(7);
	         }		
			BPlusTree btree = BPlusMap.get(tableName);
			btree.root.deleteFromTable(tableName, checkingColumn, operator, valueToCompare, randomAccessTableFile, randomAccessColFile);
		}
		else
		{
			System.out.println("Invalid query(query format incorrect)");
		}
	}
	private static void parseShowString(String userCommand) throws IOException {
		randomAccessTableFile.processShowTableQuery();		
	}
	
	private static void parseDropString(String userCommand) throws IOException {
		//DROP TABLE table_name;
		ArrayList<String> querySplit = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if((querySplit.size()==3)) {
			String tableName = querySplit.get(2);
	
			BPlusTree btree = BPlusMap.get(tableName);
			btree.root.processDropString(tableName, randomAccessTableFile, randomAccessColFile);
			btree.root.close();
			btree.root.f.delete();		
		}
		else
		{
			System.out.println("Invalid query(query format incorrect)");
		}
	}
	private static void parseCreateString(String userCommand) throws IOException {
		String splitElements[] = userCommand.split("\\(");
		String getTable[] = splitElements[0].split(" ");
		String tableName = getTable[2];
		
		randomAccessTableFile.seek(1);
		int numberOfRecords = randomAccessTableFile.readByte();
		
		randomAccessTableFile.seek(2);
		int raTablePos = randomAccessTableFile.readShort() + 1; // this position will give us the table name
		randomAccessTableFile.seek(raTablePos);
		String raReferTableline = randomAccessTableFile.readLine().substring(0, 20);
		int m = 1;
		int w;
		for (w = 0; w < (numberOfRecords-2); w++) {
			if(!raReferTableline.contains(tableName)) { 
				raTablePos = raTablePos + 21*(m);
				randomAccessTableFile.seek(raTablePos);
				raReferTableline = randomAccessTableFile.readLine().substring(0, 20);
			}
			else 
				break;
		}
		
		if(w == (numberOfRecords-2)) {
		}
			
		else
		{
			System.out.println("Table exists");
			return;
		}
		
		randomAccessTableFile.insertToDBTables(tableName);
		randomAccessColFile.insertToCreatedTable(tableName, splitElements[1]);
		
		File f = new File("Data\\user_data\\" + tableName + ".tbl");
		CreateNew NeoTable = new CreateNew(f, "rw", pageSize);
		NeoTable.setLength(pageSize);
		NeoTable.seek(4);
		NeoTable.writeInt(-1);
		int roottartPosition = pageSize - 512;
		NeoTable.seek(roottartPosition);
		NeoTable.writeByte(5);
		
		NeoTable.seek(roottartPosition + 4);
		NeoTable.writeInt(0);
		NeoTable.setPageType(13);
		
		splitElements[1] = splitElements[1].substring(0, splitElements[1].length()-1);
		String columns[] = splitElements[1].split(",");
		NeoTable.noColumns = columns.length;
		
		for (int i = 0; i < columns.length; i++) {
			String temp1[] = columns[i].split(" ");
			NeoTable.recordLength += NeoTable.ut.getRecLenAccordingToData(temp1[1]);
		}		
		BPlusTree bPlusTree = new BPlusTree(NeoTable);
		BPlusMap.put(tableName, bPlusTree);
	}
	
	public static void parseInsertString(String userCommand) throws IOException {
		//INSERT INTO TABLE (column_list) table_name VALUES (value1,value2,value3,…);
		String splitElements[] = userCommand.split(" ");
		String tableName = splitElements[4];
		String columnList[] = splitElements[3].substring(1,splitElements[3].length()-1).split(",");
		String columnValues[] = splitElements[6].substring(1,splitElements[6].length()-1).split(",");
		if(((columnList.length == 1) && (columnList[0].equals(""))) || columnList.length == columnValues.length) {
			BPlusTree tree = BPlusMap.get(tableName);
			tree.root.insertRecordIntoTable(tableName, columnList, columnValues, randomAccessTableFile, randomAccessColFile, tree);
		} else {
			System.out.println("Incorrect format");
		}	
	}
	
	private static void parseUpdateString(String userCommand) throws IOException {
		// UPDATE table_name SET column_name = value [WHERE column_name = value];
		ArrayList<String> querySplit = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		if((querySplit.size()==6) || (querySplit.size()==10)) {
			String tableName = querySplit.get(1);
			String columnToBeUpdated = querySplit.get(3);	
			String valueToBeSet = querySplit.get(5);
			String checkingColumn = null, valueToCompare = null, operator = null;
			if(querySplit.size() > 6){
				checkingColumn = querySplit.get(7);
				operator = querySplit.get(8);
				valueToCompare = querySplit.get(9);			
	         }		
			BPlusTree btree = BPlusMap.get(tableName);
			btree.root.processUpdateString(tableName, columnToBeUpdated, valueToBeSet, checkingColumn, operator, valueToCompare, randomAccessTableFile, randomAccessColFile);
		
		}
		else
		{
			System.out.println("Invalid query(query format incorrect)");
		}
	}
}
