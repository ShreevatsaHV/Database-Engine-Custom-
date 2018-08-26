import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.*;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CreateNew extends RandomAccessFile {

	File f;
	int pageSize, noRecords, pageType, noRecRoot, positionRoot, rowId, pointer, raTablePos, raColumnPos, recordLength, noColumns;
	int startPosition = 8;
	int pagesCount = 1;
	Utility ut = new Utility();
	
	public CreateNew(File file, String mode, int pageSize) throws FileNotFoundException {
		super(file, mode);
		this.f = file;
		this.pageSize = pageSize;
		pointer = pageSize/2;
		positionRoot = pageSize;
		raTablePos = pageSize;
		raColumnPos = pageSize;
	}

	public CreateNew(String file, String mode, int pageSize) throws FileNotFoundException {
		super(file, mode);
		this.pageSize = pageSize;
		pointer = pageSize/2;
		positionRoot = pageSize;
		raTablePos = pageSize;
		raColumnPos = pageSize;
	}
	
	public void insertToDBTables(String tableName) throws IOException {		
		recordLength = 1 + 20;
		seek(1);
		noRecords += 1;
		writeByte(noRecords);
		raTablePos = raTablePos - recordLength;
		seek(2);
		writeShort(raTablePos);
		seek(startPosition);
		writeShort(raTablePos);
		startPosition += 2;		
		seek(raTablePos);
		writeByte(++rowId);		
		seek(raTablePos+1);
		writeBytes(tableName);			
		noColumns = 2;
	}

	public void setPageType(int pageType) throws IOException {
		this.pageType = pageType;
		seek(0);
		writeByte(pageType);
	}
	
	public void insertToDBColumns(String tableName, String columnName,
			String data_type, String columnKey, int ordinal_position, String is_nullable) throws IOException {

		recordLength = 1 + 20 + 20 + 2 + 20 + 1 + 20;		
		seek(1);
		//System.out.println(noRecords);
		noRecords += 1;
		writeByte(noRecords);		
		raColumnPos = raColumnPos - recordLength;
		//System.out.println(raColumnPos);
		seek(2);
		writeShort(raColumnPos);		
		seek(startPosition);
		writeShort(raColumnPos);
		startPosition += 2;
		
		seek(raColumnPos);
		writeByte(++rowId); 
		seek(raColumnPos+1);
		writeBytes(tableName);
		seek(raColumnPos+21);
		writeBytes(columnName);
		seek(raColumnPos+41);
		int serialCode = ut.getSerialCode(data_type);
		writeShort(serialCode);
		seek(raColumnPos+43);
		writeBytes(columnKey);
		seek(raColumnPos+63);
		writeByte(ordinal_position);
		seek(raColumnPos+64);
		writeBytes(is_nullable);
		
		noColumns = 7;
	}

	public void insertToCreatedTable(String tableName, String coldetail) throws IOException {
		coldetail = coldetail.substring(0, coldetail.length()-1);
		String cols[] = coldetail.split(",");
		
		for (int i = 0; i < cols.length; i++) {
			seek(1);
			int numberOfRecords = readByte();
			seek(1);
			writeByte(++numberOfRecords);
			++this.noRecords;	
			raColumnPos = raColumnPos - recordLength;
			
			seek(startPosition);
			writeShort(raColumnPos);
			startPosition += 2;
			
			seek(raColumnPos);
			writeByte(numberOfRecords);
			seek(raColumnPos+1);
			writeBytes(tableName);
			seek(raColumnPos+21);
			
			String temp[] = new String[4];			
			String temp1[] = cols[i].split(" ");
			
			for (int j = 0; j < temp1.length; j++) {
				temp[j] = temp1[j];
			}
			
			writeBytes(temp[0]); 
			seek(raColumnPos+41);
			
			int serialCode = ut.getSerialCode(temp[1]);
			writeShort(serialCode);
			seek(raColumnPos+43);
			
			if(temp[2] != null && temp[2].equalsIgnoreCase("PRI"))
				writeBytes(temp[2]);
			else {
				writeBytes("");
				temp[3] = temp[2];
			}
			seek(raColumnPos+63);			
			writeByte(i+1);						 
			seek(raColumnPos+64);
			
			if(temp[3] != null)
				writeBytes(temp[3]); 
			else
				writeBytes("");
		}	
		seek(2);
		writeShort(raColumnPos);
	}

	public void insertRecordIntoTable(String tableName, String[] columnList, String[] columnValues, CreateNew raTable, CreateNew raColumn, BPlusTree btree) throws IOException {
		
		int numberOfRecords;
		if(pagesCount == 1) {
			seek(1);
			numberOfRecords = readByte();
		} else {
			seek(pageSize + (pagesCount-2)*512 + 1);
			numberOfRecords = readByte();
		}
		
		if(((pageSize/2) - ((numberOfRecords*recordLength)+8+(numberOfRecords*2)))> recordLength) {
		} 
		else
		{
			setLength(pageSize + pagesCount*512);

			if(pagesCount == 1) 
			{
				seek(4);
				writeInt(pageSize);				
				seek(pageSize + (pagesCount-1)*512 + 4);
				writeInt(-1);
			} 
			else 
			{
				seek(pageSize + (pagesCount-2)*512 + 4);
				writeInt(pageSize + (pagesCount-1)*512);
				seek(pageSize + (pagesCount-1)*512 + 4);
				writeInt(-1);
			}	
			
			seek(pointer);
			int rootKeyValue = readInt();			
			int end = positionRoot-1;
			positionRoot = end - 4;
			seek(positionRoot);
			writeInt(rootKeyValue);
			seek(512+1);
			write(++noRecRoot);
			pointer = pageSize + pagesCount*512;
			//seek(pos-(countPages*512));
			seek(pageSize+(pagesCount-1)*512);
			writeByte(13);
			pagesCount++;
			//noRecords = 0;			
			startPosition = pageSize + (pagesCount - 2)*512 + 8;
		}
						
		if(pagesCount == 1) {
			raColumn.seek(2);
			int position = raColumn.readShort() + 1;
			raColumn.seek(position);
			String referTableline = raColumn.readLine().substring(0, 20);
			
			int k = 1;
			while(!referTableline.contains(tableName)) {
				position = position + 84*(k);
				raColumn.seek(position);
				referTableline = raColumn.readLine().substring(0, 20);
			}
			
			if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
				boolean flagPrimeKey = checkIfPrimaryKeyIsPresent(columnList, raColumn, position);
				if(!flagPrimeKey){
					System.out.println("Primary key not present");
					return;
				}
			}
			
			if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
				int flagnullablKey = isNullablePresent(columnList, columnValues, raColumn, position);
				if(flagnullablKey == 0){
					System.out.println("Not Nullable Column missing");
					return;
				}else if(flagnullablKey == 2){
					System.out.println("Incorrect value for not nullable column");
					return;
				}
			}
						
			boolean testUniqueness = checkUniquenessPrimary(Integer.parseInt(columnValues[0]));
			if(!testUniqueness) {
				System.out.println("Key is not unique");
				return;
			}			
			seek(1);
			int numbOfRecords = readByte();
			seek(1);
			writeByte(++numbOfRecords);
			++this.noRecords;
			
			for(int i = columnValues.length - 1; i >= 0; i--){				
				raColumn.seek(position + 40);
				String data_type = ut.getDataTypefromCode(raColumn.readShort());
				//System.out.println(data_type);
				
				if(data_type.equalsIgnoreCase("int")){
					pointer = pointer - 4;
					seek(pointer);
					if("null".equalsIgnoreCase(columnValues[i]))
						writeInt(0);
					else
						writeInt((Integer.parseInt(columnValues[i])));
				} 
				else if(data_type.equalsIgnoreCase("tinyint"))
		        {
					pointer = pointer - 1;
					seek(pointer);
					if("null".equalsIgnoreCase(columnValues[i]))
						writeByte(0);
					else
						writeByte((Integer.parseInt(columnValues[i])));					
		        }
		        else if(data_type.equalsIgnoreCase("smallint"))
		        {
		        	pointer = pointer - 2;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeShort(0);
					else
						writeShort((Short.parseShort(columnValues[i])));					
		        }
		        else if(data_type.equalsIgnoreCase("bigint"))
		        {
		        	pointer = pointer - 8;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeLong(0);
					else
						writeLong((Long.parseLong(columnValues[i])));					
		        }
		        else if(data_type.equalsIgnoreCase("real"))
		        {
		        	pointer = pointer - 4;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
						writeFloat(0);
					else
						writeFloat((Float.parseFloat(columnValues[i])));					
		        }
		        else if(data_type.equalsIgnoreCase("double"))
		        {
		        	pointer = pointer - 8;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeDouble(0);
					else
						writeDouble((Double.parseDouble(columnValues[i])));
		        }
		        else if(data_type.equalsIgnoreCase("datetime"))
		        {
		        	pointer = pointer - 8;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeLong(0);
		        	else {
		        		String dateParams[] = columnValues[i].split("-");
		        		ZoneId zoneId = ZoneId.of( "America/Chicago");
		        		
		        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		        		/* ZonedDateTime toLocalDate() method will display in a simple format */
		        		//System.out.println(zdt.toLocalDate()); 

		        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        		writeLong ( epochSeconds );	        		
		        	}				
		        }
		        else if(data_type.equalsIgnoreCase("date"))
		        {
		        	pointer = pointer - 8;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeLong(0);
		        	else {
		        		String dateParams[] = columnValues[i].split("-");
		        		ZoneId zoneId = ZoneId.of( "America/Chicago");
		        		
		        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
		        		//System.out.println(zdt.toLocalDate()); 

		        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        		writeLong ( epochSeconds );		        		
		        	}					
		        }
		        else if(data_type.equalsIgnoreCase("text"))
		        {
		        	pointer = pointer - 20;
		        	seek(pointer);
		        	if("null".equalsIgnoreCase(columnValues[i]))
		        		writeBytes("");
		        	else
		        		writeBytes(columnValues[i]);					
		        }
				position = position + 84;			
				}
				seek(2);
				writeShort(pointer);				
				seek(startPosition);
				writeShort(pointer);
				startPosition += 2;
				}
			else 
			{
				raColumn.seek(2);
				int position = raColumn.readShort() + 1; 
				raColumn.seek(position);
				String referTableline = raColumn.readLine().substring(0, 20);

				int k = 1;
				while(!(referTableline.contains(tableName))) {
					position = position + 84*(k);
					raColumn.seek(position);
					referTableline = raColumn.readLine().substring(0, 20);
				}
				
				if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
					boolean flagPrimeKey = checkIfPrimaryKeyIsPresent(columnList, raColumn, position);
					if(!flagPrimeKey){
						System.out.println("Primary key not present");
						return;
					}
				}
				
				if(!( (columnList.length == 1) && (columnList[0].equals("")) )) {
					int flagNullabKey = isNullablePresent(columnList, columnValues, raColumn, position);
					if(flagNullabKey == 0){
						System.out.println("Not Nullable Column missing");
						return;
					}else if(flagNullabKey == 2){
						System.out.println("Incorrect value for not nullable column");
						return;
					}
				}
								
				boolean testUniqueness = checkUniquenessPrimary(Integer.parseInt(columnValues[0]));
				if(!testUniqueness) {
					System.out.println("Key is not unique");
					return;
				}
				
				seek(pageSize + (pagesCount - 2)*512 + 1);
				int n = readByte();
				seek(pageSize + (pagesCount - 2)*512 + 1);
				writeByte(++n);
				
				for(int i = columnValues.length - 1; i >= 0; i--){										
					raColumn.seek(position + 40);
					String data_type = ut.getDataTypefromCode(raColumn.readShort());
					//System.out.println(data_type);
					
					if(data_type.equalsIgnoreCase("int")){
						pointer = pointer - 4;
						seek(pointer);
						if("null".equalsIgnoreCase(columnValues[i]))
							writeInt(0);
						else
							writeInt((Integer.parseInt(columnValues[i])));
					} 
					else if(data_type.equalsIgnoreCase("tinyint"))
			        {
						pointer = pointer - 1;
						seek(pointer);
						if("null".equalsIgnoreCase(columnValues[i]))
							writeByte(0);
						else
							writeByte((Integer.parseInt(columnValues[i])));						
			        }
			        else if(data_type.equalsIgnoreCase("smallint"))
			        {
			        	pointer = pointer - 2;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeShort(0);
						else
							writeShort((Short.parseShort(columnValues[i])));						
			        }
			        else if(data_type.equalsIgnoreCase("bigint"))
			        {
			        	pointer = pointer - 8;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeLong(0);
						else
							writeLong((Long.parseLong(columnValues[i])));						
			        }
			        else if(data_type.equalsIgnoreCase("real"))
			        {
			        	pointer = pointer - 4;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
							writeFloat(0);
						else
							writeFloat((Float.parseFloat(columnValues[i])));
						
			        }
			        else if(data_type.equalsIgnoreCase("double"))
			        {
			        	pointer = pointer - 8;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeDouble(0);
						else
							writeDouble((Double.parseDouble(columnValues[i])));
			        }
			        else if(data_type.equalsIgnoreCase("datetime"))
			        {
			        	pointer = pointer - 8;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);						
			        }
			        else if(data_type.equalsIgnoreCase("date"))
			        {
			        	pointer = pointer - 8;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);						
			        }
			        else if(data_type.equalsIgnoreCase("text"))
			        {
			        	pointer = pointer - 20;
			        	seek(pointer);
			        	if("null".equalsIgnoreCase(columnValues[i]))
			        		writeBytes("");
			        	else
			        		writeBytes(columnValues[i]);						
			        }
					position = position + 84;					
				}
				seek(pageSize + (pagesCount - 2)*512 + 2);
				writeShort(pointer);
				seek(startPosition);
				writeShort(pointer);
				startPosition += 2;
			}			
		} 

	private int isNullablePresent(String[] columnList, String[] columnValues, CreateNew raColumns, int position) throws IOException {
		
		for (int i = noColumns-1; i >= 0; i--) {
			int status = 0;
			raColumns.seek(position + 63);
			String refernullcol = null;
			String referKey = raColumns.readLine().substring(0, 20);
			
			if(referKey != null && referKey.contains("no")){
				raColumns.seek(position + 20);
				refernullcol = raColumns.readLine().substring(0, 20);
							
				for (int j = 0; j < columnList.length; j++) {
					if(refernullcol != null && refernullcol.contains(columnList[j])) {
						status = 1;
						if("null".equalsIgnoreCase(columnValues[j])) {
							System.out.println("Null value is present for not nullable key");
							status = 2;
						}								
					}
				}
				if(status == 0 || status == 2){
					return status;
				}						
			}
			position = position + 84;
		}
		return 1;
	}	


	private boolean checkIfPrimaryKeyIsPresent(String[] columnList, CreateNew raColumn, int position) throws IOException {
		boolean flag = false;
		for (int i = noColumns-1; i >= 0; i--) {
			raColumn.seek(position + 42);
			String referPricol = null;
			String referKey = raColumn.readLine().substring(0, 20);
			
			if(referKey != null && referKey.contains("pri")){
				raColumn.seek(position + 20);
				referPricol = raColumn.readLine().substring(0, 20);
				
				for (int j = 0; j < columnList.length; j++) {
					if(referPricol != null && referPricol.contains(columnList[j])) {
						return true;
					}
				}						
			}
			position = position + 84;
		}
		return flag;
	}
	
	private boolean checkUniquenessPrimary(Integer Val) throws IOException {
		int page = 1;
		while(page <= pagesCount) {
			if(page == 1) {
				seek(1);
				int numRecs = readByte();
				seek(8);
				int checkStart = 0;
				if(numRecs > 0) {
					checkStart = readShort();
				}
				int noIteration = numRecs;
				while(noIteration != 0) {
					seek(checkStart);
					if(readInt() == Val){
						return false;
					}
					noIteration--;
					checkStart = checkStart - recordLength;
				}				
			} 
			else 
			{
				seek(pageSize + (page - 2)*512 + 1);
				int numRecs = readByte();
				seek(pageSize + (page - 2)*512 + 8);
				int checkStart = 0;
				if(numRecs > 0) {
					checkStart = readShort();
				}
				int noIteration = numRecs;
				while(noIteration != 0) {
					seek(checkStart);
					if(readInt() == Val){
						return false;
					}
					noIteration--;
					checkStart = checkStart - recordLength;
				}			
			}
			page++;
		}		
		return true;		
	}

	public void queryFromTable(String tableName, String wildCard, String checkingColumn, String operator, String valueToCompare, CreateNew raTable, CreateNew raColumn) throws IOException {

		raColumn.seek(2);
		int position = raColumn.readShort() + 1; 
		raColumn.seek(position);
		String TableLine = raColumn.readLine().substring(0, 20);		
		//System.out.println("Our table: " + tableName);
		//System.out.println("Reference Table name: " + TableLine);
		
		int k = 1;
		while(!TableLine.contains(tableName)) { //readLine gives us the table name	
			position = position + 84*(k);
			raColumn.seek(position);
			TableLine = raColumn.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + TableLine);
			//k++;
		}
		
		int pageProcess = 1;
		while(pageProcess <= pagesCount) {
			if(pageProcess == 1) 
			{
				seek(1);
				int numRecs = readByte();
				selectHelp(pageProcess, numRecs, wildCard, checkingColumn, operator, valueToCompare, raTable, raColumn, position);
			}
			else
			{
				seek(pageSize + (pageProcess - 2)*512 + 1);
				int numRecs = readByte();
				selectHelp(pageProcess, numRecs, wildCard, checkingColumn, operator, valueToCompare, raTable, raColumn, position);
			}
			pageProcess++;
		}		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void selectHelp(int processPage, int numRecs, String wildCard, String checkingColumn, String operator, String valueToCompare, CreateNew raTable, CreateNew raColumn, int position) throws IOException {
				
				ArrayList[] list = new ArrayList[noColumns];
				String data_type = null;
				int recStartPos = 8;
				int noBytes = 0, sum = 0;								
				String columnNames[] = new String[noColumns];
				Map<String, Integer> mapOrdinalPos = new HashMap<String, Integer>();
				Map<String, String> mapDataTypes = new HashMap<String, String>();
				
				for (int i = noColumns-1; i >= 0; i--) {
					raColumn.seek(position+20);
					String columnName = raColumn.readLine().substring(0, 20).trim();
					columnNames[i] = columnName;

					raColumn.seek(position+62);
					int ordPos = raColumn.readByte();
					mapOrdinalPos.put(columnName, ordPos);

					raColumn.seek(position + 40);
					data_type = ut.getDataTypefromCode(raColumn.readShort());
					//System.out.println(data_type);

					mapDataTypes.put(columnName, data_type);
					list[i] = getArrayList(data_type);
					int temp = numRecs;	
					noBytes = ut.getRecLenAccordingToData(data_type);
					sum = sum + noBytes;
					
					int start;
					if(processPage == 1) {
						seek(recStartPos);
						start = readShort() + recordLength - sum; 
						seek(start);
					} else {
						seek(pageSize + (processPage - 2)*512 + recStartPos);
						start = readShort() + recordLength - sum; 	
						seek(start);
					}
					
					while(temp != 0) {
						
						if(data_type.equalsIgnoreCase("int"))
						{
							list[i].add(readInt());
						} 
						else if(data_type.equalsIgnoreCase("tinyint"))
				        {
							list[i].add(readByte());					
				        }
				        else if(data_type.equalsIgnoreCase("smallint"))
				        {
				        	list[i].add(readShort());
				        }
				        else if(data_type.equalsIgnoreCase("bigint"))
				        {
				        	list[i].add(readLong());
				        }
				        else if(data_type.equalsIgnoreCase("real"))
				        {
				        	list[i].add(readFloat());
				        }
				        else if(data_type.equalsIgnoreCase("double"))
				        {
				        	list[i].add(readDouble());
				        }
				        else if(data_type.equalsIgnoreCase("datetime"))
				        {	
				        	
				        	list[i].add(readLong());
				        }
				        else if(data_type.equalsIgnoreCase("date"))
				        {
				        	list[i].add(readLong());
						 }
				        else if(data_type.equalsIgnoreCase("text"))
				        {
				        	list[i].add(readLine().substring(0, 20).trim());
				        }
						
						start = start - recordLength;
						seek(start);
						temp--;
					}
					position = position + 84;	
				}

				ArrayList<Integer> index = new ArrayList<>();
				if(checkingColumn != null) {
					int checkingordinalPos = mapOrdinalPos.get(checkingColumn);
					String checkingdataType = mapDataTypes.get(checkingColumn);
					ArrayList deciding_List = list[checkingordinalPos-1];
					for (int j = 0; j < deciding_List.size(); j++) {
						if("<".equals(operator)) 
						{
							if(checkingdataType.equalsIgnoreCase("int")) 
							{
								if((Integer)deciding_List.get(j) < Integer.parseInt(valueToCompare)){
									index.add(j);
								}
							} 
							else if(checkingdataType.equalsIgnoreCase("tinyint"))
					        {
								if((Byte)deciding_List.get(j) < Byte.parseByte(valueToCompare)){
									index.add(j);
								}					
					        }
					        else if(checkingdataType.equalsIgnoreCase("smallint"))
					        {
					        	if((Short)deciding_List.get(j) < Short.parseShort(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("bigint"))
					        {
					        	if((Long)deciding_List.get(j) < Long.parseLong(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("real"))
					        {
					        	if((Float)deciding_List.get(j) < Float.parseFloat(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("double"))
					        {
					        	if((Double)deciding_List.get(j) < Double.parseDouble(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("datetime"))
					        {	
					        	String dateParams[] = valueToCompare.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		//System.out.println(zdt.toLocalDate()); 

				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) < epochSeconds){
									index.add(j);
								}
					        } 
					        else if(checkingdataType.equalsIgnoreCase("date")) 
					        {
					        	String dateParams[] = valueToCompare.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");

				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		//System.out.println(zdt.toLocalDate()); 

				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) < epochSeconds){
									index.add(j);
								}
					        }				
						} 
						else if(">".equals(operator))
						{
							if(checkingdataType.equalsIgnoreCase("int")) 
							{
								if((Integer)deciding_List.get(j) > Integer.parseInt(valueToCompare)){
									index.add(j);
								}
							} 
							else if(checkingdataType.equalsIgnoreCase("tinyint"))
					        {
								if((Byte)deciding_List.get(j) > Byte.parseByte(valueToCompare)){
									index.add(j);
								}					
					        }
					        else if(checkingdataType.equalsIgnoreCase("smallint"))
					        {
					        	if((Short)deciding_List.get(j) > Short.parseShort(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("bigint"))
					        {
					        	if((Long)deciding_List.get(j) > Long.parseLong(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("real"))
					        {
					        	if((Float)deciding_List.get(j) > Float.parseFloat(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("double"))
					        {
					        	if((Double)deciding_List.get(j) > Double.parseDouble(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("datetime"))
					        {	
					        	String dateParams[] = valueToCompare.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		//System.out.println(zdt.toLocalDate()); 

				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) > epochSeconds){
									index.add(j);
								}
					        } 
					        else if(checkingdataType.equalsIgnoreCase("date")) 
					        {
					        	String dateParams[] = valueToCompare.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");

				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		//System.out.println(zdt.toLocalDate()); 

				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) > epochSeconds){
									index.add(j);
								}
					        }
						}
						else 
						{ 
							if(checkingdataType.equalsIgnoreCase("int")) 
							{
								if((Integer)deciding_List.get(j) == Integer.parseInt(valueToCompare)){
									index.add(j);
								}
							} 
							else if(checkingdataType.equalsIgnoreCase("tinyint"))
					        {
								if((Byte)deciding_List.get(j) == Byte.parseByte(valueToCompare)){
									index.add(j);
								}					
					        }
					        else if(checkingdataType.equalsIgnoreCase("smallint"))
					        {
					        	if((Short)deciding_List.get(j) == Short.parseShort(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("bigint"))
					        {
					        	if((Long)deciding_List.get(j) == Long.parseLong(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("real"))
					        {
					        	if((Float)deciding_List.get(j) == Float.parseFloat(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("double"))
					        {
					        	if((Double)deciding_List.get(j) == Double.parseDouble(valueToCompare)){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("datetime"))
					        {	
					        	String dateParams[] = valueToCompare.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
				        		
				        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
				        		//System.out.println(zdt.toLocalDate()); 

				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) == epochSeconds){
									index.add(j);
								}
					        } 
					        else if(checkingdataType.equalsIgnoreCase("date")) 
					        {
					        	String dateParams[] = valueToCompare.split("-");
					        	ZoneId zoneId = ZoneId.of( "America/Chicago");
					        	ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );

				        		//System.out.println(zdt.toLocalDate()); 
				        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
					        	
					        	if((Long)deciding_List.get(j) == epochSeconds){
									index.add(j);
								}
					        }
					        else if(checkingdataType.equalsIgnoreCase("text")) {
								if(((String)deciding_List.get(j)).equals(valueToCompare)){
									index.add(j);
								}
							}
						}
					}				
				} 
				
				if(!("*".equals(wildCard))) {					
					String selectColnames[] = wildCard.split(",");
					if(processPage == 1) {
						for (int j = 0; j < selectColnames.length; j++) {
							System.out.print(selectColnames[j] + " ");
						}
						System.out.println();
					}
										
					if(checkingColumn != null) {						
						int m = 0;
						while(m < index.size()){
							for (int z = 0; z < selectColnames.length; z++) {
								int temp = mapOrdinalPos.get(selectColnames[z]) - 1;
								if(mapDataTypes.get(selectColnames[z]).equalsIgnoreCase("datetime") || mapDataTypes.get(selectColnames[z]).equalsIgnoreCase("date"))
								{
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	long retreivedEpochSeconds = (long) list[temp].get(index.get(m));
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );

									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[temp].get(index.get(m)) + " ");
								}									
							}
							System.out.println();
							m++;
						}
					} 
					else 
					  {						
						int m = 0;
						while(m < numRecs){
							for (int z = 0; z < selectColnames.length; z++) {
								int temp = mapOrdinalPos.get(selectColnames[z]) - 1;
								if(mapDataTypes.get(selectColnames[z]).equalsIgnoreCase("datetime") || mapDataTypes.get(selectColnames[z]).equalsIgnoreCase("date"))
								{
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	long retreivedEpochSeconds = (long) list[temp].get(m);
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );

									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[temp].get(m) + " ");
								}							
							}
							System.out.println();
							m++;
						}
					}
				} 
				else 
				{	
					if(processPage == 1) {
						for (int j = 0; j < columnNames.length; j++) {
							System.out.print(columnNames[j] + " ");
						}
						System.out.println();
					}

					if (checkingColumn != null) {
						int m = 0;
						while (m < index.size()) {
							for (int z = 0; z < columnNames.length; z++) {								
								if(mapDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
								{
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );

						        	long retreivedEpochSeconds = (long)list[z].get(index.get(m));
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );
									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[z].get(index.get(m)) + " ");
								}								
							}
							System.out.println();
							m++;
						}
					} 
					else 
					{
						int m = 0;
						while (m < numRecs) {
							for (int z = 0; z < list.length; z++) {
								if(mapDataTypes.get(columnNames[z]).equalsIgnoreCase("datetime") || mapDataTypes.get(columnNames[z]).equalsIgnoreCase("date"))
								{
						        	ZoneId zoneId = ZoneId.of ( "America/Chicago" );
						        	//System.out.println(list[z].get(m));
						        	long retreivedEpochSeconds = (long) list[z].get(m);
						        	Instant a = Instant.ofEpochSecond (retreivedEpochSeconds); 
						        	ZonedDateTime zdt2 = ZonedDateTime.ofInstant ( a, zoneId );

									System.out.print(zdt2.toLocalDate() + " ");
								}
								else
								{
									System.out.print(list[z].get(m) + " ");
								}
							}
							System.out.println();
							m++;
						}
					}		
				}
	}
		
	@SuppressWarnings("rawtypes")
	private ArrayList getArrayList(String data_type) {
		if(data_type.equalsIgnoreCase("int")){
			return new ArrayList<Integer>();
		} 
		else if(data_type.equalsIgnoreCase("tinyint"))
        {			
			return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
        	return new ArrayList<Integer>();
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
        	return new ArrayList<Float>();			
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
        	return new ArrayList<Double>();
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
        	return new ArrayList<Long>();			
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
        	return new ArrayList<Long>();			
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
        	return new ArrayList<String>();
        } 
        else {
        	return null;
        }
	}

	public void processShowTableQuery() throws IOException {
		//ArrayList<String> listOfTables = new ArrayList<>();
		seek(8);
		int start = readShort() + 1;
		seek(1);
		int count = readByte();
		
		while(count != 0) {
			seek(start);
			//listOfTables.add(readLine().substring(0, 20).trim());
			System.out.println(readLine().substring(0, 20).trim());
			start = start - recordLength;
			count--;
		}
	}

	public void deleteFromTable(String tableName, String checkingColumn, String operator, String valueToCompare, CreateNew raTable, CreateNew raColumn) throws IOException {
		if (checkingColumn != null) {
			raColumn.seek(2);
			int position = raColumn.readShort() + 1; 
			raColumn.seek(position);
			String referTableline = raColumn.readLine().substring(0, 20);
			//System.out.println("Our table: " + tableName);
			//System.out.println("Reference Table name: " + referenceTableLine);
			
			int k = 1;
			while (!referTableline.contains(tableName)) { 
				position = position + 84 * (k);
				raColumn.seek(position);
				referTableline = raColumn.readLine().substring(0, 20);
				//System.out.println("Inside Reference Table name: "+ referenceTableLine);
				//k++;
			}
			
			String data_type = null;
			String colnames[] = new String[noColumns];
			Map<String, Integer> mapOrdinalPos = new HashMap<String, Integer>();
			// Map<String, String> mapOfDataTypes = new HashMap<String,String>();
			Map<Integer, String> mapDatatype = new HashMap<Integer, String>();
			for (int i = noColumns - 1; i >= 0; i--) {

				raColumn.seek(position + 20);
				String columnName = raColumn.readLine().substring(0, 20).trim();
				colnames[i] = columnName;

				raColumn.seek(position + 62);
				int ordinalPosition = raColumn.readByte();

				mapOrdinalPos.put(columnName, ordinalPosition);

				raColumn.seek(position + 40);
				data_type = ut.getDataTypefromCode(raColumn.readShort());
				//System.out.println(data_type);

				mapDatatype.put(ordinalPosition, data_type);
				position = position + 84;
			}
			int checkingOrdinalPos = mapOrdinalPos.get(checkingColumn);
			String checkingDataType = mapDatatype.get(checkingOrdinalPos);
			int length = 0;
			for (int i = 1; i < checkingOrdinalPos; i++) {
				length += ut.getRecLenAccordingToData(mapDatatype.get(i));
				//System.out.println("Length: " + length);
			}
			
			int processPage = 1;
			while(processPage <= pagesCount) {
				if(processPage == 1) 
				{
					seek(1);
					int numRecs = readByte();
					deleteHelp(processPage, numRecs, length, checkingColumn, checkingDataType, operator, valueToCompare, mapDatatype, mapOrdinalPos);
				}
				else
				{
					seek(pageSize + (processPage - 2)*512 + 1);
					int numberOfRecords = readByte();
					deleteHelp(processPage, numberOfRecords, length, checkingColumn, checkingDataType, operator, valueToCompare, mapDatatype, mapOrdinalPos);
				}
				processPage++;
			}
		} 
		else 
		{
			int end = pageSize + (pagesCount - 1)*512;
			for (int i = 1; i < end; i++) {
				seek(i);
				writeByte(0);
			}
			startPosition = 8;
			noRecords = 0;
			setLength(pageSize);
			pointer = 512;
			pagesCount = 1;
		}
		System.out.println("Delete successful");
	}

	public void deleteHelp(int processPage, int numRecs, int length, String checkingCol, String checkingDatatype, String operator, String valueToCompare, Map<Integer, String> mapDataTypes, Map<String, Integer> mapOrdinalPos) throws IOException {
		int recStart = 8;
		int start;
		if(processPage == 1) {
			seek(recStart);
			start = readShort();
			seek(start);
		} else {
			seek(pageSize + (processPage - 2)*512 + recStart);
			start = readShort();
			seek(start);
		}
		
		//boolean flag = false;
		for (int i = 0; i < numRecs; i++) {
			boolean flag = false;
			seek(start + length);
			if ("<".equals(operator)) {
				if(checkingDatatype.equalsIgnoreCase("int"))
				{
					if (readInt() < Integer.parseInt(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}				
				}
				else if(checkingDatatype.equalsIgnoreCase("tinyint"))
		        {			
					if (readByte() < Byte.parseByte(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("smallint"))
		        {
		        	if (readShort() < Short.parseShort(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("bigint"))
		        {
		        	if (readLong() < Long.parseLong(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("real"))
		        {
		        	if (readFloat() < Float.parseFloat(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}			
		        }
		        else if(checkingDatatype.equalsIgnoreCase("double"))
		        {
		        	if (readDouble() < Double.parseDouble(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("datetime"))
		        {
		        	String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() < epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}		
		        }
		        else if(checkingDatatype.equalsIgnoreCase("date"))
		        {
		        	String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() < epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}					
		        }
		        				
			} 
			else if (">".equals(operator)) 
			{
				if(checkingDatatype.equalsIgnoreCase("int"))
				{
					if (readInt() > Integer.parseInt(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}				
				}
				else if(checkingDatatype.equalsIgnoreCase("tinyint"))
		        {			
					if (readByte() > Byte.parseByte(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("smallint"))
		        {
		        	if (readShort() > Short.parseShort(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("bigint"))
		        {
		        	if (readLong() > Long.parseLong(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("real"))
		        {
		        	if (readFloat() > Float.parseFloat(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}			
		        }
		        else if(checkingDatatype.equalsIgnoreCase("double"))
		        {
		        	if (readDouble() > Double.parseDouble(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("datetime"))
		        {
		        	String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() > epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}		   		
		        }
		        else if(checkingDatatype.equalsIgnoreCase("date"))
		        {
		        	String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() > epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}	    			
		      }
			}			
			else 
			{ 
				if(checkingDatatype.equalsIgnoreCase("int"))
				{
					if (readInt() == Integer.parseInt(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}				
				}
				else if(checkingDatatype.equalsIgnoreCase("tinyint"))
		        {			
					if (readByte() == Byte.parseByte(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("smallint"))
		        {
		        	if (readShort() == Short.parseShort(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("bigint"))
		        {
		        	if (readLong() == Long.parseLong(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("real"))
		        {
		        	if (readFloat() == Float.parseFloat(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}			
		        }
		        else if(checkingDatatype.equalsIgnoreCase("double"))
		        {
		        	if (readDouble() == Double.parseDouble(valueToCompare)) 
					{
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}
		        }
		        else if(checkingDatatype.equalsIgnoreCase("datetime"))
		        {
		        	String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() == epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}							        		
		        }
		        else if(checkingDatatype.equalsIgnoreCase("date"))
		        {
		        	String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
		        	
		        	if(readLong() == epochSeconds)
		        	{
		        		makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);	
						flag = true;
						startPosition = startPosition - 2;
					}						        		
		        } 
		        else if (checkingDatatype.equalsIgnoreCase("text")) 
		        {
					String stringToBecompared = readLine().substring(0, 20).trim();
					if (stringToBecompared.equals(valueToCompare)) {
						makeRecordZero(start, start + recordLength, mapDataTypes, mapOrdinalPos, processPage);
						flag = true;
						startPosition = startPosition - 2;
					}
				}
			}
			if(!flag) {
				start = start - recordLength;
			} else {
				i--;
			}
		}
	}

	public void makeRecordZero(int start, int end, Map<Integer, String> mapDataType, Map<String, Integer> mapOrdinalPos, int processPage) throws IOException {
		for (int j = start; j < end; j++) {
			seek(j);
			writeByte(0);
		}

		int addr = pointer;
		for (int i = 1; i <= noColumns; i++) {
			seek(addr);
			if(mapDataType.get(i).equalsIgnoreCase("int")){
				int temp = readInt();
				seek(start);
				writeInt(temp);
				start = start + 4;
				addr = addr + 4;
			}else if(mapDataType.get(i).equalsIgnoreCase("byte"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				addr = addr + 1;
	        }
			else if(mapDataType.get(i).equalsIgnoreCase("tinyint"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				addr = addr + 1;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("smallint"))
	        {
	        	int temp = readShort();
				seek(start);
				writeInt(temp);
				start = start + 2;
				addr = addr + 2;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("bigint"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				addr = addr + 8;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("real"))
	        {
	        	float temp = readFloat();
				seek(start);
				writeFloat(temp);
				start = start + 4;
				addr = addr + 4;		
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("double"))
	        {
	        	double temp = readDouble();
				seek(start);
				writeDouble(temp);
				start = start + 8;
				addr = addr + 8;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("datetime"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				addr = addr + 20;		
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("date"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				addr = addr + 20;			
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("text"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				addr = addr + 20;
	        } 
		}
		
		int s = pointer;
		for (int j = s; j < (pointer+recordLength); j++) {
			seek(j);
			writeByte(0);
		}
		
		int pageCurr = (int)Math.floor(pointer/512);
		int recStart = 8;
		
		if(pageCurr == 0) {
			seek(1);
			int numRecs = readByte();
			seek(recStart + (numRecs-1)*2);
			writeShort(0);

			seek(1);
			int updatedNumRecs = readByte();
			seek(1);
			writeByte(updatedNumRecs-1);

			pointer = pointer + recordLength;
			seek(2);
			writeShort(pointer);
			
		} 
		else {

			if((pointer + recordLength) >= (pageSize + (pageCurr - 2)*512 + 512)) {
				if(pageCurr == 2) {
					seek(2);
					pointer = readShort();
					seek(4);
					writeInt(0);					
					setLength(pageSize);
					pagesCount--;
				}
				else {
					seek((pageCurr - 1)* 512 + 2);
					pointer = readShort();

					seek(pageSize + (pageCurr - 2)*512 + 4);
					writeInt(0);
					
					setLength(pageSize + (pageCurr - 2)*512);
					pagesCount--;
				}
			}
			else {
				pointer = pointer + recordLength;	
				seek(pageSize + (pageCurr - 2)*512 + 2);
				writeShort(pointer);

				seek(pageSize + (pageCurr - 2)*512 + 1);
				int n = readByte();
				seek(pageSize + (pageCurr - 2)*512 + recStart + (n-1)*2);
				writeShort(0);

				seek(pageSize + (pageCurr - 2)*512 + 1);
				int updatedNumRecs = readByte();
				seek(pageSize + (pageCurr - 2)*512 + 1);
				writeByte(updatedNumRecs-1);
			}
		}		
		this.noRecords = findNumRecs(pagesCount);
	}

	public int findNumRecs(int PagesC) throws IOException {
		int c = 1;
		int numRecs = 0;
		while(c <= PagesC) {
			if(c == 1) {
				seek(1);
				numRecs += readByte();
			} else {
				seek(pageSize + (c - 2)*512  + 1);
				numRecs += readByte();
			}
			c++;
		}
		return numRecs;
	}

	private void doRecZeroDrop(int start, int end, Map<Integer, String> mapDataType, Map<String, Integer> mapOrdinalPos) throws IOException {
		for (int j = start; j < end; j++) {
			seek(j);
			writeByte(0);
		}
		seek(2);
		int addr = readShort();
		for (int i = 1; i <= noColumns; i++) {
			seek(addr);
			if(mapDataType.get(i).equalsIgnoreCase("int")){
				int temp = readInt();
				seek(start);
				writeInt(temp);
				start = start + 4;
				addr = addr + 4;
			}else if(mapDataType.get(i).equalsIgnoreCase("byte"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				addr = addr + 1;
	        }
			else if(mapDataType.get(i).equalsIgnoreCase("tinyint"))
	        {			
				int temp = readByte();
				seek(start);
				writeByte(temp);
				start = start + 1;
				addr = addr + 1;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("smallint"))
	        {
	        	int temp = readShort();
				seek(start);
				writeInt(temp);
				start = start + 2;
				addr = addr + 2;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("bigint"))
	        {
	        	long temp = readLong();
				seek(start);
				writeLong(temp);
				start = start + 8;
				addr = addr + 8;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("real"))
	        {
	        	float temp = readFloat();
				seek(start);
				writeFloat(temp);
				start = start + 4;
				addr = addr + 4;		
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("double"))
	        {
	        	double temp = readDouble();
				seek(start);
				writeDouble(temp);
				start = start + 8;
				addr = addr + 8;
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("datetime"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				addr = addr + 20;		
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("date"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				addr = addr + 20;			
	        }
	        else if(mapDataType.get(i).equalsIgnoreCase("text"))
	        {
	        	String temp = readLine().substring(0, 20);
				seek(start);
				writeBytes(temp);
				start = start + 20;
				addr = addr + 20;
	        } 
		}

		seek(8 + (noRecords-1)*2);
		writeShort(0);
		seek(1);
		int updatedNumRecs = readByte();
		seek(1);
		writeByte(updatedNumRecs-1);
		this.noRecords = updatedNumRecs-1;
		seek(2);
		int add = readShort();
		for (int j = add; j <= (add + recordLength); j++) {
			seek(j);
			writeByte(0);
		}	
		seek(2);
		int u = readShort() + recordLength;
		seek(2);
		writeShort(u);
	}
		
	public void processDropString(String tableName, CreateNew raTable, CreateNew raColumn) throws IOException {
		raTable.seek(2);
		int raTablePos = raTable.readShort() + 1; 
		raTable.seek(raTablePos);
		String raTableReferTableline = raTable.readLine().substring(0, 20);
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfTbl Reference Table name: " + rfTblReferenceTableLine);
		
		int m = 1;
		while(!raTableReferTableline.contains(tableName)) { //readLine gives us the table name	
			raTablePos = raTablePos + 21*(m);
			raTable.seek(raTablePos);
			raTableReferTableline = raTable.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfTblReferenceTableLine);
			//m++;
		}
		Map<String, Integer> raTblMapOrdinalPos = new HashMap<String, Integer>();
		raTblMapOrdinalPos.put("rowid", 1);
		raTblMapOrdinalPos.put("table_name", 2);
		//Map<String, String> mapOfDataTypes = new HashMap<String,String>();
		Map<Integer, String> raTableMapDataType = new HashMap<Integer, String>();
		raTableMapDataType.put(1, "byte");
		raTableMapDataType.put(2, "text");
		raTable.doRecZeroDrop(raTablePos - 1, (raTablePos -1 + raTable.recordLength), raTableMapDataType, raTblMapOrdinalPos);
		raTable.startPosition = raTable.startPosition - 2;
		raTable.raTablePos = raTable.raTablePos + raTable.recordLength;

		raColumn.seek(2);
		int raColPos = raColumn.readShort() + 1;
		raColumn.seek(raColPos);
		String raColReferTableline = raColumn.readLine().substring(0, 20);		
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfCol Reference Table name: " + rfColReferenceTableLine);
		
		int k = 1;
		while(!raColReferTableline.contains(tableName)) { //readLine gives us the table name	
			raColPos = raColPos + 84*(k);
			raColumn.seek(raColPos);
			raColReferTableline = raColumn.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfColReferenceTableLine);
			//k++;
		}
		Map<String, Integer> raColMapOrdinalPos = new HashMap<String, Integer>();
		raColMapOrdinalPos.put("rowid", 1);
		raColMapOrdinalPos.put("table_name", 2);
		raColMapOrdinalPos.put("column_name", 3);
		raColMapOrdinalPos.put("data_type", 4);
		raColMapOrdinalPos.put("column_key", 5);
		raColMapOrdinalPos.put("ordinal_position", 6);
		raColMapOrdinalPos.put("is_nullable", 7);
		//Map<String, String> mapOfDataTypes = new HashMap<String,String>();
		Map<Integer, String> raColMapDataType = new HashMap<Integer, String>();
		raColMapDataType.put(1, "BYTE");
		raColMapDataType.put(2, "TEXT");
		raColMapDataType.put(3, "TEXT");
		raColMapDataType.put(4, "SMALLINT");
		raColMapDataType.put(5, "TEXT");
		raColMapDataType.put(6, "TINYINT");
		raColMapDataType.put(7, "TEXT");
		
		for (int i = 0; i < noColumns; i++) {
			raColumn.doRecZeroDrop(raColPos - 1, (raColPos - 1 + raColumn.recordLength), raColMapDataType, raColMapOrdinalPos);
			raColPos = raColPos + 84;
			raColumn.startPosition = raColumn.startPosition - 2;
			raColumn.raColumnPos = raColumn.raColumnPos + raColumn.recordLength;
		}	
		System.out.println("Drop successful");
	}

	public void processUpdateString(String tableName, String colToUpdate, String valueToSet, String checkingColumn, String operator, String valueToCompare, CreateNew raTable, CreateNew raColumn) throws IOException {
		raColumn.seek(2);
		int raColPos = raColumn.readShort() + 1; // this position will give us the table name
		raColumn.seek(raColPos);
		String raColReferTableline = raColumn.readLine().substring(0, 20);
		//System.out.println("Our table: " + tableName);
		//System.out.println("RfCol Reference Table name: " + rfColReferenceTableLine);
		
		int k = 1;
		while(!(raColReferTableline.contains(tableName))) { //readLine gives us the table name	
			raColPos = raColPos + 84*(k);
			raColumn.seek(raColPos);
			raColReferTableline = raColumn.readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + rfColReferenceTableLine);
			//k++;
		}
		
		String data_type = null;
		String colNames[] = new String[noColumns];
		Map<String, Integer> mapOrdinalPos = new HashMap<String, Integer>();
		Map<Integer, String> mapDataType = new HashMap<Integer, String>();
		for (int i = noColumns - 1; i >= 0; i--) {

			raColumn.seek(raColPos + 20);
			String columnName = raColumn.readLine().substring(0, 20).trim();
			colNames[i] = columnName;

			raColumn.seek(raColPos + 62);
			int ordinalPos = raColumn.readByte();

			mapOrdinalPos.put(columnName, ordinalPos);

			raColumn.seek(raColPos + 40);
			data_type = ut.getDataTypefromCode(raColumn.readShort());
			//System.out.println(data_type);

			mapDataType.put(ordinalPos, data_type);
			raColPos = raColPos + 84;
		}
		
		int colToUpdateOrdinalPos = mapOrdinalPos.get(colToUpdate);
		String colToUpdateDataType = mapDataType.get(colToUpdateOrdinalPos);		
		int lengthUpdate = 0;
		for (int i = 1; i < colToUpdateOrdinalPos; i++) {
			lengthUpdate += ut.getRecLenAccordingToData(mapDataType.get(i));
			//System.out.println("Length: " + lengthUpdate);
		}
		
		if(checkingColumn != null) {
			int checkingOrdinalPos = mapOrdinalPos.get(checkingColumn);
			String checkingDataType = mapDataType.get(checkingOrdinalPos);
			int lenCondition = 0;
			for (int i = 1; i < checkingOrdinalPos; i++) {
				lenCondition += ut.getRecLenAccordingToData(mapDataType.get(i));
				//System.out.println("Length: " + lengthCondition);
			}
						
			int processPage = 1;
			while(processPage <= pagesCount) {
				if(processPage == 1) 
				{
					seek(1);
					int numRecs = readByte();
					helpUpdate(lengthUpdate, lenCondition, processPage, numRecs, checkingColumn, checkingDataType, colToUpdateDataType, operator, valueToCompare, valueToSet, raTable, raColumn);
				}
				else
				{
					seek(pageSize + (processPage - 2)*512 + 1);
					int numberOfRecords = readByte();
					helpUpdate(lengthUpdate, lenCondition, processPage, numberOfRecords, checkingColumn, checkingDataType, colToUpdateDataType, operator, valueToCompare, valueToSet, raTable, raColumn);
				}
				processPage++;
			}				
		}
		else 
		{
			int processPage = 1;
			while(processPage <= pagesCount) {
				if(processPage == 1) 
				{
					seek(8);
					int stUpdate = readShort() + lengthUpdate;
					seek(1);
					int numRecs = readByte();
					for (int j = 0; j < numRecs; j++) {
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valueToSet);
						stUpdate = stUpdate - recordLength;
					}
					
				}
				else
				{
					seek(pageSize + (processPage - 2)*512 + 8);
					int stUpdate = readShort() + lengthUpdate;
					
					seek(pageSize + (processPage - 2)*512 + 1);
					int numberOfRecords = readByte();
					
					for (int j = 0; j < numberOfRecords; j++) {
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valueToSet);
						stUpdate = stUpdate - recordLength;
					}			
				}
				processPage++;
			}		
		}		
	}
	
	private void helpUpdate(int lengthUpdate, int lenCondition, int processPage, int numRecs, String checkingColumn, String checkingDataType, String colToUpdateDataType, String operator,
			String valueToCompare, String valToSet, CreateNew raTable, CreateNew raColumn) throws IOException {

		int recStart = 8;
		int stUpdate,stCondition;
		if(processPage == 1) {
			seek(recStart);
			stUpdate = readShort() + lengthUpdate;
			seek(recStart);
			stCondition = readShort() + lenCondition;
		} else {
			seek(pageSize + (processPage - 2)*512 + recStart);
			stUpdate = readShort() + lengthUpdate;
			seek(pageSize + (processPage - 2)*512 + recStart);
			stCondition = readShort() + lenCondition;
		}
		
		for (int j = 0; j < numRecs; j++) {
			if ("<".equals(operator)) 
			{
				seek(stCondition);
				if(checkingDataType.equalsIgnoreCase("int")) 
				{
					if (readInt() < Integer.parseInt(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() < Byte.parseByte(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() < Byte.parseByte(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() < Short.parseShort(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() < Long.parseLong(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("real")) 
				{
					if (readFloat() < Float.parseFloat(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("datetime"))
				{
					String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");       		
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() < epochSeconds) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("date"))
				{
					String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() < epochSeconds) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
			} 
			else if (">".equals(operator)) 
			{
				seek(stCondition);
				if(checkingDataType.equalsIgnoreCase("int")) 
				{
					if (readInt() > Integer.parseInt(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() > Byte.parseByte(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() > Byte.parseByte(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() > Short.parseShort(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() > Long.parseLong(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("real")) 
				{
					if (readFloat() > Float.parseFloat(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() > Double.parseDouble(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("datetime"))
				{
					String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() > epochSeconds) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("date"))
				{
					String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() > epochSeconds) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
			} 
			else 
			{
				seek(stCondition);
				if(checkingDataType.equalsIgnoreCase("int")) 
				{
					int x = readInt();
					if (x == Integer.parseInt(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("byte")) 
				{
					if (readByte() == Byte.parseByte(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("tinyint")) 
				{
					if (readByte() == Byte.parseByte(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("smallint")) 
				{
					if (readShort() == Short.parseShort(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("bigint")) 
				{
					if (readLong() == Long.parseLong(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("real")) 
				{
					if (readFloat() < Float.parseFloat(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("double")) 
				{
					if (readDouble() < Double.parseDouble(valueToCompare)) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("datetime"))
				{
					String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() == epochSeconds) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if(checkingDataType.equalsIgnoreCase("date"))
				{
					String dateParams[] = valueToCompare.split("-");
		        	ZoneId zoneId = ZoneId.of( "America/Chicago");
	        		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
	        		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
	        		
	        		if (readLong() == epochSeconds) 
					{
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
				else if (checkingDataType.equalsIgnoreCase("text")) 
				{
					String stringToBecompared = readLine().substring(0, 20).trim();
					if (stringToBecompared.equals(valueToCompare)) {
						seek(stUpdate);
						updateval(stUpdate, colToUpdateDataType, valToSet);
					}
				}
			}
			stCondition = stCondition - recordLength;
			stUpdate = stUpdate - recordLength;
		}		
	}

	public void updateval(int stUpdate, String colToUpdateDataType, String valToSet) throws NumberFormatException, IOException {
		if(colToUpdateDataType.equalsIgnoreCase("int"))
		{
			writeInt(Integer.parseInt(valToSet));
		}
		else if(colToUpdateDataType.equalsIgnoreCase("byte"))
        {	
			writeByte(Byte.parseByte(valToSet));
        }
		else if(colToUpdateDataType.equalsIgnoreCase("tinyint"))
        {	
			writeByte(Byte.parseByte(valToSet));
        }
        else if(colToUpdateDataType.equalsIgnoreCase("smallint"))
        {
			writeInt(Short.parseShort(valToSet));
        }
        else if(colToUpdateDataType.equalsIgnoreCase("bigint"))
        {
			writeLong(Long.parseLong(valToSet));
        }
        else if(colToUpdateDataType.equalsIgnoreCase("real"))
        {
			writeFloat(Float.parseFloat(valToSet));		
        }
        else if(colToUpdateDataType.equalsIgnoreCase("double"))
        {
			writeDouble(Double.parseDouble(valToSet));
        }
        else if(colToUpdateDataType.equalsIgnoreCase("datetime"))
        {
        	String dateParams[] = valToSet.split("-");
    		ZoneId zoneId = ZoneId.of( "America/Chicago");
    		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
    		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
    		writeLong ( epochSeconds );			
        }
        else if(colToUpdateDataType.equalsIgnoreCase("date"))
        {
        	String dateParams[] = valToSet.split("-");
    		ZoneId zoneId = ZoneId.of( "America/Chicago");
    		ZonedDateTime zdt = ZonedDateTime.of (Integer.parseInt(dateParams[0]),Integer.parseInt(dateParams[1]),Integer.parseInt(dateParams[2]),0,0,0,0, zoneId );
    		long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
    		writeLong ( epochSeconds );			
        }
        else if(colToUpdateDataType.equalsIgnoreCase("text"))
        {
        	for (int i = stUpdate; i < (stUpdate+20); i++) {
        		writeByte(0);
            	seek(i);
			}
        	seek(stUpdate);
			writeBytes(valToSet);
        } 	
	}
	
	public int[] findNumofCols_RecLen(String tableName) throws IOException {
		seek(2);
		int position = readShort() + 1; // this position will give us the table name
		seek(position);
		String referTableline = readLine().substring(0, 20);		
		//System.out.println("Our table: " + tableName);
		//System.out.println("Reference Table name: " + referenceTableLine);
		
		int k = 1;
		int numberOfColumns = 0;
		int recordLength = 0;
		
		while(!referTableline.contains(tableName)) {
			position = position + 84*(k);
			seek(position);
			referTableline = readLine().substring(0, 20);
		}
		
		while(referTableline.contains(tableName)) { //readLine gives us the table name	
			numberOfColumns++;
			seek(position+40);
			recordLength += ut.getRecLenAccordingToData(ut.getDataTypefromCode(readShort()));
			//System.out.println("RecordLength " + recordLength);
			position = position + 84*(k);
			seek(position);
			referTableline = readLine().substring(0, 20);
			//System.out.println("Inside Reference Table name: " + referenceTableLine);
			//k++;
		}
		int values[] = new int[2];
		values[0] = numberOfColumns;
		values[1] = recordLength;
		return values;		
	}
}
