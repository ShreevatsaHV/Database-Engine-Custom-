public class Utility {
	public int getRecLenAccordingToData(String data_type) {
		int recordSize = 0;
		if(data_type.equalsIgnoreCase("int"))
        {
            recordSize=recordSize+4;
        }
        else if(data_type.equalsIgnoreCase("tinyint"))
        {
            recordSize=recordSize+1;
        }
        else if(data_type.equalsIgnoreCase("smallint"))
        {
            recordSize=recordSize+2;
        }
        else if(data_type.equalsIgnoreCase("bigint"))
        {
            recordSize=recordSize+8;
        }
        else if(data_type.equalsIgnoreCase("real"))
        {
            recordSize=recordSize+4;
        }
        else if(data_type.equalsIgnoreCase("double"))
        {
            recordSize=recordSize+8;
        }
        else if(data_type.equalsIgnoreCase("datetime"))
        {
            recordSize=recordSize+8;
        }
        else if(data_type.equalsIgnoreCase("date"))
        {
            recordSize=recordSize+8;
        }
        else if(data_type.equalsIgnoreCase("text"))
        {
            recordSize=recordSize+20;
        }
		
		return recordSize;
	}
	
	public String getDataTypefromCode(int Serial) {
		String dataType = "";
		if(Serial == 6)
        {
			dataType="int";
        }
        else if(Serial == 4)
        {
        	dataType="tinyint";
        }
        else if(Serial == 5)
        {
        	dataType="smallint";
        }
        else if(Serial == 7)
        {
        	dataType="bigint";
        }
        else if(Serial == 8)
        {
        	dataType="real";
        }
        else if(Serial == 9)
        {
        	dataType= "double";
        }
        else if(Serial == 10)
        {
        	dataType="datetime";
        }
        else if(Serial == 11)
        {
        	dataType="date";
        }
        else if(Serial == 12)
        {
        	dataType="text";
        }
		
		return dataType;
	}
	
	public int getSerialCode(String dataType) {
		int serial = 0;
		if(dataType.equalsIgnoreCase("int"))
        {
            serial=0x06;
        }
        else if(dataType.equalsIgnoreCase("tinyint"))
        {
        	serial=0x04;
        }
        else if(dataType.equalsIgnoreCase("smallint"))
        {
            serial=0x05;
        }
        else if(dataType.equalsIgnoreCase("bigint"))
        {
            serial=0x07;
        }
        else if(dataType.equalsIgnoreCase("real"))
        {
            serial=0x08;
        }
        else if(dataType.equalsIgnoreCase("double"))
        {
            serial=0x09;
        }
        else if(dataType.equalsIgnoreCase("datetime"))
        {
            serial=0x0A;
        }
        else if(dataType.equalsIgnoreCase("date"))
        {
            serial=0x0B;
        }
        else if(dataType.equalsIgnoreCase("text"))
        {
            serial=0x0C;
        }
		
		return serial;
	}
}
