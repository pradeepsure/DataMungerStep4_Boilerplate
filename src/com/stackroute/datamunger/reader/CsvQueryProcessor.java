package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	private String fileName;

	/*
	 * Parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */

	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		FileReader fr = null;
		try {
			fr = new FileReader(fileName);
			this.fileName = fileName;
		} finally {
			if (fr != null)
				try {
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public String getFileName() {
		return fileName;
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */

	@Override
	public Header getHeader() throws IOException {
		String[] columns = null;
		BufferedReader br = null;
		try {
			// read the first line
			br = new BufferedReader(new FileReader(getFileName()));
			String headerLineStr = br.readLine();

			// populate the header object with the String array containing the header names
			columns = headerLineStr.split(",");
		} finally {
			if (br != null)
				br.close();
		}
		return new Header(columns);
	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
	 * -dd')
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions dataTypeDefinitions = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(getFileName()));
			br.readLine();// header line @row1
			String dataLineStr = br.readLine();// data @row2
			String[] columns = dataLineStr.split(",", 18);

			String[] dataTypeArray = new String[columns.length];
			int count = 0;
			for (String s : columns) {
				if (s.matches("[0-9]+")) {
					dataTypeArray[count] = java.lang.Integer.class.getName();
					count++;
				} else if (s.trim().length() == 0) {
					dataTypeArray[count] = java.lang.Object.class.getName();
					count++;
				} else if (s.matches( "[0-3][0-9]/[0-1][0-9]/[0-9]{4}|[0-1][0-9]/[0-3][0-9]/[0-9]{4}|[0-3][0-9]-[a-z]{3}-[0-9]{2}|[0-3][0-9]-[a-z]{3}-[0-9]{4}|[0-3][0-9]-[a-z]*-[0-9]{2}|[0-3][0-9]-[a-z]*-[0-9]{4}|[0-9]{4}/[0-1][0-9]/[0-3][0-9]|[0-9]{4}-[0-1][0-9]-[0-3][0-9]")) {
					// checking for floating point numbers
					// checking for date format dd/mm/yyyy
					// checking for date format mm/dd/yyyy
					// checking for date format dd-mon-yy
					// checking for date format dd-mon-yyyy
					// checking for date format dd-month-yy
					// checking for date format dd-month-yyyy
					// checking for date format yyyy-mm-dd
					dataTypeArray[count] = java.util.Date.class.getName();
					count++;
				} else {
					dataTypeArray[count] = java.lang.String.class.getName();
					count++;
				}
				System.out.println(s+"==>"+dataTypeArray[count-1]);
			}
			dataTypeDefinitions = new DataTypeDefinitions(dataTypeArray);
		} finally {
			if (br != null)
				br.close();
		}

		



		return dataTypeDefinitions;
	}

}
