package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jfree.util.Log;
import org.python.core.exceptions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;
import com.experian.datatype.DateAndTime;


public class DecisionScore extends StepConfiguration{
	public static String VERSION = "0.1.19";
	public String URL = "http://localhost:8092/DAService";
	public int THREAD_SIZE = Runtime.getRuntime().availableProcessors() * 2;
	private DAXmlDocument daxmldoc = null;

	//TODO: Create Cache
	//TODO: More optimize processing... 
	public DecisionScore() {
		log("Decision Score Version : "+VERSION);
		setStepDefinitionName("HfB Decision Score");
		setStepDefinitionDescription("Calling Decision Engine Web Services");
		setStepDefinitionIcon("ROWS");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.STRING)
				.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> sp.getValue() == null ? "ERROR" : "OK")
				.withArgTextSupplier(sp -> () -> {
					if (sp.getValue() == null || sp.getValue().toString().isEmpty()) {
						return "Enter WSDL Path";
					} else {
						return "WSDL : " + sp.getValue().toString();
					}
				})
				.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")
				.validateAndReturn();

		StepProperty arg2 = new StepProperty()
				.ofType(StepPropertyType.STRING)
				.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> sp.getValue() == null ? "ERROR" : "OK")
				.withArgTextSupplier(sp -> () -> {
					if (sp.getValue() == null || sp.getValue().toString().isEmpty()) {
						return "Enter URL Path Default http://localhost:8092/DAService";
					} else {
						return "URL : " + sp.getValue().toString();
					}
				})
				.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")
				.validateAndReturn();

		setStepProperties(Arrays.asList(arg1, arg2));

		setStepOutput(new MyStepOutput());	
	}

	@Override
	public Boolean isComplete() {
		List<StepProperty> properties = getStepProperties();
		if (properties != null && !properties.isEmpty()) {
			StepProperty arg1 = properties.get(0);
			StepProperty arg2 = properties.get(1);

			if (arg1 != null && arg1.getValue() != null && arg2 != null && arg2.getValue() != null ) {
				return true;
			}
		}
		return false;
	}

	private class MyStepOutput extends StepOutput {
		static final int BLOCK_SIZE = 1000;

		Map<String, DecisionResponse> object_response = new HashMap<String, DecisionResponse>();

		@Override
		public String getName() {
			return "Decision Score";
		}

		@Override
		public void initialise() throws SDKException {
			/*	For future use, determine which input section as header, body and result, other
				options will be to be able to read the wsdl and automatically detects */
			String wsdl = getArgument(0).toString();
			URL = getArgument(1).toString();
			try {
				daxmldoc =  new DAXmlDocument(wsdl);
			} catch (Exception ex) {
				logError("ERROR OH MY GOD ! " + ex.getMessage());
			}
			//            log("Initialize header end col : " + headercolno + " Start body col " + startbodycolno + " body end col " + bodycolno);

			// clear columns so they are not saved, resulting in undefined columns
			getColumnManager().clearColumns();
			// initialise the columns with the initial (prev step) input's columns
			getColumnManager().setColumnsFromInput(getInput(0));
			getColumnManager().removeColumn("Score");

			//TODO: Check on column name output - for the result after calling decision engine
			getColumnManager().addColumnAt(this, "Score", "", getColumnManager().getColumnCount());

		}

		private String constructBodyRequestfromWSDL(long row) {
			StringBuffer params = new StringBuffer();
			String currColumnProcessed = "";

			try {
				//Construct O Control
				params.append("<soap:envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\r\n");
				params.append("<soap:body>\r\n" );
				params.append("<DAXMLDocument xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\r\n");
				params.append("<OCONTROL>\r\n");
				for (int a=0 ;a<daxmldoc.getControlDataSourceList().size(); a++) {
					currColumnProcessed = daxmldoc.getControlDataSourceList().get(a);
					//					log("Try to fetch : " + currColumnProcessed);
					String columnName = getColumnManager().getColumnByName(daxmldoc.getControlDataSourceList().get(a)).getDisplayName();
					//					log("Fetching name ok : "+ columnName);
					String columnValue = getColumnManager().getColumnByName(columnName).getValue(row) == null ? "" : getColumnManager().getColumnByName(columnName).getValue(row).toString();
					if(columnValue != null && columnValue.trim().length() > 0 && columnName != null && columnName.trim().length() > 0) {
						params.append("<").append(columnName.toUpperCase().replace(" ", "_")).append(">").append(columnValue).append("</").append(columnName.toUpperCase().replace(" ", "_")).append(">\r\n");
					}
					//					log("Fetching values ok " + columnValue);
				}
				params.append("</OCONTROL>\r\n");

				//Construct Body Input
				params.append("<INPUT>\r\n");
				for (int a=0 ;a<daxmldoc.getInputDataSourceList().size(); a++) {
					currColumnProcessed = daxmldoc.getInputDataSourceList().get(a);
					//					log("Try to fetch : " + daxmldoc.getInputDataSourceList().get(a));
					String columnName = getColumnManager().getColumnByName(daxmldoc.getInputDataSourceList().get(a)).getDisplayName();
					String columnValueStr = "";
					Object columnValue = getColumnManager().getColumnByName(columnName).getValue(row) == null ? "" : getColumnManager().getColumnByName(columnName).getValue(row).toString();

					if (columnValue != null && columnValue instanceof DateAndTime) {
						//If date format
						DateAndTime dateformatval = (DateAndTime) columnValue;
						//						log("Before : " + dateformatval.toString());

						Calendar datecalendar = dateformatval.toCalendar();

						SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
						columnValueStr = format.format(datecalendar.getTime());

						//						log("After formatted : " + columnValueStr);
					}
					else if (columnValue != null && columnValue.toString().trim().length() > 0) {
						//Everything else
						//						log("Object instance : " + columnValue.getClass());

						//Try to format a date
						Pattern p = Pattern.compile("[0-3]?[0-9][-][A-Za-z]{3}[-][\\d]{4}");
						Matcher isDateFormat = p.matcher(columnValue.toString());
						if (isDateFormat.matches()) { 
							try {
								Date newDate = new Date(columnValue.toString());

								SimpleDateFormat format = new SimpleDateFormat("yyyy-dd-MM");
								columnValueStr = format.format(newDate);

								//								log("After formatted : " + columnValueStr);
							}catch (Exception ex) {
								logError("Unable to transform to date");
							}
						} else {
							//Date 
							columnValueStr = columnValue.toString();
						}
					}
					params.append("<").append(columnName).append(">").append(columnValueStr).append("</").append(columnName).append(">\r\n");
				}
				params.append("</INPUT>\r\n");

				//Construct Results dummy
				params.append("<RESULTS>\r\n");
				for (int a=0 ;a<daxmldoc.getResultsDataSourceList().size(); a++) {
					//					log("Try to fetch : " + daxmldoc.getResultsDataSourceList().get(a));
					currColumnProcessed = daxmldoc.getControlDataSourceList().get(a);
					String columnName = null;

					try {
						columnName = getColumnManager().getColumnByName(daxmldoc.getResultsDataSourceList().get(a)).getDisplayName();
					}
					catch (NullPointerException ex) {
						columnName = daxmldoc.getResultsDataSourceList().get(a);
					}

					Object columnValue = null;
					try {
						columnValue = getColumnManager().getColumnByName(columnName).getValue(row) == null ? "" : getColumnManager().getColumnByName(columnName).getValue(row).toString();
					}
					catch (NullPointerException ex) {
						columnValue = "0";
					}
					String columnValueStr = columnValue == null ? "" : columnValue.toString();
					params.append("<").append(columnName).append(">").append(columnValueStr).append("</").append(columnName).append(">\r\n");
				}			
				params.append("</RESULTS>\r\n");

				//Construct end tag
				params.append("</DAXMLDocument>\r\n");
				params.append("</soap:body>\r\n");
				params.append("</soap:envelope>");
//				log("Full xml doc : " + params.toString());
			} catch (Exception e) {
				logError(e.getMessage() + " - Current Column Processed : " + currColumnProcessed + " - got problem !");
			}

			//log("string constructed: "+params.toString());
			return params.toString();
		}

		@Override
		public long execute() throws SDKException {
//			log("Execute...");
			Long rowCount = Long.valueOf(getInput(0).getRowCount());
			ExecutorService es = Executors.newFixedThreadPool(THREAD_SIZE);

			// queue up to 1000 threads, for processing BLOCK_SIZE times simultaneously
			List<Future> futures = new ArrayList<>();
			Double progress = 0D;
			long rowId = 0L;
			for (rowId = 1L; rowId <= rowCount; rowId++) {
				String rowIdStr = String.valueOf(rowId-1);
				futures.add(es.submit(() -> performScoring(rowIdStr)));

				if (rowId % BLOCK_SIZE == 0) {
					//log("Future size processed : " + futures.size() + " row id " + rowId);
					waitForFutures(futures);
					progress = (Long.valueOf(rowId).doubleValue()/rowCount) * 100;
					//log("Processed: " + progress.intValue() + "%");
					sendProgress(progress);
				}
			}

			// process the remaining futures
			waitForFutures(futures);

			// close all threads
			es.shutdown();

			// log that we are complete
			//log("Processed: " + 100 + "%");
			sendProgress(progress);
			return rowCount;
		}	

		private String getScoreFromResponseMessages (String messages) {
//			log("Parsing result scoring...");
			String ret_msg="";
			try {  
				DocumentBuilderFactory fctr = DocumentBuilderFactory.newInstance();
				DocumentBuilder bldr = fctr.newDocumentBuilder();
				InputSource insrc = new InputSource(new StringReader(messages));
				Document document = bldr.parse(insrc);
				NodeList nodeList = document.getElementsByTagName("RESULTS");
				for (int i = 0; i < nodeList.getLength(); i++) {
					Node node = nodeList.item(i);
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						// do something with the current element
						//System.out.println(node.getNodeName());
						NodeList nodeList2 = node.getChildNodes();
						for (int j = 0; j < nodeList2.getLength(); j++) {
							//System.out.println(nodeList2.item(j).getNodeName());

							Element resultEl = null;
							if(nodeList2.item(j).getNodeType() == Node.ELEMENT_NODE)
								resultEl = (Element) nodeList2.item(j);

							if (resultEl != null && resultEl.getNodeName().equals("Score")) {
								//								System.out.println("Ketemu Score dengan value : " + resultEl.getTextContent());
								ret_msg = resultEl.toString();
								return resultEl.getTextContent();
							}
						}
					}
				}
			} catch (Exception e) {  
				Log.error("Error happened when return result : " + e.getMessage());
				log("Return messages : " + ret_msg);
				e.printStackTrace();  
			} 

			return "Error";
		}

		private DecisionResponse performScoring(String rowId) {
			HttpURLConnection con;
			URL obj;
//			log("Calling perform scorring...");
			
			try {
				String urlStr = "";

				if (URL != null && URL.trim().length() > 0)
					urlStr = URL;
				else
					urlStr = "http://localhost:8092/DAService";
//				log("sending to " + urlStr);	

				obj = new URL(urlStr);
				con = (HttpURLConnection) obj.openConnection();

				String urlBody = constructBodyRequestfromWSDL(Long.parseLong(rowId));

				con.setRequestMethod("POST");
				con.setRequestProperty("Content-Type", "application/xml");
				con.setDoOutput(true);

				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				wr.writeBytes(urlBody);
				wr.flush();
				wr.close();

				//int responseCode = con.getResponseCode();

				BufferedReader in = new BufferedReader(
						new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();

//				log("Response - message : " + response.toString());
				String responseScore = getScoreFromResponseMessages(response.toString());
				return new DecisionResponse(String.valueOf(rowId), responseScore);
			}
			catch (IOException ex){
				Log.error("Error while sending messages " + ex.getMessage());
			}
			catch (Exception ex) {
				Log.error("Error while getting response messages " + ex.getMessage());
			}

			return new DecisionResponse(String.valueOf(rowId), String.valueOf("Unknown"));
		}

		private void waitForFutures(List<Future> futures) throws SDKException {
			//log("Check object to save : " + futures.size());

			for (Future future : futures) {				
				Object emr = null;
				try {
					emr = future.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new SDKException(e);
				} catch (Exception e) {
					throw new SDKException(e);
				}
				if (emr != null && emr instanceof DecisionResponse) {
					//log("Param  " + ((DecisionResponse) emr).getRowID() + ": " + String.valueOf(emr) + "");
					object_response.put(((DecisionResponse) emr).getRowID(), (DecisionResponse) emr);
				}

			}
			futures.clear();
		}

		@Override
		public Object getValueAt(long row, int col) throws SDKException {
			String result = "";
			//			log("row : " + row + "col : " + getColumnManager().getColumns().get(col).getDisplayName());

			result = object_response.get(String.valueOf(row)).getResponseMsg();
			return result;
			//String value = countObj.get(currentCellValue).toString();

			//log("Fungsi GetValueAt... return " + result);

		}

		private class DecisionResponse {
			private String rowID;
			private String responseMsg;

			public DecisionResponse(String rowID, String responseMsg) {
				this.rowID = rowID;
				this.responseMsg = responseMsg;
			}

			public String getRowID() {
				return rowID;
			}

			private String getResponseMsg() {
				return responseMsg;
			}

		}

	}	

}
