package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepColumn;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;

public class DecisionScore extends StepConfiguration{
	public static String VERSION = "0.1.1";
	//TODO: Create Cache
	//TODO: More optimize processing... 

	public DecisionScore() {
		log("Decision Score Version : "+VERSION);
		setStepDefinitionName("HfB Decision Score");
		setStepDefinitionDescription("Calling Decision Engine Web Services");
		setStepDefinitionIcon("ROWS");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.COLUMN_CHOOSER)
				.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> sp.getValue() == null ? "ERROR" : "OK")
				.withArgTextSupplier(sp -> () -> sp.allowedValuesProvider == null ? "Connect an input" : (sp.getValue() == null ? "<Select text column>" : sp.getValue().toString()))
				.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")
				.validateAndReturn();

		setStepProperties(Arrays.asList(arg1));

		setStepOutput(new MyStepOutput());	
	}

	@Override
	public Boolean isComplete() {
		List<StepProperty> properties = getStepProperties();
		if (properties != null && !properties.isEmpty()) {
			StepProperty arg1 = properties.get(0);
			if (arg1 != null && arg1.getValue() != null) {
				return null;
			}
		}
		return false;
	}

	private class MyStepOutput extends StepOutput {
		static final int BLOCK_SIZE = 1000;
		static final int THREAD_SIZE = 24;

		Map<String, DecisionResponse> object_response = new HashMap<String, DecisionResponse>();

		@Override
		public String getName() {
			return "Decision Score";
		}

		@Override
		public void initialise() throws SDKException {
			// clear columns so they are not saved, resulting in undefined columns
			getColumnManager().clearColumns();
			// initialise the columns with the first input's columns
			getColumnManager().setColumnsFromInput(getInput(0));

			//TODO: Check on column name output - for the result after calling decision engine
			getColumnManager().addColumnAt(this, "Score", "", getColumnManager().getColumnCount());

		}

		private String constructBodyRequest() {
			StringBuffer params = new StringBuffer();

			int totalCol = getColumnManager().getColumnCount();

			//Construct O Control
			params.append("<soap:envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\r\n");
			params.append("<soap:body>\r\n" );
			params.append("<DAXMLDocument xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\r\n");
			params.append("<OCONTROL>\r\n");
			for (int a=0 ;a<4; a++) {
				String columnName = getColumnManager().getColumns().get(a).getDisplayName();
				params.append("<").append(columnName).append(">").append("VALUE").append("</").append(columnName).append(">\r\n");
			}
			params.append("</OCONTROL>\r\n");

			params.append("<INPUT>\r\n");
			for (int a=4 ;a<totalCol-6; a++) {
				String columnName = getColumnManager().getColumns().get(a).getDisplayName();
				params.append("<").append(columnName).append(">").append("VALUE").append("</").append(columnName).append(">\r\n");
			}
			params.append("</INPUT>\r\n");
			params.append("<RESULTS>\r\n");
			//TODO: Parsing results
			for (int a=4 ;a<totalCol-6; a++) {
				String columnName = getColumnManager().getColumns().get(a).getDisplayName();
				params.append("<").append(columnName).append(">").append("VALUE").append("</").append(columnName).append(">\r\n");
			}			
			params.append("<Category/>\r\n" + 
					"<InstToTopCat>0</InstToTopCat>\r\n" + 
					"<Mob>0</Mob>\r\n" + 
					"<RiskCategory/>\r\n" + 
					"<Score>0</Score>\r\n" + 
					"<Treatment/>\r\n"); 
			
			params.append("</RESULTS>\r\n");
			params.append("</DAXMLDocument>\r\n");
			params.append("</soap:body>\r\n");
			params.append("</soap:envelope>");

			return params.toString();
		}

		@Override
		public long execute() throws SDKException {
			Long rowCount = Long.valueOf(getInput(0).getRowCount());
			ExecutorService es = Executors.newFixedThreadPool(THREAD_SIZE);

			String selectedColumnName = getArgument(0);
			StepColumn selectedColumn = getColumnManager().getColumnByName(selectedColumnName);

			// queue up to 1000 threads, for processing BLOCK_SIZE times simultaneously
			List<Future> futures = new ArrayList<>();
			Double progress = 0D;
			long rowId = 0L;
			for (rowId = 1L; rowId <= rowCount; rowId++) {
				try {
					StringBuffer parameters = new StringBuffer();
					//TODO: Capture all parameter 
					//Initial logic to capture all available column as input parameter for DA
					String param1 = (String.valueOf(selectedColumn.getValue(rowId)));
					parameters = parameters.append(param1);

					String allparam = parameters.toString();
					String rowIdStr = String.valueOf(rowId-1);
					futures.add(es.submit(() -> performScoring(rowIdStr, allparam)));
					//futures.add(es.submit(() -> performScoringTest(rowIdStr, allparam)));
				} catch (Exception e) {
					throw new SDKException(e);
				}

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

		private DecisionResponse performScoringTest(String rowId, String parameter) {
			int hitung = 0;

			for (int a=0;a<parameter.length();a++)
				hitung = hitung + Integer.valueOf(parameter.charAt(a));

			return new DecisionResponse(String.valueOf(rowId), String.valueOf(hitung));
		}

		/**
		 * Implement your call into your slow Rest (or other) API here.
		 * It will be called concurrently THREAD_SIZE times in order to improve performance.
		 * It currently obtains and returns a composite object that can be customised or changed as required.
		 * @param textString
		 * @return String
		 */
		private DecisionResponse performScoring(String rowId, String parameter) {
			HttpURLConnection con;
			URL obj;
			String messages = "";

			try {
				String urlStr = "http://localhost:8092/DAService";
				obj = new URL(urlStr);
				con = (HttpURLConnection) obj.openConnection();

				String urlBody = "<soap:envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\r\n" + 
						"<soap:body>\r\n" + 
						"<DAXMLDocument xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\r\n" + 
						"<OCONTROL>\r\n" + 
						"<ALIAS>COLE2E</ALIAS>\r\n" + 
						"<SIGNATURE>EXPN</SIGNATURE>\r\n" + 
						"<APPLICATION_ID>111111</APPLICATION_ID>\r\n" + 
						"<DALOGLEVEL>0</DALOGLEVEL>\r\n" + 
						"</OCONTROL>\r\n" + 
						"<INPUT>\r\n" + 
						"<Ba2flag/>\r\n" + 
						"<Badacctflag/>\r\n" + 
						"<Bussunit/>\r\n" + 
						"<Ca>C0</Ca>\r\n" + 
						"<Cam1>C0</Cam1>\r\n" + 
						"<Cam2>C0</Cam2>\r\n" + 
						"<Cam3>C0</Cam3>\r\n" + 
						"<Contractno>0</Contractno>\r\n" + 
						"<Currpalsts/>\r\n" + 
						"<Cy>C0</Cy>\r\n" + 
						"<Cym1>C0</Cym1>\r\n" + 
						"<Cym2>C0</Cym2>\r\n" + 
						"<Cym3>C0</Cym3>\r\n" + 
						"<Grsdp>1000000</Grsdp>\r\n" + 
						"<Instno>14</Instno>\r\n" + 
						"<Isautowo>N</Isautowo>\r\n" + 
						"<Ispelsus>0</Ispelsus>\r\n" + 
						"<Jtp>5</Jtp>\r\n" + 
						"<MobDate>2018-03-10</MobDate>\r\n" + 
						"<Monthinst>1235000</Monthinst>\r\n" + 
						"<Netdp>0</Netdp>\r\n" + 
						"<Objprice>20000000</Objprice>\r\n" + 
						"<Officecode>0</Officecode>\r\n" + 
						"<Pb>L5</Pb>\r\n" + 
						"<Pbm1>L5</Pbm1>\r\n" + 
						"<Pbm2>L5</Pbm2>\r\n" + 
						"<Pbm3>L5</Pbm3>\r\n" + 
						"<Periode>0</Periode>\r\n" + 
						"<Principal>20005000</Principal>\r\n" + 
						"<Prncots>16122976</Prncots>\r\n" + 
						"<Rotype/>\r\n" + 
						"<Sip/>\r\n" + 
						"<Sipgrade>BRONZE</Sipgrade>\r\n" + 
						"<St>AC</St>\r\n" + 
						"<Tgglbyrm1>27</Tgglbyrm1>\r\n" + 
						"<Tgglbyrm2>28</Tgglbyrm2>\r\n" + 
						"<Tgglbyrm3>28</Tgglbyrm3>\r\n" + 
						"<Top>24</Top>\r\n" + 
						"<Tt1>PIM</Tt1>\r\n" + 
						"<Tt2>RC</Tt2>\r\n" + 
						"<Tt3>RC</Tt3>\r\n" + 
						"</INPUT>\r\n" + 
						"<RESULTS>\r\n" + 
						"<Category/>\r\n" + 
						"<InstToTopCat>0</InstToTopCat>\r\n" + 
						"<Mob>0</Mob>\r\n" + 
						"<RiskCategory/>\r\n" + 
						"<Score>0</Score>\r\n" + 
						"<Treatment/>\r\n" + 
						"</RESULTS>\r\n" + 
						"</DAXMLDocument>\r\n" + 
						"</soap:body>\r\n" + 
						"</soap:envelope>";

				con.setRequestMethod("POST");
				con.setRequestProperty("Content-Type", "application/xml");
				con.setDoOutput(true);

				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				wr.writeBytes(urlBody);
				wr.flush();
				wr.close();

				int responseCode = con.getResponseCode();

				BufferedReader in = new BufferedReader(
						new InputStreamReader(con.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();

				//print result

				messages = " - Response (" + responseCode +") : "+  response.toString();
				return new DecisionResponse(String.valueOf(rowId), String.valueOf("TEST"));
			}
			catch (IOException ex){
				ex.printStackTrace();

			}
			catch (Exception ex) {
				ex.printStackTrace();
				messages = ex.getMessage();
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
					e.printStackTrace();
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

			//String value = countObj.get(currentCellValue).toString();
			result = object_response.get(String.valueOf(row)).getResponseMsg();
			//log("Fungsi GetValueAt... return " + result);

			return result;
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
