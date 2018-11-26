package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepColumn;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;

public class DecisionScore extends StepConfiguration{
	public static String VERSION = "0.0.2";
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
					String rowIdStr = String.valueOf(rowId);
					futures.add(es.submit(() -> performScoringTest(rowIdStr, allparam)));
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
		private String performScoring(String parameter) {
			HttpClient httpClient = new DefaultHttpClient();
			HttpPost httpPost = new HttpPost("http://35.192.129.194:5000/classify");
			// Request parameters and other properties.

			try {
				httpPost.addHeader("content-type", "application/json");
				StringEntity paramsTxt = new StringEntity("{\"text\":\"" + "" + "\"}") ;
				httpPost.setEntity(paramsTxt);
			} catch (UnsupportedEncodingException e) {
				// writing error to Log
				log("Error ! perform http post " + e.getMessage());
				e.printStackTrace();
			}
			/*
			 * Execute the HTTP Request
			 */
			try {
				HttpResponse response = httpClient.execute(httpPost);
				HttpEntity respEntity = response.getEntity();

				if (respEntity != null) {
					// EntityUtils to get the response content
					String content =  EntityUtils.toString(respEntity);
					JSONObject obj = new JSONObject(content);
					//log("Return : " + obj.getString("result"));
					return obj.getString("result");
				}
			} catch (ClientProtocolException e) {
				// writing exception to log
				e.printStackTrace();
				log("Error ! perform response retrieval" + e.getMessage());
			} catch (IOException e) {
				// writing exception to log
				e.printStackTrace();
				log("Error ! perform response retrieval" + e.getMessage());
			} catch (Exception e) {
				e.printStackTrace();
				log("Error ! perform response retrieval" + e.getMessage());
			}

			return "Unknown";
		}

		private void waitForFutures(List<Future> futures) throws SDKException {
			log("Check object to save : " + futures.size());
			
			for (Future future : futures) {				
				Object emr = null;
				try {
					emr = future.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new SDKException(e);
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (emr != null && emr instanceof String) {
					//log("Param  " + rowIdHere + ": " + String.valueOf(emr) + "");
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
			log("Fungsi GetValueAt... return " + result);

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
