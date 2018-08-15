package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;


/**
 * This is a custom step definition that triggers a workflow
 */
public class ExecuteWorkflow extends StepConfiguration {
	public static String VERSION = "0.0.3";
	
	public ExecuteWorkflow() {
		// Basic step information
		log("Workflow Execution Version : "+VERSION);
		setStepDefinitionName("Execute Workflow ");
		setStepDefinitionDescription("Executing a workflow");
		setStepDefinitionIcon("SHARE");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.STRING)
				.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> sp.getValue() == null ? "ERROR" : "OK")
				.withArgTextSupplier(sp -> () -> (sp.getValue() == null || sp.getValue().toString().isEmpty()) ? "Input Value" : sp.getValue().toString())
				.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")	
				.validateAndReturn();

		setStepProperties(Arrays.asList(arg1));

		// Define and set the step output class
		setStepOutput(new ExecuteWorkflowOutput());
	}

	@Override
	public Boolean isComplete() {
		List<StepProperty> properties = getStepProperties();
		if (properties != null && !properties.isEmpty()) {
			StepProperty arg1 = properties.get(0);
			if (arg1 != null && arg1.getValue() != null ) {
				return null;
			}
		}
		return false;
	}

	private class ExecuteWorkflowOutput extends StepOutput {
		String messages = "";
		
		@Override
		public String getName() {
			return "Execute Workflow Output";
		}

		@Override
		public long execute() throws SDKException {
			log(getStepDefinitionName() + " - execute output...");
			String workflowName = (String) getStepProperties().get(0).getValue();
			
			try {
				String urlParameters = "{ \n" + 
						"   \"scheduling\": { \n" + 
						"     \"duration\": 0, \n" + 
						"     \"end_action\": \"NONE\", \n" + 
						"     \"end_time\": \"2018-08-14T17:50:10.323Z\", \n" + 
						"     \"end_time_type\": \"DURATION\", \n" + 
						"     \"reschedule_interval\": 0, \n" + 
						"     \"reschedule_limit\": 0, \n" + 
						"     \"start_time\": \"2018-08-14T16:00:00.323Z\"  \n" + 
						"   }, \n" + 
						"   \"use_export_steps\": true  \n" + 
						" }";
				URL obj = new URL("http://localhost:7701/api/v1/workflows/"+workflowName+"/execute");
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				con.setRequestMethod("POST");
				con.setRequestProperty("user-Agent", "Mozilla/5.0");
				con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
				con.setRequestProperty("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsIml2IjpbMzYsOTYsNjMsMTQwLDI1NSwxNjcsNTMsNjAsNjIsMTY2LDIwMSwxNDIsMTY5LDE0MywyMjcsMzNdfQ==.eyJzdWIiOiJhZG1pbmlzdHJhdG9yIiwic3R0IjoiUkVTVCIsInRrbiI6IlJFU1QiLCJpc3MiOiIwMC0wNS05QS0zQy03QS0wMCIsImV4cCI6MTUzNDI4MDIyNjI5OSwiaWF0IjoxNTM0MjM3MDI2Mjk5fQ==.xsBlrCAE3f2UCx/vPVEV6unbj52EIxsccpXD+hghBWc=");
				con.setRequestProperty("Accept", "application/json");
				con.setRequestProperty("Content-Type", "application/json");
				con.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				wr.writeBytes(urlParameters);
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
				messages = getStepDefinitionName() + " - Response (" + responseCode +") : "+  response.toString();
				log(messages);
			}
			catch (Exception ex) {
				messages = ex.getMessage();
				logError("ERROR EXECUTE WORKFLOW" + ex.getLocalizedMessage());
				logError("Caused by " + ex.getCause().getMessage()+"("+ex.getCause().getLocalizedMessage()+")");
			}
			//Original abstract structure
			return 1;
		}
		
		@Override
		public void initialise() throws SDKException {
			// clear columns so they are not saved, resulting in undefined columns
			messages = "Not executing, just initialize";
			getColumnManager().clearColumns();
			getColumnManager().addColumn(this, "Status", "Status");
		}

		@Override
		public Object getValueAt(long arg0, int arg1) throws SDKException {
			return messages;
		}
	}
}
