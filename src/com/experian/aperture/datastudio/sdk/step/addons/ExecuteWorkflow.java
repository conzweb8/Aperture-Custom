package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;


/**
 * This is a custom step definition that triggers a workflow
 * Modified By: Constantin
 * 
 * Limitation:
 * - Workflow name must not contain whitespace or any special characters
 */
public class ExecuteWorkflow extends StepConfiguration {
	public static String VERSION = "0.0.3";

	public ExecuteWorkflow() {
		// Basic step information
		log("Workflow Execution Version : "+VERSION);
		setStepDefinitionName("CB Execute Workflow ");
		setStepDefinitionDescription("Executing a workflow");
		setStepDefinitionIcon("SHARE");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.STRING)
				//.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> sp.getValue() == null ? "ERROR" : "OK")
				.withArgTextSupplier(sp -> () -> (sp.getValue() == null || sp.getValue().toString().isEmpty()) ? "Workflow name" : sp.getValue().toString())
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
		String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsIml2IjpbMjEwLDE0NywyMTUsODIsNjgsMzgsMSwyOCwxNTUsMSwzNiwxNDcsMjE3LDE4MiwyMzEsODFdfQ==.eyJzdWIiOiJhZG1pbmlzdHJhdG9yIiwic3R0IjoiUkVTVCIsInRrbiI6IlJFU1QiLCJpc3MiOiI0Mi0wMS0wQS05NC0wMC0wMyIsImV4cCI6MTUzNjYxOTg4MjY3OCwiaWF0IjoxNTM2NTc2NjgyNjc4fQ==.ANk0zOpbA3UtbEnohNdWr7UlkN8IBuLkTT3p0qEw9Oo=";

		@Override
		public String getName() {
			return "Execute Workflow Output";
		}

		@Override
		public long execute() throws SDKException {
			log(getStepDefinitionName() + " - execute output...");
			String workflowName = getArgument(0);
			try {
				JSONObject otfToken = new JSONObject(getOnTheFlyAuthenticationToken());

				log("Workflow Name: " + workflowName);
				log("Authentication otf : " + otfToken.toString());

				if (otfToken != null && otfToken.length() > 0) {
					token = otfToken.getString("authentication_token");
					log("Use otf token.");
				}
			}
			catch (Exception ex) {
				ex.printStackTrace();
				logError("Error while parsing JSON authentication value");
			}

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
				con.setRequestProperty("Authorization", "Bearer " + token);
				con.setRequestProperty("Accept", "application/json");
				con.setRequestProperty("Content-Type", "application/json");
				con.setDoOutput(true);

				log ("Token used : \nBearer " + token);
				log (con.getRequestProperty("Authorization"));

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
				ex.printStackTrace();
				messages = ex.getMessage();
				logError("EXECUTE WORKFLOW FAILED ! " + ex.getLocalizedMessage());
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

		private String getOnTheFlyAuthenticationToken () throws SDKException {
			StringBuffer response = new StringBuffer();

			try {
				String urlParameters = "{ \n" + 
						"   \"password\": \"P@ssw0rd\", \n" + 
						"   \"username\": \"administrator\" \n" + 
						" }";
				URL obj = new URL("http://localhost:7701/api/v1/login");
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				con.setRequestMethod("POST");
				con.setRequestProperty("user-Agent", "Mozilla/5.0");
				con.setRequestProperty("Accept", "*/*");
				con.setRequestProperty("Content-Type", "application/json");
				con.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(con.getOutputStream());
				wr.writeBytes(urlParameters);
				wr.flush();
				wr.close();

				BufferedReader in = new BufferedReader(
						new InputStreamReader(con.getInputStream()));
				String inputLine;

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();

			}
			catch (Exception ex) {
				logError("ERROR EXECUTE WORKFLOW" + ex.getLocalizedMessage());
				logError("Caused by " + ex.getCause().getMessage()+"("+ex.getCause().getLocalizedMessage()+")");
			}		

			return response.toString();
		}
	}
}
