package com.experian.aperture.v120.datastudio.sdk.step.addons;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

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
	public static String VERSION = "0.1.1";

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
		String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsIml2IjpbNzMsMzEsMTg2LDEwNywxNDgsMjU1LDM4LDU3LDY5LDMyLDIwNCwxNjEsMjU1LDYwLDYzLDEzOV19.eyJzdWIiOiJhZG1pbmlzdHJhdG9yIiwic3R0IjoiUkVTVCIsInRrbiI6IlJFU1QiLCJpc3MiOiIwMC0wNS05QS0zQy03QS0wMCIsImV4cCI6MTUzNDM0OTQ4MDMyNywiaWF0IjoxNTM0MzA2MjgwMzI3fQ==.aGEoFM2M4geNV5MkYzwnFd54YmpikyBZ6eA1xv6VDXs=";

		@Override
		public String getName() {
			return "Execute Workflow Output";
		}

		@Override
		public long execute() throws SDKException {
			log(getStepDefinitionName() + " - execute output...");
			String workflowName = getArgument(0);
			workflowName = workflowName.replace(" ", "%20");
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

			Date currdate = new Date();
			Calendar cal = Calendar.getInstance();
			cal.setTime(currdate);
			cal.add(Calendar.SECOND, 10);
			cal.add(Calendar.HOUR, -7);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

			try {
				String urlParameters = "{ \n" + 
						"   \"scheduling\": { \n" + 
						"     \"duration\": 0, \n" + 
						"     \"end_action\": \"NONE\", \n" + 
						"     \"end_time\": \""+ sdf.format(cal.getTime()) + "\", \n" +
						"     \"end_time_type\": \"NONE\", \n" +
						"     \"reschedule_interval\": 0, \n" + 
						"     \"reschedule_limit\": 0, \n" + 
						"     \"start_time\": \""+ sdf.format(cal.getTime()) + "\"  \n" +
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
				log ("Request JSON : " + urlParameters);

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
