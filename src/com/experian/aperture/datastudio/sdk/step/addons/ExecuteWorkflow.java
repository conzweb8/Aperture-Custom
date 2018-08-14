	/**
	 * Copyright © 2017 Experian plc.
	 *
	 * Licensed under the Apache License, Version 2.0 (the "License");
	 * you may not use this file except in compliance with the License.
	 * You may obtain a copy of the License at
	 * http://www.apache.org/licenses/LICENSE-2.0
	 *
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS,
	 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 * See the License for the specific language governing permissions and
	 * limitations under the License.
	 */
	
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
	 * This is a custom step definition that used for executing workflow
	 */
	public class ExecuteWorkflow extends StepConfiguration {
	
		public ExecuteWorkflow() {
			// Basic step information
			setStepDefinitionName("Execute Workflow");
			setStepDefinitionDescription("Executing a workflow");
			setStepDefinitionIcon("ALPHA_NUMERIC");
	
			StepProperty arg1 = new StepProperty()
					.ofType(StepPropertyType.STRING)
					.withIconTypeSupplier(sp -> () -> "OK")
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
					log(getStepDefinitionName() + " - Validate ...");
					return null;
				}
			}
			return false;
		}
	
		private class ExecuteWorkflowOutput extends StepOutput {
			@Override
			public String getName() {
				return "Execute Workflow Output";
			}
	
			@Override
			public long execute() throws SDKException {
				log(getStepDefinitionName() + " - execute output...");
				try {
					String urlParameters = "{ \n" + 
							"   \"scheduling\": { \n" + 
							"     \"duration\": 0, \n" + 
							"     \"end_action\": \"NONE\", \n" + 
							"     \"end_time\": \"2018-02-23T02:50:10.323Z\", \n" + 
							"     \"end_time_type\": \"DURATION\", \n" + 
							"     \"reschedule_interval\": 0, \n" + 
							"     \"reschedule_limit\": 0, \n" + 
							"     \"start_time\": \"2018-02-23T02:30:10.323Z\"  \n" + 
							"   }, \n" + 
							"   \"use_export_steps\": true  \n" + 
							" }";
					URL obj = new URL("http://localhost:7701/api/v1/workflows/Import%20File/execute");
					HttpURLConnection con = (HttpURLConnection) obj.openConnection();
					con.setRequestMethod("POST");
					con.setRequestProperty("user-Agent", "Mozilla/5.0");
					con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
					con.setRequestProperty("Authorization", "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsIml2IjpbMjE0LDQ0LDE1NiwxMjgsMjAsMjQxLDY1LDE5MywyMDQsNzAsMTU3LDE0NSwyMDAsMTYwLDE2OCw2OV19.eyJzdWIiOiJhZG1pbmlzdHJhdG9yIiwic3R0IjoiUkVTVCIsInRrbiI6IlJFU1QiLCJpc3MiOiIwMC0wNS05QS0zQy03QS0wMCIsImV4cCI6MTUxOTQwNDAxMTYyOSwiaWF0IjoxNTE5MzYwODExNjI5fQ==./60eb0VM2a1X9uRcDOsiFK8mUOsR291aUfSgUxg+jEk=");
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
					log(getStepDefinitionName() + " - Response (" + responseCode +") : "+  response.toString());
				}
				catch (Exception ex) {
					logError("ERROR EXECUTE WORKFLOW" + ex.getMessage());
				}
				//Original abstract structure
				return super.execute();
			}
	
			@Override
			public Object getValueAt(long arg0, int arg1) throws SDKException {
				return null;
			}
		}
	}
