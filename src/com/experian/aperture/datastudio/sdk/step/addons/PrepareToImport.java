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

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;

/**
 * 
 */
public class PrepareToImport extends StepConfiguration {

	public PrepareToImport() {
		// Basic step information
		setStepDefinitionName("Custom - Prepare to import");
		setStepDefinitionDescription("Preparation to be made for processing the imported data");
		setStepDefinitionIcon("ALPHA_NUMERIC");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.STRING)
				//.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> "OK")
				.withArgTextSupplier(sp -> () -> (sp.getValue() == null || sp.getValue().toString().isEmpty()) ? "Input Path" : "Path: " + sp.getValue().toString())
				//.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")
				.withInitialValue("Default")
				.validateAndReturn();

		setStepProperties(Arrays.asList(arg1));

		// Define and set the step output class
		setStepOutput(new PreparetoImportOutput());
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

	private class PreparetoImportOutput extends StepOutput {
		@Override
		public String getName() {
			return "Move files";
		}
		
		@Override
		public long execute() throws SDKException {
			log(getStepDefinitionName() + " - execute output...");
			try {
				// Still using hard code on path
				//TODO: Recommend to parameterized this path reference
				boolean test = Files.exists(Paths.get("C:\\\\ApertureDataStudio\\\\data\\\\export\\\\testfounding.xlsx"), LinkOption.NOFOLLOW_LINKS);
				log("wait for the file ... ");
				int attempt= 1;
				while (test == false) {
					Thread.sleep(1000);
					test = Files.exists(Paths.get("C:\\\\ApertureDataStudio\\\\data\\\\export\\\\testfounding.xlsx"), LinkOption.NOFOLLOW_LINKS);
					attempt++;
					if(attempt > 10)
						break;
					log("wait for the file ...  " + attempt + " attempt... ");
				}
				log(getStepDefinitionName() + " moving file...");
				Files.move(Paths.get("C:\\ApertureDataStudio\\data\\export\\testfounding.xlsx"), Paths.get("C:\\pandora\\import\\testfounding.xlsx"), StandardCopyOption.REPLACE_EXISTING);
			}
			catch(Exception ex) {
				logError(getStepDefinitionName() + " moving error : " + ex.getMessage());
			}			
			return super.execute();
		}

		@Override
		public void initialise() throws SDKException {
			log(getStepDefinitionName() + " - Initialize output...");

		}

		public Object getValueAt(long row, int col) throws SDKException {
			// get the user-defined column names and get the associated columns from the ColumnManager
			log(getStepDefinitionName() + " - getvalueat output...");
			return null;

		}
	}
}
