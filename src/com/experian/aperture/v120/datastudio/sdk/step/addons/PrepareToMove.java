package com.experian.aperture.v120.datastudio.sdk.step.addons;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
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
 * Wait for file to be created and moving files from aperture export
 * folder into Pandora import folder.
 * Modified By: Constantin
 * 
 * Limitation:
 * - 
 */
public class PrepareToMove extends StepConfiguration {
	public static String VERSION = "0.0.2";
	private long wait_in_ms = 1000;
	private long wait_loop = 10;

	public PrepareToMove() {

		// Basic step information
		log("Import Version : "+VERSION);
		setStepDefinitionName("CB Auto Move");
		setStepDefinitionDescription("Move preparation to be made for processing the imported data");
		setStepDefinitionType("PROCESS_ONLY");
		setStepDefinitionIcon("PAUSE");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.DECIMAL)
				//.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> "NUMBER")
				.withArgTextSupplier(sp -> () -> (sp.getValue() == null || sp.getValue().toString().isEmpty()) ? "Wait interval (ms) " : sp.getValue().toString())
				.havingInputNode(() -> "input0")
				//.havingOutputNode(() -> "output0")
				.withInitialValue(wait_in_ms)
				.validateAndReturn();

		StepProperty arg2 = new StepProperty()
				.ofType(StepPropertyType.DECIMAL)
				//.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> "NUMBER")
				.withArgTextSupplier(sp -> () -> (sp.getValue() == null || sp.getValue().toString().isEmpty()) ? "Wait for (interval)" :  sp.getValue().toString())
				//.havingInputNode(() -> "input0")
				//.havingOutputNode(() -> "output0")
				.withInitialValue(wait_loop)
				.validateAndReturn();

		setStepProperties(Arrays.asList(arg1, arg2));

		// Define and set the step output class
		setStepOutput(new PreparetoImportOutput());
	}

	@Override
	public Boolean isComplete() {
		List<StepProperty> properties = getStepProperties();
		if (properties != null && !properties.isEmpty()) {
			StepProperty arg1 = properties.get(0);
			StepProperty arg2 = properties.get(1);
			if (arg1 != null && arg1.getValue() != null 
					&& arg2 != null && arg2.getValue() != null ) {
				log(getStepDefinitionName() + " - Validate ...");
				return true;
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
			String millisecondsString = getArgument(0);
			String waitloopString = getArgument(1);
			wait_in_ms = Long.parseLong(millisecondsString);
			wait_loop = Long.parseLong(waitloopString);

			log(getStepDefinitionName() + " - execute information");
			log(getStepDefinitionName() + " - wait for : " + wait_in_ms + " ms");
			log(getStepDefinitionName() + " - wait interval : " + wait_loop + "x");

			try {
				boolean test = Files.exists(Paths.get("C:\\\\ApertureDataStudio\\\\data\\\\export\\\\testfounding.xlsx"), LinkOption.NOFOLLOW_LINKS);
				log("wait for the file ... ");
				int attempt= 1;
				while (test == false) {
					Thread.sleep(wait_in_ms);
					test = Files.exists(Paths.get("C:\\\\ApertureDataStudio\\\\data\\\\export\\\\testfounding.xlsx"), LinkOption.NOFOLLOW_LINKS);
					attempt++;
					if(attempt > wait_loop)
						break;
					log("wait for the file ...  " + attempt + " attempt... ");
				}
				log(getStepDefinitionName() + " moving file...");
				
				Path successPath = null;
				while (successPath == null) {	
					Thread.sleep(wait_in_ms);
					successPath = Files.move(Paths.get("C:\\ApertureDataStudio\\data\\export\\testfounding.xlsx"), Paths.get("C:\\pandora\\import\\testfounding.xlsx"), StandardCopyOption.REPLACE_EXISTING);
					
					attempt++;
					if(attempt > wait_loop)
						break;
					log("wait for the file ...  " + attempt + " attempt... ");
				}
			}
			catch(Exception ex) {
				logError(getStepDefinitionName() + " moving error : " + ex.getMessage());
			}			
			return 0;
		}

		public Object getValueAt(long row, int col) throws SDKException {
			// get the user-defined column names and get the associated columns from the ColumnManager
			log(getStepDefinitionName() + " - getvalueat output...");
			return null;

		}
	}
}
