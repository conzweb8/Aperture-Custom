package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import org.apache.pdfbox.pdmodel.PDDocument;

import com.experian.aperture.datastudio.sdk.exception.SDKException;
import com.experian.aperture.datastudio.sdk.step.StepConfiguration;
import com.experian.aperture.datastudio.sdk.step.StepOutput;
import com.experian.aperture.datastudio.sdk.step.StepProperty;
import com.experian.aperture.datastudio.sdk.step.StepPropertyType;

public class ReadPdfDocuments extends StepConfiguration {
	public static String VERSION = "0.3";

	public ReadPdfDocuments() {
		log("ReadPdfDocuments Version : "+VERSION);
		setStepDefinitionName("Read PDF");
		setStepDefinitionDescription("Parsing PDF Files");
		setStepDefinitionIcon("ROWS");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.STRING)
				.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> "ADD")
				.withArgTextSupplier(sp -> () -> sp.getValue().toString())
				//.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")
				.withInitialValue("C:\\Working Stuff\\Client\\BRI Finance\\sample.pdf")
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
				//log("complete parameter readpdf");
				return true;
			}
		}
		
		//log("not complete parameter readpdf");
		return false;
	}

	private class MyStepOutput extends StepOutput {
		String[][] cells;
		boolean error = false;
		String errMessages = "";
		MyPDFTextParser stripper;
		PDDocument document;
		
		@Override
		public String getName() {
			return "Extract PDF Files";
		}

		@Override
		public void initialise() throws SDKException {
			try {
				String filePath = getStepProperties().get(0).getValue().toString();
				if(filePath == null || filePath.isEmpty())
					filePath = "C:\\Working Stuff\\Client\\BRI Finance\\sample.pdf";
				
				log("Try to open " + filePath);
				document = PDDocument.load(new File(filePath),"brif");
				stripper = new MyPDFTextParser();
			}
			catch(Exception ex) {
				log(ex.getStackTrace().toString());
				cells = new String[1][1];
				error = true;
				errMessages = ex.getMessage();
			}
			// clear columns so they are not saved, resulting in undefined columns
			getColumnManager().clearColumns();
			getColumnManager().addColumn(this, "Nama", "Auto Generated Nama from PDF");

			cells = new String[100][1];
		}

		@Override
		public long execute() throws SDKException {
			int rowcount = 0;
			
			if (error) {
				cells[0][0] = errMessages; 
				return 1;
			}
			
			Writer dummy = new OutputStreamWriter(new ByteArrayOutputStream());
			try {
				stripper.writeText(document, dummy);
			} catch (IOException ex) {
				log(ex.getStackTrace().toString());
				error = true;
				errMessages = ex.getMessage();
			}

			rowcount = stripper.getLines().size();
			return Integer.toUnsignedLong(rowcount);
		}		

		@Override
		public Object getValueAt(long row, int col) throws SDKException {
			String value = cells[Long.valueOf(row).intValue()][col];
			if (value == null) {
				value = stripper.getLines().get(Integer.parseInt(String.valueOf(row)));
				cells[Long.valueOf(row).intValue()][col] = value;
			}

			return value;
		}
	}	
}
