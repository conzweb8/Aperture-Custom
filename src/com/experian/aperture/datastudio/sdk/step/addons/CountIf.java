package com.experian.aperture.datastudio.sdk.step.addons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

public class CountIf extends StepConfiguration{
	public static String VERSION = "1.0.0";

	public CountIf() {
		log("Countif Version : "+VERSION);
		setStepDefinitionName("Count If");
		setStepDefinitionDescription("Performing Count based on duplicate string");
		setStepDefinitionIcon("ROWS");

		StepProperty arg1 = new StepProperty()
				.ofType(StepPropertyType.COLUMN_CHOOSER)
				.withStatusIndicator(sp -> () -> sp.allowedValuesProvider != null)
				.withIconTypeSupplier(sp -> () -> sp.getValue() == null ? "ERROR" : "OK")
				.withArgTextSupplier(sp -> () -> sp.allowedValuesProvider == null ? "Connect an input" : (sp.getValue() == null ? "<Select match column>" : sp.getValue().toString()))
				.havingInputNode(() -> "input0")
				.havingOutputNode(() -> "output0")
				.validateAndReturn();

		setStepProperties(Arrays.asList(arg1));

		setStepOutput(new MyStepOutput());	
	}

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
		Map<String, Long> countObj = new HashMap<String, Long>();
		Map<String, Long> keyObj = new HashMap<String, Long>();

		@Override
		public String getName() {
			return "Count IF";
		}

		@Override
		public long execute() throws SDKException {
			long lastRowCount = 0L;
			Long rowCount = Long.valueOf(getInput(0).getRowCount());
			ExecutorService es = Executors.newFixedThreadPool(THREAD_SIZE);

			String selectedColumnName = getArgument(0);
			log("Column Name Selected : " + selectedColumnName);
			StepColumn selectedColumn = getColumnManager().getColumnByName(selectedColumnName);

			// get the column object from the first input
			// queue up to 1000 threads, for processing BLOCK_SIZE times simultaneously
			List<Future> futures = new ArrayList<>();
			//log("Future size processed : " + futures.size() + " row id " + rowCount);
			for (long rowId = 1L; rowId <= rowCount; rowId++) {
				try {
					Object test = selectedColumn.getValue(rowId-1);
					//log("Type instance of " + (rowId) + " are " + test.getClass().getName());
					String matchText = (String.valueOf(selectedColumn.getValue(rowId-1)));
					futures.add(es.submit(() -> matchText));
				} catch (Exception e) {
					throw new SDKException(e);
				}

				if (rowId % BLOCK_SIZE == 0) {
					lastRowCount = rowId;
					//log("Future size processed : " + futures.size() + " row id " + rowId);
					waitForFutures(futures, lastRowCount);
					Double progress = (Long.valueOf(rowId).doubleValue()/rowCount) * 100;
					log("Processed: " + progress.intValue() + "%");
				}
			}

			// process the remaining futures
			log("(Ini di luar loop : ) Future size processed : " + futures.size() + " row id " + lastRowCount + "/"+ rowCount);
			waitForFutures(futures, lastRowCount);

			// close all threads
			es.shutdown();
			keyObj.clear();

			// log that we are complete
			log("Processed: " + 100 + "%");

			return rowCount;
		}		

		private void waitForFutures(List<Future> futures, long rowId) throws SDKException {
			long rowIdHere;
			if (futures.size() % BLOCK_SIZE > 0 && rowId > 0) {
				rowIdHere = rowId;
			}
			else if (rowId % BLOCK_SIZE == 0 && rowId > 0) {
				rowIdHere = rowId - BLOCK_SIZE;
			}
			else
				rowIdHere = 0;

			for (Future future : futures) {				
				Object emr = null;
				try {
					emr = future.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new SDKException(e);
				}
				//log("Row processed : " + rowIdHere + " emr : " + emr);
				if (emr != null && emr instanceof String) {
					if(keyObj.containsKey(emr)) {
						long currentKeyCount = keyObj.get(emr);
						countObj.put(String.valueOf(rowIdHere), currentKeyCount + 1);
						keyObj.put(String.valueOf(emr), currentKeyCount + 1);
					}
					else {
						countObj.put(String.valueOf(rowIdHere), Long.valueOf(1));
						keyObj.put(String.valueOf(emr), Long.valueOf(1));
					}

					//log("Add to countObj ("+ (String) emr +") at "+ countObj.get(emr).toString() +"...");
				}
				rowIdHere++;
			}
			futures.clear();
		}

		@Override
		public void initialise() throws SDKException {
			// clear columns so they are not saved, resulting in undefined columns
			getColumnManager().clearColumns();

			String selectedColumnName = getArgument(0);
			// ensure that our output columns pass through all those from the first input (the default behaviour)
			getColumnManager().setColumnsFromInput(getInput(0));
			// fine the user-selected column
			StepColumn selectedColumn = getColumnManager().getColumnByName(selectedColumnName);
			if (selectedColumn != null) {
				// get it's position in the column list
				int selectedColumnPosition = getColumnManager().getColumnPosition(selectedColumnName);
				// and add our own column in its place, so we can change its value in getValueAt()
				getColumnManager().addColumnAt(this, selectedColumnName + " Count If", "", selectedColumnPosition + 1);
			} else {
				logError(getStepDefinitionName() + " - Couldn't find a column by the name of: " + selectedColumnName);
			}
		}

		@Override
		public Object getValueAt(long row, int col) throws SDKException {
			//log("Fungsi GetValueAt...");
			// get the user-defined column

			String selectedColumnName = getArgument(0);

			// get the column object from the first input
			Optional<StepColumn> inputColumn = null;
			if (selectedColumnName != null && !selectedColumnName.isEmpty()) {
				inputColumn = getInputColumn(0, selectedColumnName);
			}
			if (inputColumn.isPresent()) {			
				//String value = countObj.get(currentCellValue).toString();
				return countObj.get(String.valueOf(row));

			} else {
				// if not found return an empty value. We could alternatively throw an error.
				logError(getStepDefinitionName() + " - There was an Error doing getValueAt Row: " + row + ", Column: " + col);
				return "ERROR";
			}
		}
	}	

}
