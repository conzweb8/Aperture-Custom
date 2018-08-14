package com.experian.aperture.datastudio.sdk.step.addons;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

public class MyPDFTextParser extends PDFTextStripper {
	String filePath = "";
	private List<String> lines = new ArrayList<String>();
	
	public MyPDFTextParser() throws IOException {
		super();
	}
	
	protected void writeString(String str, List<TextPosition> textPositions) throws IOException {
        String name = "";
        //System.out.println("Hallo");
        Pattern patternNameAnchor = Pattern.compile("[\\w]{2}[\\w].[\\d][\\d][\\d] Name:");
        Pattern patternName = Pattern.compile("([\\d]: [\\w'\\-\\ ]+)(( \\b)|$)");
        Matcher matcher = patternNameAnchor.matcher(str);
        if (matcher.find())
        {
        	Matcher matcher2 = patternName.matcher(str);
        	while(matcher2.find()) {
        		String strtmp = matcher2.group();
        		name = strtmp != null ? name.concat(strtmp) : name;
        	}
        	name = name.replaceAll("[\\d]:", "").trim().replaceAll(" +", " ");
        	getLines().add(name);
        }
        
    }

	public List<String> getLines() {
		return lines;
	}

	public void setLines(List<String> lines) {
		this.lines = lines;
	}

}
