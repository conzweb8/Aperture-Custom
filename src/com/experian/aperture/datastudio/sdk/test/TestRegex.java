package com.experian.aperture.datastudio.sdk.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {
	public static void main(String[] args) {
		String str = "1: ABDUL MANAN AGHA 2: na 3: na 4: na";
        Pattern patternName = Pattern.compile("([\\d]: [\\w'\\-\\ ]+)(( \\b)|$)");
        Matcher matcher = patternName.matcher(str);
        
        System.out.println(str.replaceAll("[\\d]:", "").trim().replaceAll(" +", " "));
        
//		int i = 0;
//		while (matcher.find()) {
//		   for (int j = 0; j <= matcher.groupCount(); j++) {
//		      
//		      System.out.println("Group " + i + ": " + matcher.group(j));
//		      i++;
//		   }
//		}
//		
//		System.out.println("------------------------------------");
//		
//		matcher.reset();
//		while (matcher.find()) {
//			System.out.println(matcher.group());
//		}
	}
	
}
