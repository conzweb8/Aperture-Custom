package com.experian.aperture.datastudio.sdk.step.addons;

import java.util.HashMap;
import java.util.Map;

public class Testing {

	public static void main(String[] args) {
		Map<String, Double> aMap = new HashMap<String, Double>();
		aMap.put("tintin@gmail.com" , Double.valueOf(1));
		
		System.out.println(aMap.get("tintin@gmail.com"));
		aMap.put("tintin@gmail.com", aMap.get("tintin@gmail.com") + 1);
		
		System.out.println(aMap.get("tintin@gmail.com"));
	}
}
