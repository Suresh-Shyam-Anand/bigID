package com.bigid.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigIDTask {

	static List<String> inputSearchList = Arrays.asList("James", "John", "Robert", "Michael", "William", "David",
			"Richard", "Charles", "Joseph", "Thomas", "Christopher", "Daniel", "Paul", "Mark", "Donald", "George",
			"Kenneth", "Steven", "Edward", "Brian", "Ronald", "Anthony", "Kevin", "Jason", "Matthew", "Gary", "Timothy",
			"Jose", "Larry", "Jeffrey", "Frank", "Scott", "Eric", "Stephen", "Andrew", "Raymond", "Gregory", "Joshua",
			"Jerry", "Dennis", "Walter", "Patrick", "Peter", "Harold", "Douglas", "Henry", "Carl", "Arthur", "Ryan",
			"Roger");

	static BigIDTask sampleTextData = new BigIDTask();
	

	public static void main(String[] args) throws IOException {
		
		String fileNameURL = "http://norvig.com/big.txt";
		URL url = new URL(fileNameURL);
				
		int linesPerBlock = 1000;
		int linesRead = 0;
		int aggrLinesRead = 0;
		ArrayList<String> lineList = new ArrayList<String>();
		ArrayList<Map<String, List<SearchItem>>> searchMapList = new ArrayList<Map<String, List<SearchItem>>>();

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
			String line;
			String aggrLine = "";

			while ((line = reader.readLine()) != null) {
				aggrLine = aggrLine + line;
				linesRead++;

				if (linesRead >= linesPerBlock) {

					lineList.add(aggrLine);
					final int lineOffset = aggrLinesRead;
					final String inputLine = aggrLine;
					Thread thread = new Thread(new Runnable() {
						@Override
						public void run() {
							matcher(searchMapList, lineOffset, inputLine);
						}
					});
					thread.start();

					aggrLinesRead += linesRead;
					linesRead = 0;
					aggrLine = "";

				}
			}
			aggregator(searchMapList);

		} catch (IOException e) {
			System.err.println("Error reading the file: " + e.getMessage());
		}

	}

	public BigIDTask() {
		super();
	}

	public static void matcher(ArrayList<Map<String, List<SearchItem>>> searchMapList, int lineOffset,
			String inputLine) {

		Map<String, List<SearchItem>> searchListMap = new HashMap<String, List<SearchItem>>();
		for (String inputSearch : inputSearchList) {
			List<SearchItem> searchItemList = new ArrayList<SearchItem>();
			int index = 0;
			while (index != -1) {
				index = inputLine.indexOf(inputSearch, index);
				if (index != -1) {
					
					SearchItem searchItem = sampleTextData.new SearchItem();
					searchItem.setLineOffset(lineOffset);
					searchItem.setCharOffset(index);
					searchItemList.add(searchItem);
					
					index++;
				}
			}
			searchListMap.put(inputSearch, searchItemList);
		}
		searchMapList.add(searchListMap);
	}

	public static void aggregator(ArrayList<Map<String, List<SearchItem>>> searchMapList) {

		Map<String, List<SearchItem>> combinedSearchMap = searchMapList.stream().flatMap(map -> map.entrySet().stream())
				.collect(Collectors.groupingBy(Map.Entry::getKey,
						Collectors.flatMapping(entry -> entry.getValue().stream(), Collectors.toList())));

		System.out.println("Final Map of searched keys - " + combinedSearchMap);
	}

	class SearchItem {

		private long lineOffset;

		private long charOffset;

		public long getLineOffset() {
			return lineOffset;
		}

		public void setLineOffset(long lineOffset) {
			this.lineOffset = lineOffset;
		}

		public long getCharOffset() {
			return charOffset;
		}

		public void setCharOffset(long charOffset) {
			this.charOffset = charOffset;
		}

		public SearchItem() {
			super();
		}

		@Override
		public String toString() {
			return "[lineOffset=" + lineOffset + ", charOffset=" + charOffset + "]";
		}

	}
}
