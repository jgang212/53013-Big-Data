package edu.uchicago.mpcs53013.ingestData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import org.json.simple.JSONValue;
import org.json.simple.JSONObject;

import edu.uchicago.mpcs53013.redditSummary.RedditSummary;


public abstract class RedditSummaryProcessor {
	static class MissingDataException extends Exception {

	    public MissingDataException(String message) {
	        super(message);
	    }

	    public MissingDataException(String message, Throwable throwable) {
	        super(message, throwable);
	    }

	}

	void processLine(String line, File file) throws IOException {
		try {
			processRedditSummary(redditFromLine(line), file);
		} catch(MissingDataException e) {
			// Just ignore lines with missing data
		}
	}

	abstract void processRedditSummary(RedditSummary summary, File file) throws IOException;
	BufferedReader getFileReader(File file) throws FileNotFoundException, IOException {
		if(file.getName().endsWith(".gz"))
			return new BufferedReader
					     (new InputStreamReader
					    		 (new GZIPInputStream
					    				 (new FileInputStream(file))));
		return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}
	
	void processFile(File file) throws IOException {		
		BufferedReader br = getFileReader(file);
		br.readLine(); // Discard header
		String line;
		while((line = br.readLine()) != null) {
			processLine(line, file);
		}
	}

	void processDirectory(String directoryName) throws IOException {
		File directory = new File(directoryName);
		File[] directoryListing = directory.listFiles();
		for(File file : directoryListing)
			processFile(file);
	}
	
	RedditSummary redditFromLine(String line) throws MissingDataException {
		Object obj = JSONValue.parse(line);
		JSONObject jsonObject = (JSONObject) obj;
		Long gilded;
		Long num_comments;
		Long score;
		String selftext;
		String subreddit;
		String title;
		try
		{
			gilded = (Long) jsonObject.get("gilded");
			num_comments = (Long) jsonObject.get("num_comments");
			score = (Long) jsonObject.get("score");
			selftext = (String) jsonObject.get("selftext");
			subreddit = (String) jsonObject.get("subreddit");
			title = (String) jsonObject.get("title");
		}
		catch (NullPointerException e)
		{
			throw new MissingDataException("Incomplete data"); 
		}
		if (gilded == null || num_comments == null || score == null 
				|| selftext == null || subreddit == null || title == null)
		{
			throw new MissingDataException("Incomplete data");
		}
		
		RedditSummary summary 
			= new RedditSummary(gilded, num_comments, score, selftext, subreddit, title);
		return summary;
	}
}
