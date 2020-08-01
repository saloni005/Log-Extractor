package parser;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class LogExtractor {

	/**
	 * Class to contain fileName and log line index within that file
	 *
	 */
	class FileInfo {
		public String fileName;
		public long index;

		public FileInfo() {
		}

		public FileInfo(String name, long index) {
			this.fileName = name;
			this.index = index;
		}
	}

	// File name prefix
	private static final String PREFIX = "LogFile-";
	// File extension
	private static final String EXTENSION = ".log";

	// Buffer Size for log capturing within file
	private static final int LOG_BUFFER_SIZE = 100000;

	// Buffer Size for grouping log files for search operation
	private static final long FILE_BUFFER_SIZE = 1000;
	// Last File number in the context
	private static final long LAST_FILE_INDEX = 18203;

	// Objects to store information about start/end log files for printing
	public FileInfo startInfo;
	public FileInfo endInfo;

	/**
	 * Method to get next filename for the given file
	 *
	 * @param fileName
	 * @return
	 */
	private String getNextFileName(String fileName) {
		long fileNum = Long.parseLong(fileName.substring(PREFIX.length(), PREFIX.length() + 6));
		return PREFIX + String.format("%06d", ++fileNum) + EXTENSION;
	}

	/**
	 * Method to get valid file name for given index
	 * 
	 * @param index
	 * @return
	 */
	private String getFileNameWithNum(long index) {
		return PREFIX + String.format("%06d", index) + EXTENSION;
	}

	/**
	 * Method to extract timestamp from log line
	 * 
	 * @param logLine
	 * @return
	 */
	private static ZonedDateTime getDateFromLog(String logLine) {
		return ZonedDateTime.parse(logLine.substring(0, logLine.indexOf(",")));
	}

	/**
	 * Method to set startInfo instance variable Groups of files of size
	 * FILE_BUFFER_SIZE are taken into consideration for applying binary search
	 * operation to search for first log line that matches the given start timestamp
	 * 
	 * If no match is found, return 
	 * (i) First index of first file, if start timestamp
	 * is less than timestamp of first log line.
	 * (ii) null, if start timestamp is
	 * greater than timestamp of last log line of the last file for given buffer
	 * 
	 * @param dir
	 * @param startTime
	 * @param startIndex
	 * @param endIndex
	 * @throws IOException
	 */
	private void getStartValidFile(String dir, ZonedDateTime startTime, long startIndex, long endIndex)
			throws IOException {

		long mid = (startIndex + endIndex) / 2;

		// search for log line within the file with index as mid, returns
		// -2, if left sub-set of file is to be traversed
		// -1, if right sub-set of file is to be traversed
		// index of log line within the matched file, if any file contains the log line
		long comparator = getIndexForValidFile(dir, getFileNameWithNum(mid), startTime);

		if (comparator == -2) {
			// return first log line index if we have reached the first file
			if (mid == 1) {
				this.startInfo = new FileInfo(getFileNameWithNum(mid), 0);
				return;
			}
			// Apply binary search operation on left sub-set of files
			getStartValidFile(dir, startTime, startIndex, mid);
		} else if (comparator == -1) {
			// return null if last file of this buffer is reached and start timestamp is still greater
			if (mid == FILE_BUFFER_SIZE) {
				this.startInfo = null;
				return;
			}
			// Apply binary search operation on right sub-set of files
			getStartValidFile(dir, startTime, mid + 1, endIndex);
		} else {
			// Set startInfo if start timestamp is found in this buffer
			// comparator is the index of log line within the matched file
			this.startInfo = new FileInfo(getFileNameWithNum(mid), comparator);
		}
	}

	/**
	 * Method to set endInfo instance variable Groups of files of size
	 * FILE_BUFFER_SIZE are taken into consideration for applying binary search
	 * operation to search for last log line that matches the given end timestamp
	 * 
	 * If no match is found, return 
	 * (i) null, if end timestamp is less than timestamp of first log line 
	 * of the very first file
	 * (ii) Last index of last file, if end timestamp
	 * is greater than timestamp of last log line of the very last file.
	 * (iii) null, if end timestamp is
	 * greater than timestamp of last log line of the last file for given buffer
	 * 
	 * @param dir
	 * @param endTime
	 * @param startIndex
	 * @param endIndex
	 * @throws IOException
	 */
	private void getEndValidFile(String dir, ZonedDateTime endTime, long startIndex, long endIndex) throws IOException {

		long mid = (startIndex + endIndex) / 2;

		// search for log line within the file with index as mid, returns
		// -2, if left sub-set of file is to be traversed
		// -1, if right sub-set of file is to be traversed
		// index of log line within the matched file, if any file contains the log line
		long comparator = getIndexForValidFile(dir, getFileNameWithNum(mid), endTime);

		if (comparator == -2) {
			// if end timestamp is less than timestamp of first log line 
			// of the very first file
			if (mid == 1) {
				this.endInfo = null;
				return;
			}
			// Apply binary search operation on left sub-set of files
			getEndValidFile(dir, endTime, startIndex, mid);
		} else if (comparator == -1) {
			// return endInfo indicating last index of very last file
			if (mid == LAST_FILE_INDEX) {
				this.endInfo = new FileInfo(getFileNameWithNum(mid), -1);
				return;
			}
			// return null if last file of this buffer is reached and end timestamp is still greater
			if (mid == FILE_BUFFER_SIZE) {
				this.endInfo = null;
				return;
			}
			// Apply binary search operation on right sub-set of files
			getEndValidFile(dir, endTime, mid + 1, endIndex);
		} else {
			// Set endInfo if start timestamp is found in this buffer
			// comparator is the index of log line within the matched file
			this.endInfo = new FileInfo(getFileNameWithNum(mid), comparator);
		}
	}

	/**
	 * Method to search for log line with time stamp inside the given file
	 * Lines are stored in buffer list of size LOG_BUFFER_SIZE
	 * 
	 * Read the first log line and compare the timestamp with given time, 
	 * if given time is less than log time, return -2 indicating that left sub-set of files
	 * need to be traversed
	 * 
	 * if given time is greater than log time, store logs in buffer list of size LOG_BUFFER_SIZE
	 * and compare the timestamp from (LOG_BUFFER_SIZE + 1)th log line with given time,
	 * this operation is repeated till end of file is reached or log time becomes less then given time
	 * 
	 * if log time equals given time return index, indicating the index of first matching log line
	 * 
	 * if time of last log line in buffer is less than given time, return -1, indicating that 
	 * right sub-set of files need to be traversed
	 * else
	 * perform binary search on last buffer, since it must contain the log with given time
	 * and return index of log line within the last buffer
	 * 
	 * @param dir
	 * @param file
	 * @param time
	 * @return
	 * @throws IOException
	 */
	private long getIndexForValidFile(String dir, String file, ZonedDateTime time) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dir.concat(file))));

		int index = 0;
		int comparator = 0;

		String logLine = reader.readLine();

		ZonedDateTime logTime = getDateFromLog(logLine);

		comparator = time.compareTo(logTime);

		if (comparator < 0)
			return -2;

		List<String> str = null;

		while (comparator > 0) {
			str = null;
			str = new ArrayList<String>();
			str.add(logLine);
			for (int i = 1; i < LOG_BUFFER_SIZE; i++) {
				logLine = reader.readLine();

				if (logLine == null) {
					break;
				}
				str.add(logLine);
			}

			index += LOG_BUFFER_SIZE;
			if (logLine == null) {
				break;
			} else {
				logLine = reader.readLine();
			}

			comparator = time.compareTo(getDateFromLog(logLine));
		}

		if (comparator == 0) {
			reader.close();
			return index;
		}

		if (str != null) {
			if (time.compareTo(getDateFromLog(str.get(str.size() - 1))) > 0) {
				reader.close();
				return -1;
			}

			index = index - LOG_BUFFER_SIZE + binarySearch(str, time, 0, str.size() - 1);
		}

		reader.close();
		return index;
	}

	
	/**
	 * Method to perform binary search on log buffer
	 * 
	 * @param arr
	 * @param time
	 * @param start
	 * @param end
	 * @return
	 */
	private int binarySearch(List<String> arr, ZonedDateTime time, int start, int end) {

		int mid = (start + end) / 2;

		ZonedDateTime logTime = getDateFromLog(arr.get(mid));

		if (logTime.compareTo(time) < 0) {
			return binarySearch(arr, time, mid + 1, end);
		} else if (logTime.compareTo(time) > 0) {
			return binarySearch(arr, time, start, mid);
		} else {
			return mid;
		}
	}

	/**
	 * Method to print logs based on startInfo and endInfo
	 * Read startFile and skip the lines till the log line matching the startTime is reached
	 * start writing to output stream till the EOF for startFile is reached, then jump to next file
	 * and call this function recursively for next file, keeping the endInfo fixed.
	 * 
	 * If startFile equals the endFile, print till endIndex of start file, or print the entire file 
	 * if endIndex is set to -1
	 * 
	 * @param out
	 * @param dir
	 * @param startInfo
	 * @param endInfo
	 * @throws IOException
	 */
	private void printLogs(OutputStream out, String dir, FileInfo startInfo, FileInfo endInfo) throws IOException {

		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(dir.concat(startInfo.fileName))));

		// skip printing lines, till the log line matching the startTime is reached 
		for (int i = 0; i < startInfo.index; i++) {
			reader.readLine();
		}

		String logLine = reader.readLine();

		if (startInfo.fileName.equals(endInfo.fileName)) {

			// print logs till endIndex if endIndex is not -1
			if (endInfo.index > 0) {
				long lineCount = endInfo.index - startInfo.index;

				while (lineCount-- > 0) {
					out.write(logLine.getBytes());
					out.write("\n".getBytes());
					logLine = reader.readLine();
				}

				reader.close();
			} else {
				// print all logs of the endFile, if endIndex is -1
				while (logLine != null) {
					out.write(logLine.getBytes());
					out.write("\n".getBytes());
					logLine = reader.readLine();
				}
			}

			return;
		}

		// print logs from startIndex to EOF for startFile
		// when startFile is not equal to endFile
		while (logLine != null) {
			out.write(logLine.getBytes());
			out.write("\n".getBytes());
			logLine = reader.readLine();
		}

		// call this function recursively, when EOF for current file is reached
		if (logLine == null) {
			printLogs(out, dir, new FileInfo(getNextFileName(startInfo.fileName), 0), endInfo);
		}

		reader.close();
	}

	public static void main(String[] args) throws IOException {

		if (args.length != 6) {
			System.out.println("Invalid Number of arguments provided...");
			System.exit(1);
		}

		String startTime = null;
		String endTime = null;
		String logDir = null;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-f")) {
				startTime = args[++i];
			}
			if (args[i].equals("-t")) {
				endTime = args[++i];
			}
			if (args[i].equals("-i")) {
				logDir = args[++i];
			}
		}

		// fix the directory string, add '/' if missing
		if (logDir.charAt(logDir.length() - 1) != '/') {
			logDir.concat("/");
		}

		LogExtractor parser = new LogExtractor();

		long fileIndex = 1;
		
		// start with the first file buffer
		parser.getStartValidFile(logDir, ZonedDateTime.parse(startTime), fileIndex, FILE_BUFFER_SIZE);
		fileIndex += FILE_BUFFER_SIZE;

		// pick next buffer, if startInfo was not set in last buffer
		while (parser.startInfo == null && fileIndex < LAST_FILE_INDEX) {
			parser.getStartValidFile(logDir, ZonedDateTime.parse(startTime), fileIndex,
					fileIndex + FILE_BUFFER_SIZE - 1);
			
			fileIndex += FILE_BUFFER_SIZE;
		}

		// if fileIndex crosses total number of files
		if (fileIndex > LAST_FILE_INDEX) {
			// set fileIndex to previous buffer end index
			fileIndex = fileIndex - FILE_BUFFER_SIZE;
			
			parser.getStartValidFile(logDir, ZonedDateTime.parse(startTime), fileIndex, LAST_FILE_INDEX);
		}

		if (parser.startInfo == null) {
			System.out.println("No Logs found from given timestamp range!!");
		} else {
			fileIndex = 1;
			
			// start with the first file buffer
			parser.getEndValidFile(logDir, ZonedDateTime.parse(endTime), fileIndex, FILE_BUFFER_SIZE);
			fileIndex += FILE_BUFFER_SIZE;

			// pick next buffer, if endInfo was not set in last buffer
			while (parser.endInfo == null && fileIndex < LAST_FILE_INDEX) {
				parser.getEndValidFile(logDir, ZonedDateTime.parse(endTime), fileIndex,
						fileIndex + FILE_BUFFER_SIZE - 1);
				fileIndex += FILE_BUFFER_SIZE;
			}

			// if fileIndex crosses total number of files
			if (fileIndex > LAST_FILE_INDEX) {
				fileIndex = fileIndex - FILE_BUFFER_SIZE;
				parser.getEndValidFile(logDir, ZonedDateTime.parse(endTime), fileIndex, LAST_FILE_INDEX);
			}

			OutputStream out = new BufferedOutputStream(System.out);

			parser.printLogs(out, logDir, parser.startInfo, parser.endInfo);

			out.flush();
		}
	}
}
