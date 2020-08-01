# Log-Extractor


*******************************************************************************************************************************************************************************
Problem Statement: <br>
Mostly we use our log database to query our logs, but every now and then, we may have to query older logs for customer support or debugging. In most of these cases we know the time range for which we need to analyze the logs. What we now need is a tool that could extract the log lines from given time range and print it to console in time effective manner.

The command line (CLI) for the desired program is as below

LogExtractor.exe -f "From Time" -t "To Time" -i "Log file directory location"

All time formats will be "ISO 8601" format.

The extraction process should complete in few seconds minimizing engineer's wait time.

Log file format <br>
The log file has one log per line. <br>
Every log line will start with TimeStamp in "ISO 8601" format followed by comma (','). <br>
All the log lines will be separated by single new line character '\n'. <br>
Example logline: <br>
2020-01-31T20:12:38.1234Z,Some Field,Other Field, And so on, Till new line,...\n 
*******************************************************************************************************************************************************************************






As per the problem statement, it is clear that the log file size is going to be very large. So we need an efficient approach for performing the I/O operations. Also, the timestamp format as provided in command line argument must be compatible with the timestamp label in the log lines.

Java.io library is used to handle the I/O operations efficiently. Since the timestamp format is “ISO 8601”, java.time.ZonedDateTime is used. FileInputStream is used to read files and stored in BufferedReader using InputStreamReader.

Since the timestamp will always be stored in increasing order, we can apply binary search on the number of files in order to select the files containing the logs in range of the input parameters provided. We store the start point and end point of logs falling in the desired range in a data structure which contains 2 fields i.e. file name and index of log within the file.

As the number of files is large, we apply binary search operation on 1000 files at a time. For selection of a single log file, we perform the below operation on each selected file using divide and conquer. 
To get the start/end point of log containing required timestamp, every time we will load the buffer with 1,00,000 logs at a time. If the buffer contains the desired timestamp, then we perform binary search on it to get the exact index of the log.

Analysis: Binary search operations to select file as well as desired log line within the file outperforms linear search operations drastically. Also, using buffers for file selection ensures that every time a subset of total files is taken into consideration.

*******************************************************************************************************************************************************************************
