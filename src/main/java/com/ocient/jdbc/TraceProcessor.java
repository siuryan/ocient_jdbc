package com.ocient.jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class TraceProcessor
{
	static HashMap<Integer, PrintWriter> outFiles = new HashMap<>();
	static int lastThreadId = 0;

	public static void main(final String[] args)
	{
		if (args.length != 1)
		{
			System.out.println("Usage: TraceProcessor <trace input file>");
			System.exit(1);
		}

		final String inFile = args[0];
		BufferedReader reader;
		try
		{
			reader = new BufferedReader(new FileReader(inFile));
			String line = reader.readLine();
			while (line != null)
			{
				// 09:41:31 [19] com.ocient.jdbc.XGConnection isClosed INFO: Called isClosed()
				final int start = 10;
				final int end = line.indexOf(']');
				int threadId = 0;

				if (line.length() < start || line.charAt(start - 1) != '[' || end == -1)
				{
					threadId = lastThreadId;
				}
				else
				{
					threadId = Integer.parseInt(line.substring(start, end));
				}

				PrintWriter outFile = outFiles.get(threadId);
				if (outFile == null)
				{
					final FileWriter writer = new FileWriter("thread" + threadId + ".txt", false);
					outFile = new PrintWriter(writer);
					outFiles.put(threadId, outFile);
				}

				outFile.println(line);
				line = reader.readLine();
				lastThreadId = threadId;
			}

			reader.close();

			for (final Map.Entry<Integer, PrintWriter> entry : outFiles.entrySet())
			{
				entry.getValue().close();
			}
		}
		catch (final IOException e)
		{
			e.printStackTrace();
		}
	}
}
