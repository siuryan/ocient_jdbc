package com.ocient.cli;

public final class FastStringTokenizer
{
	private int index;
	private String delim;
	private String string;

	private String[] temp;

	private int limit;

	public FastStringTokenizer(final String string, final String delim, final boolean bool)
	{
		this.delim = delim;
		this.string = string;
		final char delimiter = delim.charAt(0);

		temp = new String[(string.length() >> 1) + 1];
		int wordCount = 0;
		int i = 0;
		int j = string.indexOf(delimiter);

		while (j >= 0)
		{
			temp[wordCount++] = string.substring(i, j);
			i = j + 1;
			j = string.indexOf(delimiter, i);
		}

		if (i < string.length())
		{
			temp[wordCount++] = string.substring(i);
		}

		limit = wordCount;
		index = 0;
	}

	public String[] allTokens()
	{
		final String[] result = new String[limit];
		System.arraycopy(temp, 0, result, 0, limit);
		return result;
	}

	@Override
	public FastStringTokenizer clone()
	{
		return new FastStringTokenizer(string, delim, false);
	}

	public int getLimit()
	{
		return limit;
	}

	public boolean hasMoreTokens()
	{
		return index < limit;
	}

	public String nextToken()
	{
		return temp[index++];
	}

	public void reuse(final String string, final String delim, final boolean bool)
	{
		this.delim = delim;
		this.string = string;
		final char delimiter = delim.charAt(0);

		if (temp.length < (string.length() >> 1) + 1)
		{
			temp = new String[(string.length() >> 1) + 1];
		}

		int wordCount = 0;
		int i = 0;
		int j = string.indexOf(delimiter);

		while (j >= 0)
		{
			temp[wordCount++] = string.substring(i, j);
			i = j + 1;
			j = string.indexOf(delimiter, i);
		}

		if (i < string.length())
		{
			temp[wordCount++] = string.substring(i);
		}

		limit = wordCount;
		index = 0;
	}

	public void setIndex(final int index)
	{
		this.index = index;
	}
}
