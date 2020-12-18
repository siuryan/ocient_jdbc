package com.ocient.cli;

public final class FastStringTokenizer {
  private int index;
  private String delim;
  private String string;

  private String[] temp;

  private int limit;

  public FastStringTokenizer(final String string, final String delim, final boolean bool) {
    this.delim = delim;
    this.string = string;
    final char delimiter = delim.charAt(0);

    this.temp = new String[(string.length() >> 1) + 1];
    int wordCount = 0;
    int i = 0;
    int j = string.indexOf(delimiter);

    while (j >= 0) {
      this.temp[wordCount++] = string.substring(i, j);
      i = j + 1;
      j = string.indexOf(delimiter, i);
    }

    if (i < string.length()) {
      this.temp[wordCount++] = string.substring(i);
    }

    this.limit = wordCount;
    this.index = 0;
  }

  public String[] allTokens() {
    final String[] result = new String[this.limit];
    System.arraycopy(this.temp, 0, result, 0, this.limit);
    return result;
  }

  @Override
  public FastStringTokenizer clone() {
    return new FastStringTokenizer(this.string, this.delim, false);
  }

  public int getLimit() {
    return this.limit;
  }

  public boolean hasMoreTokens() {
    return this.index < this.limit;
  }

  public String nextToken() {
    return this.temp[this.index++];
  }

  public final void reuse(final String string, final String delim, final boolean bool) {
    this.delim = delim;
    this.string = string;
    final char delimiter = delim.charAt(0);

    if (this.temp.length < (string.length() >> 1) + 1) {
      this.temp = new String[(string.length() >> 1) + 1];
    }

    int wordCount = 0;
    int i = 0;
    int j = string.indexOf(delimiter);

    while (j >= 0) {
      this.temp[wordCount++] = string.substring(i, j);
      i = j + 1;
      j = string.indexOf(delimiter, i);
    }

    if (i < string.length()) {
      this.temp[wordCount++] = string.substring(i);
    }

    this.limit = wordCount;
    this.index = 0;
  }

  public void setIndex(final int index) {
    this.index = index;
  }
}
