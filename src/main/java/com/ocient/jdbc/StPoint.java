package com.ocient.jdbc;

public class StPoint {
  private final double lon;
  private final double lat;

  public StPoint(final double lon, final double lat) {
    this.lon = lon;
    this.lat = lat;
  }

  public double getLongitude() {
    return this.lon;
  }

  public double getLatitude() {
    return this.lat;
  }

  public double getX() {
    return this.lon;
  }

  public double getY() {
    return this.lat;
  }

  @Override
  public String toString() {
    return "(" + this.lat + ", " + this.lon + ")";
  }
}
