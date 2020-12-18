package com.ocient.jdbc;

public class StPoint {
  private double lon;
  private double lat;

  public StPoint(double lon, double lat) {
    this.lon = lon;
    this.lat = lat;
  }

  public double getLongitude() {
    return lon;
  }

  public double getLatitude() {
    return lat;
  }

  public double getX() {
    return lon;
  }

  public double getY() {
    return lat;
  }

  public String toString() {
    return "(" + lat + ", " + lon + ")";
  }
}
