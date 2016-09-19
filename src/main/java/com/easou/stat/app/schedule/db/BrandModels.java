package com.easou.stat.app.schedule.db;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.nutz.dao.entity.annotation.Column;

public class BrandModels {

	public static BrandModels getInstance(ResultSet rs) throws SQLException {
		BrandModels ude = new BrandModels();
		ude.brand = rs.getString("BRAND");
		ude.models = rs.getString("MODELS");
		return ude;
	} 
	
	@Column("BRAND")
	private String brand;
	@Column("MODELS")
	private String models;
	public String getBrand() {
		return brand;
	}
	public void setBrand(String brand) {
		this.brand = brand;
	}
	public String getModels() {
		return models;
	}
	public void setModels(String models) {
		this.models = models;
	}
	
	@Override
	public String toString() {
		return "JobHistory [BRAND=" + brand + ", MODELS=" + models +  "]";
	}
	
	
}
