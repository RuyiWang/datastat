package com.easou.stat.app.schedule.db;

import java.util.ArrayList;
import java.util.List;

public class EventDefineModel {
	private String id;
	private String type;
	private String appkey;
	private List<DimDefineModel> dims = new ArrayList<DimDefineModel>();
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<DimDefineModel> getDims() {
		return dims;
	}
	
	public void addDim(DimDefineModel dim) {
		dims.add(dim);
	}


	public String getAppkey() {
		return appkey;
	}

	public void setAppkey(String appkey) {
		this.appkey = appkey;
	}





	public class DimDefineModel {
		private String id;
		private boolean all;
		private List<StatDefineModel> stats = new ArrayList<StatDefineModel>();
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public boolean isAll() {
			return all;
		}
		public void setAll(boolean all) {
			this.all = all;
		}
		public void AddStat(StatDefineModel stat) {
			stats.add(stat);
		}
		public List<StatDefineModel> getStats() {
			return stats;
		}
		@Override
		public String toString() {
			return "DimDefineModel [all=" + all + ", id=" + id + ", stats="
					+ stats + "]";
		}
	}
	
	public class StatDefineModel {
		private String id;
		private String method;
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getMethod() {
			return method;
		}
		public void setMethod(String method) {
			this.method = method;
		}
		@Override
		public String toString() {
			return "StatDefineModel [id=" + id + ", method=" + method + "]";
		}
	}

	@Override
	public String toString() {
		return "EventDefineModel [dims=" + dims + ", id=" + id + ", type="
				+ type + "appkey" + appkey +"]";
	}
	
}
