package org.apache.hadoop.conf;

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

public class XmlConfigTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileName = "E:\\workspaces\\datastat\\src\\main\\resources\\parameters_day_client.xml";
		XMLConfiguration c = null;
		try {
			if (c == null) {
				c = new XMLConfiguration(fileName);
				System.out.println("加载配置[catch]" + fileName);
				List<HierarchicalConfiguration> fields = c.configurationsAt("properties.indicators(0).indicator");
				System.out.println(fields.size());
				for(HierarchicalConfiguration field : fields) {
					System.out.println(field.getString("name"));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
