/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.mapper;

import static java.util.Arrays.asList;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.protocol.ResultField;


/**
 * Builds lists of PropertySetters that match the a list of result fields. This
 * accelerates the mapping from fields to arrays/lists/maps/beans.
 * 
 * @author kdubb
 *
 */
public class Mapper {
	
	/**
	 * Builds a list of mapping setters for the row and fields.
	 * 
	 * @param rowType Row type to map to
	 * @param fields Result fields to map from
	 * @return List of setters that can perform the mapping
	 */
	public static List<PropertySetter> buildMapping(Class<?> rowType, List<ResultField> fields) {

		PropertySetter[] setters;
		
		if(rowType.isArray() && rowType.getComponentType() == Object.class) {
			setters = initArraySetters(fields);
		}
		else if(List.class.isAssignableFrom(rowType)) {
			setters = initListSetters(fields);
		}
		else if(Map.class.isAssignableFrom(rowType)) {
			setters = initMapSetters(fields);
		}
		else {
			setters = initBeanSetters(rowType, fields);
		}
		
		return asList(setters);
	}
	
	/**
	 * Builds a list of mapping setters for an array row type.
	 * 
	 * @param fields Fields to map from
	 * @return List of setters that can perform the mapping
	 */
	protected static PropertySetter[] initArraySetters(List<ResultField> fields) {
		
		PropertySetter[] setters = new PropertySetter[fields.size()];
		
		for(int c=0; c < setters.length; ++c) {
			setters[c] = new ArrayPropertySetter(c);
		}
		
		return setters;
	}

	/**
	 * Builds a list of mapping setters for a list row type.
	 * 
	 * @param fields Fields to map from
	 * @return List of setters that can perform the mapping
	 */
	protected static PropertySetter[] initListSetters(List<ResultField> fields) {
		
		PropertySetter[] setters = new PropertySetter[fields.size()];
		
		for(int c=0; c < setters.length; ++c) {
			setters[c] = new ListPropertySetter(c);
		}
		
		return setters;
	}

	/**
	 * Builds a list of mapping setters for a map row type.
	 * 
	 * @param fields Fields to map from
	 * @return List of setters that can perform the mapping
	 */
	protected static PropertySetter[] initMapSetters(List<ResultField> fields) {
		
		PropertySetter[] setters = new PropertySetter[fields.size()];
		
		for(int c=0; c < setters.length; ++c) {
			setters[c] = new MapPropertySetter(fields.get(c).name);
		}
		
		return setters;
	}

	/**
	 * Builds a list of mapping setters for bean row type.
	 * 
	 * @param instanceType The bean instance type to map to
	 * @param fields List of setters that can perform the mapping
	 * @return List of setters that can perform the mapping 
	 */
	protected static PropertySetter[] initBeanSetters(Class<?> instanceType, List<ResultField> fields) {

		PropertySetter[] setters = new PropertySetter[fields.size()];
		
		//Get the bean info
		PropertyDescriptor[] propDescs;
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(instanceType);
			propDescs = beanInfo.getPropertyDescriptors();
		}
		catch(IntrospectionException e) {
			//Ignore...
			propDescs = new PropertyDescriptor[0];
		}

		for(int c=0; c < setters.length; ++c) {

			//Look for a valid property to map to
			PropertyDescriptor propDesc = findPropertyDescriptor(propDescs, fields.get(c).name);
			if(propDesc != null) {
				setters[c] = new MethodPropertySetter(propDesc.getWriteMethod());
				continue;
			}
			
			//Look for a valid field to map to
			Field field = findField(instanceType, fields.get(c).name);
			if(field != null) {
				setters[c] = new FieldPropertySetter(field);
				continue;
			}
			
			//Can't find a valid property setter
			setters[c] = null;
		}
		
		return setters;
	}

	private static Field findField(Class<?> instanceType, String name) {

		try {
			return instanceType.getField(name);
		}
		catch(NoSuchFieldException | SecurityException e) {
		}

		return null;
	}

	private static PropertyDescriptor findPropertyDescriptor(PropertyDescriptor[] propDescs, String name) {

		for(PropertyDescriptor propDesc : propDescs) {
			
			if(propDesc.getName().equals(name)) {
				return propDesc;
			}
			
		}
		
		return null;
	}

}
