package io.vertx.amqpbridge.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 
 * @author <a href="mailto:rajith@rajith.lk">Rajith Muditha Attapattu</a>
 * 
 */
public class RouteEntry {

	Pattern _pattern;

	List<String> _addressList = new ArrayList<String>();

	public RouteEntry(Pattern p, String addr) {
		_pattern = p;
		_addressList.add(addr);
	}

	public void add(String addr) {
		_addressList.add(addr);
	}

	public void remove(String addr) {
		_addressList.remove(addr);
	}

	public Pattern getPattern() {
		return _pattern;
	}

	public List<String> getAddressList() {
		return _addressList;
	}

	public int getAddressListSize() {
		return _addressList.size();
	}

	@Override
	public String toString() {
		return String.format("[pattern=%s, address-list=%s", _pattern, _addressList);
	}
}