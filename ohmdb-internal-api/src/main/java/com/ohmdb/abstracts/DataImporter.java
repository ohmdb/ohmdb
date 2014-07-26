package com.ohmdb.abstracts;

public interface DataImporter {

	void importRecord(long id, byte[] value);

}
