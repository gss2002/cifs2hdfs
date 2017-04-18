package org.apache.hadoop.cifs;

import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFilenameFilter;

public class CifsFileFilter implements SmbFilenameFilter {
	protected String wildcard;

	public CifsFileFilter(String wildcard) {
		this.wildcard = wildcard;
	}

	@Override
	public boolean accept(SmbFile dir, String name) throws SmbException {
		// TODO Auto-generated method stub
		if (this.wildcard.contains("*")) {
			return name.contains(wildcard.replace("*", ""));
		} else {
			return name.equals(wildcard);
		}
	}
}
