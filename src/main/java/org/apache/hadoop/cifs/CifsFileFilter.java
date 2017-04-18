package org.apache.hadoop.cifs;

import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFilenameFilter;

public class CifsFileFilter implements SmbFilenameFilter {
    private final String regex;

    public CifsFileFilter(String filename) {
        regex = filename;
    }

    @Override
    public boolean accept(SmbFile dir, String name) throws SmbException {
        return name.matches(regex);
    }

}
