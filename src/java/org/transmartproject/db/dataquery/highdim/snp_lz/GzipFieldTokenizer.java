/*
 * Copyright © 2013-2015 The Hyve B.V.
 *
 * This file is part of transmart-core-db.
 *
 * Transmart-core-db is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * transmart-core-db.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.transmartproject.db.dataquery.highdim.snp_lz;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Tokenizes a Blob field.
 */
//@CompileStatic
public class GzipFieldTokenizer {
  public String version = "Java version";

  private Blob blob;
  private int expectedSize;

  public GzipFieldTokenizer(Blob blob, int expectedSize) {
    this.blob = blob;
    this.expectedSize = expectedSize;
  }

  private void withTokens(Function<String, Object> action) throws SQLException, IOException {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(blob.getBinaryStream()), Charsets.US_ASCII));

    try {
      StringBuilder builder = new StringBuilder();
      int c, size = 0;
      while ((c = reader.read()) >= 0) {
        if ((char) c == ' ') {
          size++;
          if (size > expectedSize - 1) {
            throw new InputMismatchException("Got more tokens than the "+expectedSize+" expected");
          }
          action.apply(builder.toString());
          builder.setLength(0);
        } else {
          builder.append((char) c);
        }
      }

      if (size > 0 || builder.length() > 0) size += 1;
      // check this first to make sure we don't call action once too much
      if (size != expectedSize) {
        throw new InputMismatchException("Expected "+expectedSize+" tokens, but got only "+size);
      }
      if (size > 0) {
        action.apply(builder.toString());
      }
    } finally {
      reader.close();
    }
  }

  public double[] asDoubleArray() throws SQLException, IOException {
    final double[] res = new double[expectedSize];
    withTokens(new Function<String,Object>(){
      int i = 0;
      public Object apply(String tok) {
        res[i++] = Double.parseDouble(tok);
        return null;
      }});
    return res;
  }

  /**
   * @throws InputMismatchException iff the number of values read &ne; <var>expectedSize</var>.
   * @return a list of strings.
   */
  public List<String> asStringList() throws SQLException, IOException{
    final ArrayList<String> res = new ArrayList<String>(expectedSize);
    withTokens(new Function<String,Object>(){ public Object apply(String tok) {
      res.add(tok);
      return null;
    }});
    return res;
  }

}
