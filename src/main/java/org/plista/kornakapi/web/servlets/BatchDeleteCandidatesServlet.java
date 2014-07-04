/**
 * Copyright 2012 plista GmbH  (http://www.plista.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 */

package org.plista.kornakapi.web.servlets;

import com.google.common.io.Closeables;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.plista.kornakapi.core.Candidate;
import org.plista.kornakapi.core.io.CSVCandidateFileIterator;
import org.plista.kornakapi.core.storage.Storage;
import org.plista.kornakapi.web.Parameters;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/** servlet to delete items from a candidate set in batch */
public class BatchDeleteCandidatesServlet extends BaseServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    int batchSize = getParameterAsInt(request, Parameters.BATCH_SIZE, Parameters.DEFAULT_BATCH_SIZE);

    ServletFileUpload upload = new ServletFileUpload();

    FileItemIterator fileItems;
    InputStream in = null;

    boolean fileProcessed = false;

    Storage storage = this.getDomainIndependetStorage();
    try {
      fileItems = upload.getItemIterator(request);
      while (fileItems.hasNext()) {

        FileItemStream item = fileItems.next();

        if (Parameters.FILE.equals(item.getFieldName()) && !item.isFormField()) {

          in = item.openStream();
          Iterator<Candidate> candidates = new CSVCandidateFileIterator(in);

          storage.batchDeleteCandidates(candidates, batchSize);

          fileProcessed = true;

          break;
        }
      }
    } catch (FileUploadException e) {
      throw new IOException(e);
    } finally {
      Closeables.closeQuietly(in);
    }

    if (!fileProcessed) {
      throw new IllegalStateException("Unable to find supplied data file!");
    }
  }
}
