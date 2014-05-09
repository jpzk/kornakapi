package org.plista.kornakapi.web.servlets;

import org.plista.kornakapi.web.Parameters;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/** servlet to delete all items of a given candidate set */
public class DeleteAllCandidatesServlet extends BaseServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    String label = getParameter(request, Parameters.LABEL, true);

    storages().get(label).deleteAllCandidates(label);
  }

}
