package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandComplete;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.CommandError;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.ReportNotice;

import java.io.IOException;


/**
 * Generates a query request (usually "BEGIN").
 *
 * Commands are not sync'ed and errors/notices are passed to the following
 * protocol handler. This allows it to prefix whatever command comes next
 * without waiting for completion.
 */
public class LazyQueryRequest implements ServerRequest {

  private String query;

  public LazyQueryRequest(String query) {
    this.query = query;
  }

  class Handler implements CommandComplete, CommandError, ReportNotice {

    @Override
    public String toString() {
      return "Lazy Execute Query";
    }

    @Override
    public Action commandComplete(String command, Long rowsAffected, Long insertedOid) {
      return Action.Complete;
    }

    @Override
    public Action error(Notice notice) {
      // Pass error to next handler, cause it to believe an error occurred in its command
      return Action.CompletePassing;
    }

    @Override
    public Action notice(Notice notice) {
      // Pass any notices to the next handler, still need to wait for command complete.
      return Action.ResumePassing;
    }

    @Override
    public void exception(Throwable cause) {
    }

  }

  @Override
  public Handler createHandler() {
    return new Handler();
  }

  @Override
  public void execute(ProtocolChannel channel) throws IOException {

    channel
        .writeQuery(query);

  }

}
