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
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FunctionCallCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;

import java.io.IOException;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class FunctionCallCommandImpl extends CommandImpl implements FunctionCallCommand {

  private String functionName;
  private List<Type> parameterTypes;
  private List<Object> parameterValues;
  private Object result;
  private ProtocolListener listener = new BaseProtocolListener() {

    @Override
    public boolean isComplete() {
      return result != null || error != null || exception != null;
    }

    @Override
    public synchronized void functionResult(Object value) {
      FunctionCallCommandImpl.this.result = value;
      notifyAll();
    }

    @Override
    public synchronized void error(Notice error) {
      FunctionCallCommandImpl.this.error = error;
      notifyAll();
    }

    @Override
    public synchronized void exception(Throwable cause) {
      setException(cause);
      notifyAll();
    }

    @Override
    public void notice(Notice notice) {
      addNotice(notice);
    }

    @Override
    public synchronized void ready(TransactionStatus txStatus) {
      notifyAll();
    }

  };

  public FunctionCallCommandImpl(String functionName, List<Type> parameterTypes, List<Object> parameterValues) {

    this.functionName = functionName;
    this.parameterTypes = parameterTypes;
    this.parameterValues = parameterValues;
  }

  @Override
  public String getFunctionName() {
    return functionName;
  }

  @Override
  public List<Type> getParameterTypes() {
    return parameterTypes;
  }

  @Override
  public List<Object> getParameterValues() {
    return parameterValues;
  }

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public void execute(ProtocolImpl protocol) throws IOException {

    protocol.setListener(listener);

    int procId = protocol.getContext().getRegistry().lookupProcId(functionName);
    if (procId == 0)
      throw new IOException("invalid function name");

    ChannelBuffer msg = ChannelBuffers.dynamicBuffer();

    protocol.writeFunctionCall(msg, procId, parameterTypes, parameterValues);

    protocol.writeSync(msg);

    protocol.send(msg);

    waitFor(listener);

  }

}
