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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * PropertySetter for a bean using a reflection Method
 *
 * @author kdubb
 *
 */
public class MethodPropertySetter implements PropertySetter {

  private boolean primitive;
  private Method method;

  public MethodPropertySetter(Method method) {
    super();
    Class<?>[] parameterTypes = method.getParameterTypes();
    assert parameterTypes.length == 1;
    this.primitive = parameterTypes[0].isPrimitive();

    this.method = method;
    this.method.setAccessible(true);
  }

  @Override
  public void set(Object instance, Object value) {

    if (!primitive || value != null) {
      try {
        method.invoke(instance, value);
      }
      catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
        // Ignore mapping errors (they shouldn't happen)
      }
    }
  }
}
