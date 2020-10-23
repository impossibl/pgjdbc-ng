/*
 * Copyright 2017, OnGres.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */


package com.impossibl.postgres.protocol.sasl.scram.util;


/**
 * Simple methods similar to Precondition class. Avoid importing full library.
 */
public class Preconditions {
    /**
     * Checks that the argument is not null.
     * @param value The value to be checked
     * @param valueName The name of the value that is checked in the method
     * @param <T> The type of the value
     * @return The same value passed as argument
     * @throws IllegalArgumentException If value is null
     */
    public static <T> T checkNotNull(T value, String valueName) throws IllegalArgumentException {
        if(null == value) {
            throw new IllegalArgumentException("Null value for '" + valueName + "'");
        }

        return value;
    }

    /**
     * Checks that the String is not null and not empty
     * @param value The String to check
     * @param valueName The name of the value that is checked in the method
     * @return The same String passed as argument
     * @throws IllegalArgumentException If value is null or empty
     */
    public static String checkNotEmpty(String value, String valueName) throws IllegalArgumentException {
        if(checkNotNull(value, valueName).isEmpty()) {
            throw new IllegalArgumentException("Empty string '" + valueName + "'");
        }

        return value;
    }

    /**
     * Checks that the argument is valid, based in a check boolean condition.
     * @param check The boolean check
     * @param valueName The name of the value that is checked in the method
     * @throws IllegalArgumentException
     */
    public static void checkArgument(boolean check, String valueName) throws IllegalArgumentException {
        if(! check) {
            throw new IllegalArgumentException("Argument '" + valueName + "' is not valid");
        }
    }

    /**
     * Checks that the integer argument is positive.
     * @param value The value to be checked
     * @param valueName The name of the value that is checked in the method
     * @return The same value passed as argument
     * @throws IllegalArgumentException If value is null
     */
    public static int gt0(int value, String valueName) throws IllegalArgumentException {
        if(value <= 0) {
            throw new IllegalArgumentException("'" + valueName + "' must be positive");
        }

        return value;
    }
}
