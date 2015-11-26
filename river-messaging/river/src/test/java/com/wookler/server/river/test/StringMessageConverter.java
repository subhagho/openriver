/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.river.test;

import com.wookler.server.river.ByteConvertor;

import java.io.UnsupportedEncodingException;

/**
 * String message converter implementation to convert String message to byte
 * array and vice versa
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 20/08/14
 */
public class StringMessageConverter extends ByteConvertor<String> {
    @Override
    protected byte[] data(String message) throws ConversionException {
        try {
            return message.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ConversionException("Error converting message string.", e);
        }
    }

    @Override
    protected String message(byte[] data) throws ConversionException {
        try {
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ConversionException("Error converting message string.", e);
        }
    }
}
