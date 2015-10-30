/*
 * Copyright [2014] Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.river.map;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 10/12/14
 */
@SuppressWarnings("serial")
public class MapException extends Exception {
    private static final String _PREFIX_ = "Persisted Map exception : ";

    public MapException(String mesg) {
        super(String.format("%s %s",  _PREFIX_, mesg));
    }

    public MapException(String mesg, Throwable inner) {
        super(String.format("%s %s", _PREFIX_, mesg), inner);
    }
}
