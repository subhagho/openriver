/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.river;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.config.CPath;

/**
 * Interface to be implemented to handle message file recycle.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 15/08/14
 */
@CPath(path = "recycle")
public interface RecycleStrategy extends Configurable {
	/**
	 * Check if the current {@link MessageBlock} needs to be recycled.
	 *
	 * @param block
	 *            - Current message block.
	 * @return - Needs recycle?
	 */
	public boolean recycle(MessageBlock block);
}
