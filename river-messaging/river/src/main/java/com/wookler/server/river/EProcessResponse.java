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

/**
 * Enumerates the setState of a process/thread. This should be used only for
 * execution units.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/08/14
 */
public enum EProcessResponse {
	/**
	 * Task failed, but non-terminating failure. Eg. records errors.
	 */
	Failed, /**
			 * Task completed successfully.
			 */
	Success, /**
				 * Task aborted. Caused by thread interrupts, etc.
				 */
	Aborted, /**
				 * Task raised a framework getError.
				 */
	Exception,

	/**
	 * Elements in the list have partial errors.
	 */
	SuccessWithErrors;
}
