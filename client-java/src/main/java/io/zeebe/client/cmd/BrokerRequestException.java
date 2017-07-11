/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.client.cmd;

import io.zeebe.protocol.clientapi.ErrorCode;

public class BrokerRequestException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public static final String ERROR_MESSAGE_FORMAT = "Request exception (%s): %s\n";

    protected final ErrorCode errorCode;
    protected final String errorMessage;

    public BrokerRequestException(final ErrorCode errorCode, final String errorMessage)
    {
        super(String.format(ERROR_MESSAGE_FORMAT, errorCode, errorMessage));

        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public String getErrorMessage()
    {
        return errorMessage;
    }
}
