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
package io.zeebe.client.event;

public interface PollableTopicSubscription
{
    /**
     * @return true if this subscription currently receives events
     */
    boolean isOpen();

    /**
     * @return true if this subscription is not open and is not in the process of opening or closing
     */
    boolean isClosed();

    /**
     * Closes the subscription. Blocks until all pending events have been handled.
     */
    void close();

    /**
     * Handles currently pending events by invoking the supplied event handler.
     *
     * @param eventHandler the handler that is invoked for each event
     * @return number of handled events
     */
    int poll(TopicEventHandler eventHandler);

}
