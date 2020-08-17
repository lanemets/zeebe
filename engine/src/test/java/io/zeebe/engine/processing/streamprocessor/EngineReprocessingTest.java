/*
 * Copyright © 2020  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package io.zeebe.engine.processing.streamprocessor;

import static io.zeebe.protocol.record.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATED;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.stream.StreamWrapperException;
import java.util.stream.IntStream;
import org.assertj.core.internal.bytebuddy.utility.RandomString;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class EngineReprocessingTest {

  private static final String PROCESS_ID = "process";
  private static final String ELEMENT_ID = "task";
  private static final String JOB_TYPE = "test";
  private static final String INPUT_COLLECTION_VARIABLE = "items";

  private static final BpmnModelInstance SIMPLE_FLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent().endEvent().done();
  @Rule public EngineRule engineRule = EngineRule.singlePartition();

  @Before
  public void init() {
    engineRule.deployment().withXmlResource(SIMPLE_FLOW).deploy();
    IntStream.range(0, 100)
        .forEach(
            i ->
                engineRule
                    .workflowInstance()
                    .ofBpmnProcessId(PROCESS_ID)
                    .withVariable("data", RandomString.make(1024))
                    .create());

    Awaitility.await()
        .until(
            () ->
                RecordingExporter.workflowInstanceRecords()
                    .withElementType(BpmnElementType.PROCESS)
                    .withIntent(ELEMENT_ACTIVATED)
                    .count(),
            (count) -> count == 100);

    engineRule.stop();
  }

  @Test
  public void shouldReprocess() {
    // given - reprocess
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();

    // when - then
    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);
  }

  @Test
  public void shouldContinueProcessingAfterReprocessing() {
    // given - reprocess
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();

    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);

    // when - then
    engineRule.workflowInstance().ofBpmnProcessId(PROCESS_ID).create();
  }

  @Test
  public void shouldNotContinueProcessingWhenPausedDuringReprocessing() {
    // given - reprocess
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();
    engineRule.pauseProcessing(1);

    // when
    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);

    // then
    Assert.assertThrows(
        StreamWrapperException.class,
        () -> engineRule.workflowInstance().ofBpmnProcessId(PROCESS_ID).create());
  }

  @Test
  public void shouldContinueAfterReprocessWhenProcessingWasResumed() {
    // given
    final int lastSize = RecordingExporter.getRecords().size();
    // we need to reset the record exporter
    RecordingExporter.reset();
    engineRule.start();
    engineRule.pauseProcessing(1);
    engineRule.resumeProcessing(1);

    Awaitility.await("Await reprocessing of " + lastSize)
        .until(() -> RecordingExporter.getRecords().size(), (size) -> size >= lastSize);

    // when
    engineRule.workflowInstance().ofBpmnProcessId(PROCESS_ID).create();
  }
}
