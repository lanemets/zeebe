/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.subprocess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.EmbeddedSubProcessBuilder;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.model.bpmn.builder.StartEventBuilder;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.TimerIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.JobBatchRecordValue;
import io.zeebe.protocol.record.value.VariableDocumentUpdateSemantic;
import io.zeebe.protocol.record.value.WorkflowInstanceRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedWorkflow;
import io.zeebe.test.util.BrokerClassRuleHelper;
import io.zeebe.test.util.record.RecordingExporter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class EventSubprocessTest {
  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String PROCESS_ID = "proc";

  private static String messageName;

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

  @Parameterized.Parameter public String testName;

  @Parameterized.Parameter(1)
  public Function<StartEventBuilder, StartEventBuilder> builder;

  @Parameterized.Parameter(2)
  public Consumer<Long> triggerEventSubprocess;

  private DeployedWorkflow currentWorkflow;

  @Parameterized.Parameters(name = "{0} event subprocess")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "timer",
        eventSubprocess(s -> s.timerWithDuration("PT60S")),
        eventTrigger(
            key -> {
              assertThat(
                      RecordingExporter.timerRecords(TimerIntent.CREATED)
                          .withWorkflowInstanceKey(key)
                          .exists())
                  .describedAs("Expected timer to exist")
                  .isTrue();
              ENGINE.increaseTime(Duration.ofSeconds(60));
            })
      },
      {
        "message",
        eventSubprocess(s -> s.message(b -> b.name(messageName).zeebeCorrelationKey("key"))),
        eventTrigger(
            key -> {
              RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
                  .withWorkflowInstanceKey(key)
                  .withMessageName(messageName)
                  .await();
              ENGINE.message().withName(messageName).withCorrelationKey("123").publish();
            })
      },
    };
  }

  private static Function<StartEventBuilder, StartEventBuilder> eventSubprocess(
      final Function<StartEventBuilder, StartEventBuilder> consumer) {
    return consumer;
  }

  private static Consumer<Long> eventTrigger(final Consumer<Long> eventTrigger) {
    return eventTrigger;
  }

  @Before
  public void init() {
    messageName = helper.getMessageName();
  }

  @Test
  public void shouldTriggerEventSubprocess() {
    // when
    final BpmnModelInstance model = eventSubprocModel(false, builder);
    final long wfInstanceKey = createInstanceAndTriggerEvent(model);

    // then
    final Record<WorkflowInstanceRecordValue> eventOccurred =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.EVENT_OCCURRED)
            .withWorkflowInstanceKey(wfInstanceKey)
            .getFirst();
    Assertions.assertThat(eventOccurred.getValue())
        .hasWorkflowKey(currentWorkflow.getWorkflowKey())
        .hasWorkflowInstanceKey(wfInstanceKey)
        .hasBpmnElementType(BpmnElementType.START_EVENT)
        .hasElementId("event_sub_start")
        .hasVersion(currentWorkflow.getVersion())
        .hasFlowScopeKey(wfInstanceKey);

    assertEventSubprocessLifecycle(wfInstanceKey);
  }

  @Test
  public void shouldNotInterruptParentProcess() {
    // when
    final BpmnModelInstance model = eventSubprocModel(false, builder);
    final long wfInstanceKey = createInstanceAndTriggerEvent(model);

    // then
    assertEventSubprocessLifecycle(wfInstanceKey);
    ENGINE.job().ofInstance(wfInstanceKey).withType("type").complete();

    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withWorkflowInstanceKey(wfInstanceKey)
                .limitToWorkflowInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSubsequence(
            tuple(BpmnElementType.START_EVENT, WorkflowInstanceIntent.EVENT_OCCURRED),
            tuple(BpmnElementType.SUB_PROCESS, WorkflowInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.SUB_PROCESS, WorkflowInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SERVICE_TASK, WorkflowInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.END_EVENT, WorkflowInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, WorkflowInstanceIntent.ELEMENT_COMPLETED));
  }

  @Test
  public void shouldInterruptAndCompleteParent() {
    // given
    final BpmnModelInstance model = eventSubprocModel(true, builder);
    final long wfInstanceKey = createInstanceAndTriggerEvent(model);

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withWorkflowInstanceKey(wfInstanceKey)
                .limitToWorkflowInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSubsequence(
            tuple(BpmnElementType.START_EVENT, WorkflowInstanceIntent.EVENT_OCCURRED),
            tuple(BpmnElementType.SERVICE_TASK, WorkflowInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SUB_PROCESS, WorkflowInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.SUB_PROCESS, WorkflowInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, WorkflowInstanceIntent.ELEMENT_COMPLETED));
  }

  @Test
  public void shouldInterruptNestedSubprocess() {
    // given
    final BpmnModelInstance model = nestedInterruptingEventSubprocess(builder);
    final long wfInstanceKey = createInstanceAndTriggerEvent(model);

    // then
    final Record<WorkflowInstanceRecordValue> subProcess =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETING)
            .withWorkflowInstanceKey(wfInstanceKey)
            .withElementId("sub_proc")
            .getFirst();

    final Record<WorkflowInstanceRecordValue> eventSubproc =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
            .withWorkflowInstanceKey(wfInstanceKey)
            .withElementId("event_sub_proc")
            .getFirst();

    assertThat(eventSubproc.getValue().getFlowScopeKey()).isEqualTo(subProcess.getKey());
    assertThat(subProcess.getValue().getFlowScopeKey()).isEqualTo(wfInstanceKey);
    assertThat(subProcess.getSourceRecordPosition()).isEqualTo(eventSubproc.getPosition());
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_TERMINATED)
                .withWorkflowInstanceKey(wfInstanceKey)
                .withElementId("task")
                .getFirst())
        .isNotNull();
  }

  @Test
  public void shouldHaveScopeVariableIfInterrupting() {
    // given
    final BpmnModelInstance model = eventSubProcTaskModel(true, "type", helper.getJobType());
    final long wfInstanceKey = createInstanceAndWaitForTask(model);

    final long procTaskKey =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
            .withWorkflowInstanceKey(wfInstanceKey)
            .withElementId("task")
            .getFirst()
            .getKey();
    ENGINE
        .variables()
        .ofScope(procTaskKey)
        .withDocument(Map.of("y", 2))
        .withUpdateSemantic(VariableDocumentUpdateSemantic.LOCAL)
        .update();

    // when
    triggerEventSubprocess.accept(wfInstanceKey);
    assertThat(
            RecordingExporter.jobRecords(JobIntent.CREATED)
                .withWorkflowInstanceKey(wfInstanceKey)
                .withType(helper.getJobType())
                .exists())
        .isTrue();

    // then
    final Record<JobBatchRecordValue> job = ENGINE.jobs().withType(helper.getJobType()).activate();
    final Map<String, Object> jobVariables =
        job.getValue().getJobs().iterator().next().getVariables();
    assertThat(jobVariables).containsOnly(Map.entry("key", 123));
  }

  @Test
  public void shouldNotPropagateVariablesToScope() {
    // given
    final BpmnModelInstance model = eventSubProcTaskModel(false, helper.getJobType(), "sub_type");
    final long wfInstanceKey = createInstanceAndTriggerEvent(model);
    final long eventSubprocKey =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
            .withWorkflowInstanceKey(wfInstanceKey)
            .withElementType(BpmnElementType.SUB_PROCESS)
            .getFirst()
            .getKey();

    ENGINE
        .variables()
        .ofScope(eventSubprocKey)
        .withDocument(Map.of("y", 2))
        .withUpdateSemantic(VariableDocumentUpdateSemantic.LOCAL)
        .update();
    ENGINE.job().ofInstance(wfInstanceKey).withType("sub_type").complete();

    // then
    final Record<JobBatchRecordValue> job = ENGINE.jobs().withType(helper.getJobType()).activate();
    final Map<String, Object> jobVariables =
        job.getValue().getJobs().iterator().next().getVariables();

    // when
    assertThat(jobVariables).containsOnly(Map.entry("key", 123));
  }

  private static void assertEventSubprocessLifecycle(final long workflowInstanceKey) {
    final List<Record<WorkflowInstanceRecordValue>> events =
        RecordingExporter.workflowInstanceRecords()
            .withWorkflowInstanceKey(workflowInstanceKey)
            .filter(r -> r.getValue().getElementId().startsWith("event_sub_"))
            .limit(13)
            .asList();

    assertThat(events)
        .extracting(Record::getIntent, e -> e.getValue().getElementId())
        .containsExactly(
            tuple(WorkflowInstanceIntent.EVENT_OCCURRED, "event_sub_start"),
            tuple(WorkflowInstanceIntent.ELEMENT_ACTIVATING, "event_sub_proc"),
            tuple(WorkflowInstanceIntent.ELEMENT_ACTIVATED, "event_sub_proc"),
            tuple(WorkflowInstanceIntent.ELEMENT_ACTIVATING, "event_sub_start"),
            tuple(WorkflowInstanceIntent.ELEMENT_ACTIVATED, "event_sub_start"),
            tuple(WorkflowInstanceIntent.ELEMENT_COMPLETING, "event_sub_start"),
            tuple(WorkflowInstanceIntent.ELEMENT_COMPLETED, "event_sub_start"),
            tuple(WorkflowInstanceIntent.ELEMENT_ACTIVATING, "event_sub_end"),
            tuple(WorkflowInstanceIntent.ELEMENT_ACTIVATED, "event_sub_end"),
            tuple(WorkflowInstanceIntent.ELEMENT_COMPLETING, "event_sub_end"),
            tuple(WorkflowInstanceIntent.ELEMENT_COMPLETED, "event_sub_end"),
            tuple(WorkflowInstanceIntent.ELEMENT_COMPLETING, "event_sub_proc"),
            tuple(WorkflowInstanceIntent.ELEMENT_COMPLETED, "event_sub_proc"));
  }

  private long createInstanceAndTriggerEvent(final BpmnModelInstance model) {
    final long wfInstanceKey = createInstanceAndWaitForTask(model);
    triggerEventSubprocess.accept(wfInstanceKey);
    return wfInstanceKey;
  }

  private long createInstanceAndWaitForTask(final BpmnModelInstance model) {
    currentWorkflow =
        ENGINE
            .deployment()
            .withXmlResource(model)
            .deploy()
            .getValue()
            .getDeployedWorkflows()
            .get(0);

    final long wfInstanceKey =
        ENGINE
            .workflowInstance()
            .ofBpmnProcessId(PROCESS_ID)
            .withVariables(Map.of("key", 123))
            .create();
    assertThat(
            RecordingExporter.jobRecords(JobIntent.CREATED)
                .withWorkflowInstanceKey(wfInstanceKey)
                .exists())
        .describedAs("Expected job to be created")
        .isTrue();
    return wfInstanceKey;
  }

  private static BpmnModelInstance eventSubprocModel(
      final boolean interrupting,
      final Function<StartEventBuilder, StartEventBuilder> startBuilder) {
    final ProcessBuilder builder = Bpmn.createExecutableProcess(PROCESS_ID);
    startBuilder
        .apply(
            builder
                .eventSubProcess("event_sub_proc")
                .startEvent("event_sub_start")
                .interrupting(interrupting))
        .endEvent("event_sub_end");

    return builder
        .startEvent("start_proc")
        .serviceTask("task", t -> t.zeebeTaskType("type"))
        .endEvent("end_proc")
        .done();
  }

  private static BpmnModelInstance nestedInterruptingEventSubprocess(
      final Function<StartEventBuilder, StartEventBuilder> builder) {
    final EmbeddedSubProcessBuilder embeddedBuilder =
        Bpmn.createExecutableProcess(PROCESS_ID)
            .startEvent("proc_start")
            .subProcess("sub_proc")
            .embeddedSubProcess();
    builder
        .apply(
            embeddedBuilder
                .eventSubProcess("event_sub_proc")
                .startEvent("event_sub_start")
                .interrupting(true))
        .endEvent("event_sub_end");
    return embeddedBuilder
        .startEvent("sub_start")
        .serviceTask("task", t -> t.zeebeTaskType("type"))
        .endEvent("sub_end")
        .done();
  }

  private BpmnModelInstance eventSubProcTaskModel(
      final boolean interrupting, final String procTaskType, final String subprocTaskType) {
    final ProcessBuilder modelBuilder = Bpmn.createExecutableProcess(PROCESS_ID);
    builder
        .apply(
            modelBuilder
                .eventSubProcess("event_sub_proc")
                .startEvent("event_sub_start")
                .interrupting(interrupting))
        .serviceTask("event_sub_task", t -> t.zeebeTaskType(subprocTaskType))
        .endEvent("event_sub_end");

    return modelBuilder
        .startEvent("start_proc")
        .serviceTask("task", t -> t.zeebeTaskType(procTaskType))
        .endEvent("end_proc")
        .done();
  }
}
