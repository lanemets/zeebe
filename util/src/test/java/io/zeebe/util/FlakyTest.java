/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.util;

import org.junit.Assert;
import org.junit.Test;

public class FlakyTest {

  private static int counter = 0;

  @Test
  public void flakyTest() {
    System.out.println("Flaky test");
    if (counter == 0) {
      counter++;
      Assert.fail("failed");
    }
  }
}
