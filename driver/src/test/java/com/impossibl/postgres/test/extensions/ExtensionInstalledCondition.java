package com.impossibl.postgres.test.extensions;

import com.impossibl.postgres.jdbc.TestUtil;
import com.impossibl.postgres.test.annotations.ExtensionInstalled;

import java.sql.SQLException;
import java.util.Optional;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

public class ExtensionInstalledCondition implements ExecutionCondition {

  private static final ConditionEvaluationResult ENABLED =
      ConditionEvaluationResult.enabled("@ExtensionInstalled not present");

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

    Optional<ExtensionInstalled> extensionInstalledOpt =
        AnnotationSupport.findAnnotation(context.getElement(), ExtensionInstalled.class);
    if (!extensionInstalledOpt.isPresent()) {
      return ENABLED;
    }

    ExtensionInstalled extensionInstalled = extensionInstalledOpt.get();

    try {
      if (!TestUtil.isExtensionInstalled(DBProvider.open(context), extensionInstalled.value())) {
        return ConditionEvaluationResult.disabled(extensionInstalled.value() + " is not installed");
      }
    }
    catch (SQLException e) {
      return ConditionEvaluationResult.disabled("Check for extension " + extensionInstalled.value() + " failed");
    }

    return ConditionEvaluationResult.enabled(extensionInstalled.value() + " is installed");
  }

}
