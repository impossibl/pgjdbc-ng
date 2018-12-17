package com.impossibl.postgres.protocol;

import java.util.List;

public class ResultBatches {

  public static List<ResultBatch> releaseAll(List<ResultBatch> resultBatches) {
    if (resultBatches != null) {
      for (ResultBatch resultBatch : resultBatches) {
        resultBatch.release();
      }
    }
    return null;
  }

}
