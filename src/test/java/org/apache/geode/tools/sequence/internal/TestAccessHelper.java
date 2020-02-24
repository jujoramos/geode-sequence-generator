package org.apache.geode.tools.sequence.internal;

import static org.apache.geode.tools.sequence.internal.SetUpFunction.DISTRIBUTED_SEQUENCES_DISK_STORE_ID;
import static org.apache.geode.tools.sequence.internal.SetUpFunction.DISTRIBUTED_SEQUENCES_REGION_ID;
import static org.apache.geode.tools.sequence.internal.SetUpFunction.FUNCTION_ID;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.VMProvider;

/**
 * Utility class to access protected methods from and external package.
 * Only for testing purposes.
 */
public class TestAccessHelper {

  /**
   * In real life this should be done by users through gfsh deploy.
   */
  public static void registerFunctions(VMProvider... members) {
    MemberVM.invokeInEveryMember(() -> {
      FunctionService.registerFunction(new GetFunction());
      FunctionService.registerFunction(new NextBatchFunction());
      FunctionService.registerFunction(new SetUpFunction());
    }, members);
  }

  public static String getSetUpFunctionId() {
    return FUNCTION_ID;
  }

  public static String getDistributedSequencesRegionId() {
    return DISTRIBUTED_SEQUENCES_REGION_ID;
  }

  public static String getDistributedSequencesRegionDiskStoreId() {
    return DISTRIBUTED_SEQUENCES_DISK_STORE_ID;
  }
}
