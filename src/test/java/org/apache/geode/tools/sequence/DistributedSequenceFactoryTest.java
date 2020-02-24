package org.apache.geode.tools.sequence;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.cache.client.ClientCache;

public class DistributedSequenceFactoryTest {

  @Test
  public void initializeShouldThrowExceptionWhenClientCacheIsNull() {
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("ClientCache can not be null");
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(null, RegionType.PARTITION))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("ClientCache can not be null");
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(null, RegionType.REPLICATE, "TestPool"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("ClientCache can not be null");
  }

  @Test
  public void initializeShouldThrowExceptionWhenRegionTypeIsNull() {
    ClientCache clientCache = mock(ClientCache.class);
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(clientCache, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("RegionType can not be null");
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(clientCache, null, "TestPool"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("RegionType can not be null");
  }

  @Test
  public void initializeShouldThrowExceptionWhenPoolCouldNotBeFound() {
    ClientCache clientCache = mock(ClientCache.class);
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(clientCache))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The default pool could not be found.");
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(clientCache, RegionType.PARTITION))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The default pool could not be found.");
    assertThatThrownBy(() -> DistributedSequenceFactory.initialize(clientCache, RegionType.REPLICATE, "TestPool"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("The specified pool (TestPool) could not be found.");
  }
}
