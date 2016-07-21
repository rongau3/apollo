package com.ctrip.framework.apollo.configservice.controller;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.configservice.util.NamespaceUtil;
import com.ctrip.framework.apollo.configservice.util.WatchKeysUtil;
import com.ctrip.framework.apollo.core.dto.ApolloConfig;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@RunWith(MockitoJUnitRunner.class)
public class ConfigFileControllerTest {
  @Mock
  private ConfigController configController;
  @Mock
  private WatchKeysUtil watchKeysUtil;
  @Mock
  private NamespaceUtil namespaceUtil;
  private ConfigFileController configFileController;
  private String someAppId;
  private String someClusterName;
  private String someNamespace;
  private String someDataCenter;
  private String someClientIp;
  @Mock
  private HttpServletResponse someResponse;
  Multimap<String, String> watchedKeys2CacheKey;
  Multimap<String, String> cacheKey2WatchedKeys;

  @Before
  public void setUp() throws Exception {
    configFileController = new ConfigFileController();
    ReflectionTestUtils.setField(configFileController, "configController", configController);
    ReflectionTestUtils.setField(configFileController, "watchKeysUtil", watchKeysUtil);
    ReflectionTestUtils.setField(configFileController, "namespaceUtil", namespaceUtil);

    someAppId = "someAppId";
    someClusterName = "someClusterName";
    someNamespace = "someNamespace";
    someDataCenter = "someDataCenter";
    someClientIp = "10.1.1.1";

    when(namespaceUtil.filterNamespaceName(someNamespace)).thenReturn(someNamespace);

    watchedKeys2CacheKey =
        (Multimap<String, String>) ReflectionTestUtils
            .getField(configFileController, "watchedKeys2CacheKey");
    cacheKey2WatchedKeys =
        (Multimap<String, String>) ReflectionTestUtils
            .getField(configFileController, "cacheKey2WatchedKeys");
  }

  @Test
  public void testQueryConfigAsFile() throws Exception {
    String someKey = "someKey";
    String someValue = "someValue";
    String anotherKey = "anotherKey";
    String anotherValue = "anotherValue";

    String someWatchKey = "someWatchKey";
    String anotherWatchKey = "anotherWatchKey";
    Set<String> watchKeys = Sets.newHashSet(someWatchKey, anotherWatchKey);

    String cacheKey =
        configFileController
            .assembleCacheKey(someAppId, someClusterName, someNamespace, someDataCenter);

    Map<String, String> configurations =
        ImmutableMap.of(someKey, someValue, anotherKey, anotherValue);
    ApolloConfig someApolloConfig = mock(ApolloConfig.class);
    when(someApolloConfig.getConfigurations()).thenReturn(configurations);
    when(configController
        .queryConfig(someAppId, someClusterName, someNamespace, someDataCenter, "-1", someClientIp,
            someResponse)).thenReturn(someApolloConfig);
    when(watchKeysUtil
        .assembleAllWatchKeys(someAppId, someClusterName, someNamespace, someDataCenter))
        .thenReturn(watchKeys);

    ResponseEntity<String> response =
        configFileController
            .queryConfigAsFile(someAppId, someClusterName, someNamespace, someDataCenter,
                someClientIp, someResponse);

    assertEquals(2, watchedKeys2CacheKey.size());
    assertEquals(2, cacheKey2WatchedKeys.size());
    assertTrue(watchedKeys2CacheKey.containsEntry(someWatchKey, cacheKey));
    assertTrue(watchedKeys2CacheKey.containsEntry(anotherWatchKey, cacheKey));
    assertTrue(cacheKey2WatchedKeys.containsEntry(cacheKey, someWatchKey));
    assertTrue(cacheKey2WatchedKeys.containsEntry(cacheKey, anotherWatchKey));

    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertTrue(response.getBody().contains(String.format("%s=%s", someKey, someValue)));
    assertTrue(response.getBody().contains(String.format("%s=%s", anotherKey, anotherValue)));

    ResponseEntity<String> anotherResponse =
        configFileController
            .queryConfigAsFile(someAppId, someClusterName, someNamespace, someDataCenter,
                someClientIp, someResponse);

    assertEquals(response, anotherResponse);

    verify(configController, times(1))
        .queryConfig(someAppId, someClusterName, someNamespace, someDataCenter, "-1", someClientIp,
            someResponse);
  }

  @Test
  public void testHandleMessage() throws Exception {
    String someWatchKey = "someWatchKey";
    String anotherWatchKey = "anotherWatchKey";
    String someCacheKey = "someCacheKey";
    String anotherCacheKey = "anotherCacheKey";
    String someValue = "someValue";

    ReleaseMessage someReleaseMessage = mock(ReleaseMessage.class);
    when(someReleaseMessage.getMessage()).thenReturn(someWatchKey);

    Cache<String, String> cache =
        (Cache<String, String>) ReflectionTestUtils.getField(configFileController, "localCache");
    cache.put(someCacheKey, someValue);
    cache.put(anotherCacheKey, someValue);

    watchedKeys2CacheKey.putAll(someWatchKey, Lists.newArrayList(someCacheKey, anotherCacheKey));
    watchedKeys2CacheKey.putAll(anotherWatchKey, Lists.newArrayList(someCacheKey, anotherCacheKey));

    cacheKey2WatchedKeys.putAll(someCacheKey, Lists.newArrayList(someWatchKey, anotherWatchKey));
    cacheKey2WatchedKeys.putAll(anotherCacheKey, Lists.newArrayList(someWatchKey, anotherWatchKey));

    configFileController.handleMessage(someReleaseMessage, Topics.APOLLO_RELEASE_TOPIC);

    assertTrue(watchedKeys2CacheKey.isEmpty());
    assertTrue(cacheKey2WatchedKeys.isEmpty());
  }
}