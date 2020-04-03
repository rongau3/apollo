package com.ctrip.framework.apollo.configservice.util;

import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import org.springframework.stereotype.Component;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class NamespaceUtil {

  //通过缓存实现，可能存在对应的AppNamespace不存在内存中
  private final AppNamespaceServiceWithCache appNamespaceServiceWithCache;

  public NamespaceUtil(final AppNamespaceServiceWithCache appNamespaceServiceWithCache) {
    this.appNamespaceServiceWithCache = appNamespaceServiceWithCache;
  }

  public String filterNamespaceName(String namespaceName) {
    if (namespaceName.toLowerCase().endsWith(".properties")) {
      int dotIndex = namespaceName.lastIndexOf(".");
      return namespaceName.substring(0, dotIndex);
    }

    return namespaceName;
  }

  public String normalizeNamespace(String appId, String namespaceName) {
    //获取App下的AppNamespace对象
    AppNamespace appNamespace = appNamespaceServiceWithCache.findByAppIdAndNamespace(appId, namespaceName);
    if (appNamespace != null) {
      return appNamespace.getName();
    }
    //查询不到则该Namespace可能是关联的Namespace
    appNamespace = appNamespaceServiceWithCache.findPublicNamespaceByName(namespaceName);
    if (appNamespace != null) {
      return appNamespace.getName();
    }

    //可能不存在于缓存中
    return namespaceName;
  }
}
