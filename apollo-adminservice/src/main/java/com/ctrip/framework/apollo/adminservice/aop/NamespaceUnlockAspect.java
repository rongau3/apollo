package com.ctrip.framework.apollo.adminservice.aop;


import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.service.ItemService;
import com.ctrip.framework.apollo.biz.service.NamespaceLockService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;


/**
 * unlock namespace if is redo operation.
 * --------------------------------------------
 * For example: If namespace has a item K1 = v1
 * --------------------------------------------
 * First operate: change k1 = v2 (lock namespace)
 * Second operate: change k1 = v1 (unlock namespace)
 */
@Aspect
@Component
public class NamespaceUnlockAspect {

  private Gson gson = new Gson();

  private final NamespaceLockService namespaceLockService;
  private final NamespaceService namespaceService;
  private final ItemService itemService;
  private final ReleaseService releaseService;
  private final BizConfig bizConfig;

  public NamespaceUnlockAspect(
      final NamespaceLockService namespaceLockService,
      final NamespaceService namespaceService,
      final ItemService itemService,
      final ReleaseService releaseService,
      final BizConfig bizConfig) {
    this.namespaceLockService = namespaceLockService;
    this.namespaceService = namespaceService;
    this.itemService = itemService;
    this.releaseService = releaseService;
    this.bizConfig = bizConfig;
  }


  //create item
  @After("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, item, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                ItemDTO item) {
    tryUnlock(namespaceService.findOne(appId, clusterName, namespaceName));
  }

  //update item
  @After("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, itemId, item, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName, long itemId,
                                ItemDTO item) {
    tryUnlock(namespaceService.findOne(appId, clusterName, namespaceName));
  }

  //update by change set
  @After("@annotation(PreAcquireNamespaceLock) && args(appId, clusterName, namespaceName, changeSet, ..)")
  public void requireLockAdvice(String appId, String clusterName, String namespaceName,
                                ItemChangeSets changeSet) {
    tryUnlock(namespaceService.findOne(appId, clusterName, namespaceName));
  }

  //delete item
  @After("@annotation(PreAcquireNamespaceLock) && args(itemId, operator, ..)")
  public void requireLockAdvice(long itemId, String operator) {
    Item item = itemService.findOne(itemId);
    if (item == null) {
      throw new BadRequestException("item not exist.");
    }
    tryUnlock(namespaceService.findOne(item.getNamespaceId()));
  }

  private void tryUnlock(Namespace namespace) {
    if (bizConfig.isNamespaceLockSwitchOff()) {
      return;
    }
    //配置项未修改则释放锁
    if (!isModified(namespace)) {
      namespaceLockService.unlock(namespace.getId());
    }

  }

  boolean isModified(Namespace namespace) {
    //获取当前命名空间的最新发布的有效发布对象
    Release release = releaseService.findLatestActiveRelease(namespace);
    //获取当前命名空间的所有配置项
    List<Item> items = itemService.findItemsWithoutOrdered(namespace.getId());
    //判断发布对象是否存在，不存在则判断时侯有正常的配置项，若存在则修改过
    if (release == null) {
      return hasNormalItems(items);
    }
    //获得Release的配置项的key-value
    Map<String, String> releasedConfiguration = gson.fromJson(release.getConfigurations(), GsonType.CONFIG);
    //获取当前命名空间的所有配置项以及关联的命名空间的最新发布的有效发布对象的配置项的key-value
    Map<String, String> configurationFromItems = generateConfigurationFromItems(namespace, items);
    //构建对比Map对象
    MapDifference<String, String> difference = Maps.difference(releasedConfiguration, configurationFromItems);
    //判断是否全部相等，不相等则修改过
    return !difference.areEqual();

  }

  private boolean hasNormalItems(List<Item> items) {
    for (Item item : items) {
      if (!StringUtils.isEmpty(item.getKey())) {
        return true;
      }
    }

    return false;
  }

  private Map<String, String> generateConfigurationFromItems(Namespace namespace, List<Item> namespaceItems) {

    Map<String, String> configurationFromItems = Maps.newHashMap();
    //获取父命名空间
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);
    //parent namespace
    if (parentNamespace == null) {
      generateMapFromItems(namespaceItems, configurationFromItems);
    } else {//child namespace
      Release parentRelease = releaseService.findLatestActiveRelease(parentNamespace);
      if (parentRelease != null) {
        configurationFromItems = gson.fromJson(parentRelease.getConfigurations(), GsonType.CONFIG);
      }
      generateMapFromItems(namespaceItems, configurationFromItems);
    }

    return configurationFromItems;
  }

  private Map<String, String> generateMapFromItems(List<Item> items, Map<String, String> configurationFromItems) {
    for (Item item : items) {
      String key = item.getKey();
      //跳过注释和空行，因为注释和空行都为空窜
      if (StringUtils.isBlank(key)) {
        continue;
      }
      configurationFromItems.put(key, item.getValue());
    }

    return configurationFromItems;
  }

}
