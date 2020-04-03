package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.StringUtils;

import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * 配置文件解析器,解析非properties式的配置文本,进行新增/更新
 * 非properties式的配置文本仅存在一个配置项,key=content,value=配置文本
 */
@Component("fileTextResolver")
public class FileTextResolver implements ConfigTextResolver {

  @Override
  public ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems) {
    ItemChangeSets changeSets = new ItemChangeSets();
    if (CollectionUtils.isEmpty(baseItems) && StringUtils.isEmpty(configText)) {
      return changeSets;
    }
    //已有配置项为空则将配置文本作为配置项
    if (CollectionUtils.isEmpty(baseItems)) {
      changeSets.addCreateItem(createItem(namespaceId, 0, configText));
    } else {
      //配置项不为空则获取配置项判断是否相等,不相等则更新
      ItemDTO beforeItem = baseItems.get(0);
      if (!configText.equals(beforeItem.getValue())) {//update
        changeSets.addUpdateItem(createItem(namespaceId, beforeItem.getId(), configText));
      }
    }

    return changeSets;
  }

  private ItemDTO createItem(long namespaceId, long itemId, String value) {
    ItemDTO item = new ItemDTO();
    item.setId(itemId);
    item.setNamespaceId(namespaceId);
    item.setValue(value);
    item.setLineNum(1);
    item.setKey(ConfigConsts.CONFIG_FILE_CONTENT_KEY);
    return item;
  }
}
