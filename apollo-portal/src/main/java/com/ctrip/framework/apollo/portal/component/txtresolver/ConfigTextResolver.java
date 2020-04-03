package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;

import java.util.List;

/**
 * 新增/更新配置项使用的配置文本解析器
 */
public interface ConfigTextResolver {

  /**
   * 解析文本,创建ItemChangeSets对象
   *
   * @param namespaceId Namespace 编号
   * @param configText 配置文本
   * @param baseItems 已存在的ItemDTO
   * @return ItemChangeSets 对象
   */
  ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems);

}
