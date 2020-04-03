package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;

import com.google.common.base.Strings;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 正常的property文件的解析器
 * 注释或空行在apollo中作为配置项
 * 更新注释或空行是通过创建后删除同行的旧的配置项实现
 * 更新正常的配置项是通过更新来实现
 */
@Component("propertyResolver")
public class PropertyResolver implements ConfigTextResolver {

  private static final String KV_SEPARATOR = "=";
  private static final String ITEM_SEPARATOR = "\n";

  @Override
  public ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems) {
    //创建Map<行号,配置项>
    Map<Integer, ItemDTO> oldLineNumMapItem = BeanUtils.mapByKey("lineNum", baseItems);
    //创建Map<配置名,配置项>
    Map<String, ItemDTO> oldKeyMapItem = BeanUtils.mapByKey("key", baseItems);

    //remove comment and blank item map.
    oldKeyMapItem.remove("");
    //根据换行划分配置项
    String[] newItems = configText.split(ITEM_SEPARATOR);
    //判断是否存在相同配置项
    if (isHasRepeatKey(newItems)) {
      throw new BadRequestException("config text has repeat key please check.");
    }

    ItemChangeSets changeSets = new ItemChangeSets();
    Map<Integer, String> newLineNumMapItem = new HashMap<>();//use for delete blank and comment item
    //第几行
    int lineCounter = 1;
    for (String newItem : newItems) {
      newItem = newItem.trim();
      newLineNumMapItem.put(lineCounter, newItem);
      //获取当前行的配置项
      ItemDTO oldItemByLine = oldLineNumMapItem.get(lineCounter);

      //判断是否为注释
      if (isCommentItem(newItem)) {

        handleCommentLine(namespaceId, oldItemByLine, newItem, lineCounter, changeSets);

        //判断是否空行
      } else if (isBlankItem(newItem)) {

        handleBlankLine(namespaceId, oldItemByLine, lineCounter, changeSets);

        //正常配置项
      } else {
        handleNormalLine(namespaceId, oldKeyMapItem, newItem, lineCounter, changeSets);
      }

      lineCounter++;
    }
    //根据newLineNumMapItem和oldLineNumMapItem的行数做对比删除更改的注释和空行
    deleteCommentAndBlankItem(oldLineNumMapItem, newLineNumMapItem, changeSets);
    //删除不存在的配置项
    deleteNormalKVItem(oldKeyMapItem, changeSets);

    return changeSets;
  }

  private boolean isHasRepeatKey(String[] newItems) {
    Set<String> keys = new HashSet<>();
    int lineCounter = 1;
    int keyCount = 0;
    for (String item : newItems) {
      if (!isCommentItem(item) && !isBlankItem(item)) {
        keyCount++;
        String[] kv = parseKeyValueFromItem(item);
        if (kv != null) {
          keys.add(kv[0].toLowerCase());
        } else {
          throw new BadRequestException("line:" + lineCounter + " key value must separate by '='");
        }
      }
      lineCounter++;
    }

    return keyCount > keys.size();
  }

  private String[] parseKeyValueFromItem(String item) {
    int kvSeparator = item.indexOf(KV_SEPARATOR);
    if (kvSeparator == -1) {
      return null;
    }

    String[] kv = new String[2];
    kv[0] = item.substring(0, kvSeparator).trim();
    kv[1] = item.substring(kvSeparator + 1, item.length()).trim();
    return kv;
  }

  private void handleCommentLine(Long namespaceId, ItemDTO oldItemByLine, String newItem, int lineCounter, ItemChangeSets changeSets) {
    String oldComment = oldItemByLine == null ? "" : oldItemByLine.getComment();
    //判断是否为注释，不是则当前行的配置项不相同，若为注释则判断新旧的注释是否相同，不相同则新增该注释
    if (!(isCommentItem(oldItemByLine) && newItem.equals(oldComment))) {
      changeSets.addCreateItem(buildCommentItem(0l, namespaceId, newItem, lineCounter));
    }
  }

  private void handleBlankLine(Long namespaceId, ItemDTO oldItem, int lineCounter, ItemChangeSets changeSets) {
    //如果同行的旧数据项不为空行则新增数据项
    if (!isBlankItem(oldItem)) {
      changeSets.addCreateItem(buildBlankItem(0l, namespaceId, lineCounter));
    }
  }

  private void handleNormalLine(Long namespaceId, Map<String, ItemDTO> keyMapOldItem, String newItem,
                                int lineCounter, ItemChangeSets changeSets) {
    //解析拆分key-value
    String[] kv = parseKeyValueFromItem(newItem);

    if (kv == null) {
      throw new BadRequestException("line:" + lineCounter + " key value must separate by '='");
    }

    String newKey = kv[0];
    String newValue = kv[1].replace("\\n", "\n"); //handle user input \n
    //根据key获取value
    ItemDTO oldItem = keyMapOldItem.get(newKey);
    //若旧配置项不存在，则该配置项为新的配置项
    if (oldItem == null) {//new item
      changeSets.addCreateItem(buildNormalItem(0l, namespaceId, newKey, newValue, "", lineCounter));
      //旧的配置项存在判断其值和行数是否相等是否相等，不相等则更新值和行数
    } else if (!newValue.equals(oldItem.getValue()) || lineCounter != oldItem.getLineNum()) {//update item
      changeSets.addUpdateItem(
          buildNormalItem(oldItem.getId(), namespaceId, newKey, newValue, oldItem.getComment(),
              lineCounter));
    }
    keyMapOldItem.remove(newKey);
  }

  private boolean isCommentItem(ItemDTO item) {
    return item != null && "".equals(item.getKey())
        && (item.getComment().startsWith("#") || item.getComment().startsWith("!"));
  }

  private boolean isCommentItem(String line) {
    return line != null && (line.startsWith("#") || line.startsWith("!"));
  }

  private boolean isBlankItem(ItemDTO item) {
    return item != null && "".equals(item.getKey()) && "".equals(item.getComment());
  }

  private boolean isBlankItem(String line) {
    return  Strings.nullToEmpty(line).trim().isEmpty();
  }

  private void deleteNormalKVItem(Map<String, ItemDTO> baseKeyMapItem, ItemChangeSets changeSets) {
    //surplus item is to be deleted
    for (Map.Entry<String, ItemDTO> entry : baseKeyMapItem.entrySet()) {
      changeSets.addDeleteItem(entry.getValue());
    }
  }

  private void deleteCommentAndBlankItem(Map<Integer, ItemDTO> oldLineNumMapItem,
                                         Map<Integer, String> newLineNumMapItem,
                                         ItemChangeSets changeSets) {

    for (Map.Entry<Integer, ItemDTO> entry : oldLineNumMapItem.entrySet()) {
      int lineNum = entry.getKey();
      ItemDTO oldItem = entry.getValue();
      String newItem = newLineNumMapItem.get(lineNum);

      //1. old is blank by now is not
      //2.old is comment by now is not exist or modified
      //判断旧配置项为空行却新配置项不为空行则删除该配置项
      if ((isBlankItem(oldItem) && !isBlankItem(newItem))
              //旧的配置项是注释且新配置项不存在或新注释不等于旧注释则删除该行
          || isCommentItem(oldItem) && (newItem == null || !newItem.equals(oldItem.getComment()))) {
        changeSets.addDeleteItem(oldItem);
      }
    }
  }

  private ItemDTO buildCommentItem(Long id, Long namespaceId, String comment, int lineNum) {
    return buildNormalItem(id, namespaceId, "", "", comment, lineNum);
  }

  private ItemDTO buildBlankItem(Long id, Long namespaceId, int lineNum) {
    return buildNormalItem(id, namespaceId, "", "", "", lineNum);
  }

  private ItemDTO buildNormalItem(Long id, Long namespaceId, String key, String value, String comment, int lineNum) {
    ItemDTO item = new ItemDTO(key, value, comment, lineNum);
    item.setId(id);
    item.setNamespaceId(namespaceId);
    return item;
  }
}
