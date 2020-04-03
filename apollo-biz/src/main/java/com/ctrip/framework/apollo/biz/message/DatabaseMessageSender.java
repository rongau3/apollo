package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DatabaseMessageSender implements MessageSender {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);
  //阻塞队列的大小
  private static final int CLEAN_QUEUE_MAX_SIZE = 100;
  //用于保存将被清除的阻塞队列，阻塞队列在队列满或空时尝试添加或获取可阻塞当前线程
  private BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);
  private final ExecutorService cleanExecutorService;
  //清理是否被暂停
  private final AtomicBoolean cleanStopped;

  private final ReleaseMessageRepository releaseMessageRepository;

  public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
    //创建单线程模型的线程池
    cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
    cleanStopped = new AtomicBoolean(false);
    this.releaseMessageRepository = releaseMessageRepository;
  }

  @Override
  @Transactional
  public void sendMessage(String message, String channel) {
    logger.info("Sending message {} to channel {}", message, channel);
    if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {
      logger.warn("Channel {} not supported by DatabaseMessageSender!");
      return;
    }

    Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
    Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
    try {
      //保存发送发布消息到数据库
      ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
      //添加发布消息到阻塞队列，当添加失败的时侯不阻塞线程
      toClean.offer(newMessage.getId());
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      logger.error("Sending message to database failed", ex);
      transaction.setStatus(ex);
      throw ex;
    } finally {
      transaction.complete();
    }
  }

  @PostConstruct//对象创建后自动调用
  private void initialize() {
    //提交定时清理任务
    cleanExecutorService.submit(() -> {
      //清理未被暂停且当前线程未被中断时持续执行
      while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          //从阻塞队列中获取一个发布消息的ID，期限为1s
          Long rm = toClean.poll(1, TimeUnit.SECONDS);
          //未超过期限或者队列存在数据时发送消息
          if (rm != null) {
            cleanMessage(rm);
          } else {
            //队列为空时使线程休眠，避免空跑占用cpu
            TimeUnit.SECONDS.sleep(5);
          }
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  private void cleanMessage(Long id) {
    //用于判断是否存在更多还未删除的消息
    boolean hasMore = true;
    //double check in case the release message is rolled back
    //查询对应的 ReleaseMessage 对象，避免已经删除。因为，DatabaseMessageSender 会在多进程中执行。
    //例如：1）Config Service + Admin Service ；2）N * Config Service ；3）N * Admin Service
    //因为 DatabaseMessageSender 添加了 @Component 注解，而 NamespaceService 注入了 DatabaseMessageSender。
    //而NamespaceService被apollo-adminservice和apoll-configservice项目都引用了，所以都会启动该任务
    ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
    if (releaseMessage == null) {
      return;
    }
    //循环删除相同message的老消息
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //获取前100条且id小于传入的id的消息(老消息)
      List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
              releaseMessage.getMessage(), releaseMessage.getId());

      releaseMessageRepository.deleteAll(messages);
      //判断获取到的消息是否等于100，若等于100即有可能存在更多还未删除的id
      hasMore = messages.size() == 100;

      messages.forEach(toRemove -> Tracer.logEvent(
              String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
    }
  }

  void stopClean() {
    cleanStopped.set(true);
  }
}
