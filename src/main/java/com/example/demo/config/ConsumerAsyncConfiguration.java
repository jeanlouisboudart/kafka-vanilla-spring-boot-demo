package com.example.demo.config;

import com.example.demo.kafka.consumers.KafkaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class ConsumerAsyncConfiguration implements AsyncConfigurer {

    private final ConfigurableApplicationContext appContext;
    private final KafkaConfig kafkaConfig;

    private final Logger logger = LoggerFactory.getLogger(ConsumerAsyncConfiguration.class);
    private final List<KafkaReader> kafkaReaders = new ArrayList<>();

    public ConsumerAsyncConfiguration(ConfigurableApplicationContext appContext, KafkaConfig kafkaConfig) {
        this.appContext = appContext;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public Executor getAsyncExecutor() {
        return createThreadPoolBean();
    }


    @Bean
    public ThreadPoolTaskExecutor createThreadPoolBean() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        String[] consumerClasses = appContext.getBeanNamesForType(KafkaReader.class);
        int nbThread = consumerClasses.length * kafkaConfig.getNbConsumerThreads();

        //set the number of consumer thread
        threadPoolTaskExecutor.setCorePoolSize(nbThread);
        threadPoolTaskExecutor.setMaxPoolSize(nbThread);
        threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        threadPoolTaskExecutor.setAwaitTerminationMillis(Duration.ofSeconds(10).toMillis());
        threadPoolTaskExecutor.setThreadNamePrefix("consumer-");
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (throwable, method, args) -> {
            logger.error("Encountered the following exception during processing. Shutting down the application", throwable);
            SpringApplication.exit(appContext, () -> 1);
        };
    }

    @Bean
    public List<KafkaReader> createConsumerThreads() {
        String[] consumerClasses = appContext.getBeanNamesForType(KafkaReader.class);

        for (String consumerClass : consumerClasses) {
            for (int i = 0; i < kafkaConfig.getNbConsumerThreads(); i++) {
                KafkaReader consumerInstance = appContext.getBean(consumerClass, KafkaReader.class);
                consumerInstance.run();
                kafkaReaders.add(consumerInstance);
                Runtime.getRuntime().addShutdownHook(new Thread(consumerInstance::onShutdown));
            }
        }
        return kafkaReaders;
    }


    @PreDestroy
    public void onShutdown() {
        kafkaReaders.forEach(KafkaReader::onShutdown);
    }
}
