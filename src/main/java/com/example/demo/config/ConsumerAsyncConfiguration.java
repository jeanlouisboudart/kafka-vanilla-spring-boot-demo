package com.example.demo.config;

import com.example.demo.services.KafkaReader;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

@Configuration
@AllArgsConstructor
@EnableAsync
public class ConsumerAsyncConfiguration implements AsyncConfigurer {

    private final ConfigurableApplicationContext appContext;
    private final KafkaConfig kafkaConfig;

    private final Logger logger = LoggerFactory.getLogger(ConsumerAsyncConfiguration.class);

    @Override
    public Executor getAsyncExecutor() {
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
            Runtime.getRuntime().exit(1);
        };
    }

    @Bean
    public List<KafkaReader> createConsumerThreads() {
        String[] consumerClasses = appContext.getBeanNamesForType(KafkaReader.class);

        List<KafkaReader> multiThreadConsumers = new ArrayList<>();
        for (String consumerClass : consumerClasses) {
            for (int i = 0; i < kafkaConfig.getNbConsumerThreads(); i++) {
                KafkaReader consumerInstance = appContext.getBean(consumerClass, KafkaReader.class);
                consumerInstance.run();
                multiThreadConsumers.add(consumerInstance);
                Runtime.getRuntime().addShutdownHook(new Thread(consumerInstance::onShutdown));
            }
        }
        return multiThreadConsumers;
    }
}
