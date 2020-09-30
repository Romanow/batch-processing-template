package ru.romanow.processing.batch;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.romanow.processing.batch.model.EmptyObject;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfiguration
        extends DefaultBatchConfigurer {
    private static final Logger logger = LoggerFactory.getLogger(BatchConfiguration.class);

    private static final int CHUNK_SIZE = 50;

    private final DataSource dataSource;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Override
    public void setDataSource(DataSource dataSource) {
    }

    @Bean
    public Job importUserJob() {
        return jobBuilderFactory
                .get("processJob")
                .flow(step())
                .end()
                .build();
    }

    @Bean
    public Step step() {
        return stepBuilderFactory
                .get("processor")
                .<EmptyObject, EmptyObject>chunk(CHUNK_SIZE)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .listener(stepListener())
                .build();
    }

    @Bean
    public StepExecutionListener stepListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(@Nonnull StepExecution stepExecution) {
                logger.info("Start execute step {} time: {}", stepExecution.getStepName(), stepExecution.getStartTime());
            }

            @Override
            public ExitStatus afterStep(@Nonnull StepExecution stepExecution) {
                logger.info("Start execute step {} time: {}", stepExecution.getStepName(), stepExecution.getEndTime());
                return null;
            }
        };
    }

    @Bean
    public ItemReader<EmptyObject> reader() {
        return () -> null;
    }

    @Bean
    public ItemProcessor<EmptyObject, EmptyObject> processor() {
        return item -> item;
    }

    @Bean
    public ItemWriter<EmptyObject> writer() {
        return items -> items.forEach(i -> logger.info("Processed '{}'", i));
    }
}
