package com.vladmihalcea.hibernate.masterclass.laboratory.cache;

import com.vladmihalcea.hibernate.masterclass.laboratory.cache.ReadWriteCacheConcurrencyStrategyTest.Repository;
import com.vladmihalcea.hibernate.masterclass.laboratory.util.AbstractPostgreSQLIntegrationTest;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Interceptor;
import org.hibernate.Transaction;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.cache.spi.CacheKey;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.*;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


/**
 * ReadWriteCacheConcurrencyStrategyWithTimeoutTest - Test to check CacheConcurrencyStrategy.READ_WRITE with lock timeout
 *
 * @author Vlad Mihalcea
 */
public class ReadWriteCacheConcurrencyStrategyWithLockTimeoutTest extends AbstractPostgreSQLIntegrationTest {

    @Override
    protected Class<?>[] entities() {
        return new Class<?>[] {
                Repository.class
        };
    }

    private AtomicBoolean applyInterceptor = new AtomicBoolean();

    @Override
    protected Interceptor interceptor() {
        return new EmptyInterceptor() {
            @Override
            public void beforeTransactionCompletion(Transaction tx) {
                if(applyInterceptor.get()) {
                    tx.rollback();
                }
            }
        };
    }

    @Override
    protected Properties getProperties() {
        Properties properties = super.getProperties();
        properties.put("hibernate.cache.use_second_level_cache", Boolean.TRUE.toString());
        properties.put("hibernate.cache.region.factory_class", "org.hibernate.cache.ehcache.EhCacheRegionFactory");
        properties.put("net.sf.ehcache.hibernate.cache_lock_timeout", String.valueOf(250));
        properties.put("hibernate.cache.use_structured_entries", Boolean.TRUE.toString());

        return properties;
    }

    @Before
    public void init() {
        super.init();
        doInTransaction(session -> {
            Repository repository = new Repository("Hibernate-Master-Class");
            session.persist(repository);
        });
        
        LOGGER.info("\t --> 1st Transaction - INIT");
        printEntityCacheStats(Repository.class.getName(), true);
    }

    @Test
    public void testRepositoryEntityUpdate() {
        try {
            doInTransaction(session -> {
                Repository repository = (Repository) session.get(Repository.class, 1L);
                repository.setName("High-Performance Hibernate");

                LOGGER.info("\t --> 2nd Transaction");
                LOGGER.info("\t --> BEFORE ROLLBACK");
                printEntityCacheStats(Repository.class.getName(), true);

                applyInterceptor.set(true);
            });


        } catch (Exception e) {
            LOGGER.info("Expected", e);
        }
        
        LOGGER.info("\t --> AFTER ROLLBACK");
        printEntityCacheStats(Repository.class.getName(), true);
        applyInterceptor.set(false);

        AtomicReference<Object> previousCacheEntryReference = new AtomicReference<>();
        AtomicBoolean cacheEntryChanged = new AtomicBoolean();

        int t=1;
        
        while (!cacheEntryChanged.get()) {
        	LOGGER.info("\t --> Transaction #" + t++);

        	doInTransaction(session -> {
            	boolean entryChange;
                LOGGER.info("\t --> try to get the Entity instance...\n\t\t Observes if it hit the DB");
                LOGGER.info("\t --> Hit the 2LC ???");
                session.get(Repository.class, 1L);

                printEntityCacheStats(Repository.class.getName(), true);

                try {
                    Object previousCacheEntry = previousCacheEntryReference.get();
                    LOGGER.info("\t --> try to get the Entity instance directly from the Cache!");
                    LOGGER.info("\t --> Hit the 2LC ???");
                    Object cacheEntry = getCacheEntry(Repository.class, 1L);
                    
                    LOGGER.info("\n\t previousCacheEntry: {}", ToStringBuilder.reflectionToString(previousCacheEntry));
                    LOGGER.info("\n\t cacheEntry:         {}", ToStringBuilder.reflectionToString(cacheEntry));

                    // in 1st time evaluates to false
                    entryChange = (previousCacheEntry != null) && (previousCacheEntry != cacheEntry);
                    previousCacheEntryReference.set(cacheEntry);

                    printEntityCacheStats(Repository.class.getName(), true);
                    
                    if(!entryChange) {
                    	LOGGER.info("\n\t Wait 100ms until the cache entry LOCK ends...");
                        sleep(100);
                    } else {
                        cacheEntryChanged.set(true);
                    }
                } catch (IllegalAccessException e) {
                    LOGGER.error("Error accessing Cache", e);
                }
        	});

        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getCacheEntry(Class<T> clazz, Long id) throws IllegalAccessException {
        EntityPersister entityPersister = ((SessionFactoryImplementor) getSessionFactory()).getEntityPersister(clazz.getName() );
        return (T) getCache(clazz).get(cacheKey(1L, entityPersister));
    }

    private net.sf.ehcache.Cache getCache(Class clazz) throws IllegalAccessException {
        EntityPersister entityPersister = ((SessionFactoryImplementor) getSessionFactory()).getEntityPersister(clazz.getName() );
        EntityRegion region = entityPersister.getCacheAccessStrategy().getRegion();
        Field cacheField = getField(region.getClass(), "cache");
        return  (net.sf.ehcache.Cache) cacheField.get(region);
    }

    private Field getField(Class clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (Exception e) {
            if(clazz.getSuperclass() != null && clazz.getSuperclass() != clazz) {
                return getField(clazz.getSuperclass(), fieldName);
            }
        }
        throw new IllegalArgumentException("No field " + fieldName + " found!");
    }

    private CacheKey cacheKey(Serializable identifier, EntityPersister p) {
        return new CacheKey(
                identifier,
                p.getIdentifierType(),
                p.getRootEntityName(),
                null,
                (SessionFactoryImplementor) getSessionFactory()
        );
    }

    /**
     * Repository - Repository
     *
     * @author Vlad Mihalcea
     */
    @Entity(name = "repository")
    @org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
    public static class Repository {

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;

        private String name;

        @Version
        private int version;

        public Repository() {
        }

        public Repository(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }
    }
}
