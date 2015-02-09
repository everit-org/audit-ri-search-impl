/**
 * This file is part of org.everit.osgi.audit.ri.conf.
 *
 * org.everit.osgi.audit.ri.conf is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * org.everit.osgi.audit.ri.conf is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with org.everit.osgi.audit.ri.conf.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.everit.osgi.audit.ri.internal;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.everit.osgi.audit.dto.AuditEvent;
import org.everit.osgi.audit.dto.AuditEventType;
import org.everit.osgi.audit.dto.EventDataType;
import org.everit.osgi.audit.ri.conf.AuditRiConstants;
import org.everit.osgi.audit.ri.dto.AuditApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QEvent;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;
import org.everit.osgi.audit.ri.schema.qdsl.QEventType;
import org.everit.osgi.querydsl.support.QuerydslSupport;
import org.everit.osgi.resource.ResourceService;
import org.everit.osgi.transaction.helper.api.TransactionHelper;

import com.mysema.query.Tuple;
import com.mysema.query.sql.SQLQuery;
import com.mysema.query.sql.dml.SQLInsertClause;
import com.mysema.query.types.ConstructorExpression;
import com.mysema.query.types.template.BooleanTemplate;

@Component(name = AuditRiConstants.SERVICE_FACTORY_PID, metatype = true, configurationFactory = true,
        policy = ConfigurationPolicy.REQUIRE)
@Properties({
        @Property(name = AuditRiConstants.PROP_TRASACTION_HELPER),
        @Property(name = AuditRiConstants.PROP_QUERYDSL_SUPPORT),
        @Property(name = AuditRiConstants.PROP_RESOURCE_SERVICE)
})
@Service
public class AuditComponent implements AuditService {

    @Reference(bind = "setTransactionHelper")
    private TransactionHelper transactionHelper;

    @Reference(bind = "setQuerydslSupport")
    private QuerydslSupport querydslSupport;

    @Reference(bind = "setResourceService")
    private ResourceService resourceService;

    @Override
    public Application createApplication(final String appName) {
        return createApplication(appName, null);
    }

    @Override
    public Application createApplication(final String appName, final Long resourceId) {
        Objects.requireNonNull(appName, "appName cannot be null");

        return transactionHelper.required(() -> {

            return querydslSupport.execute((connection, configuration) -> {

                Long insertedResourceId = Optional.ofNullable(resourceId).orElseGet(resourceService::createResource);

                QApplication qApplication = QApplication.application;
                Long appId = new SQLInsertClause(connection, configuration, qApplication)
                        .set(qApplication.resourceId, insertedResourceId)
                        .set(qApplication.applicationName, appName)
                        .executeWithKey(qApplication.applicationId);

                return new Application(appId, appName, insertedResourceId);
            });
        });
    }

    private EventType createEventType(final Application app, final String eventTypeName) {
        return transactionHelper.required(() -> {

            return querydslSupport.execute((connection, configuration) -> {

                Long resourceId = resourceService.createResource();

                QEventType qEventType = QEventType.eventType;

                Long eventTypeId = new SQLInsertClause(connection, configuration, qEventType)
                        .set(qEventType.name, eventTypeName)
                        .set(qEventType.applicationId, app.getApplicationId())
                        .set(qEventType.resourceId, resourceId)
                        .executeWithKey(qEventType.eventTypeId);

                return new EventType(eventTypeId, eventTypeName, app.getApplicationId());
            });
        });
    }

    @Override
    public Application findApplicationByName(final String applicationName) {
        Objects.requireNonNull(applicationName, "applicationName cannot be null");

        return querydslSupport.execute((connection, configuration) -> {

            QApplication qApplication = QApplication.application;

            return new SQLQuery(connection, configuration)
                    .from(qApplication)
                    .where(qApplication.applicationName.eq(applicationName))
                    .uniqueResult(ConstructorExpression.create(Application.class,
                            qApplication.applicationId,
                            qApplication.applicationName,
                            qApplication.resourceId));
        });
    }

    @Override
    public List<EventUi> findEvents(final Long[] selectedAppIds, final Long[] selectedEventTypeIds,
            final List<String> dataFields, final List<DataFilter> dataFilters,
            final Instant eventsFrom, final Instant eventsTo,
            final long offset, final long limit) {

        return querydslSupport.execute((connection, configuration) -> {

            return new ComplexEventLoader(connection, configuration,
                    selectedAppIds,
                    selectedEventTypeIds, dataFields,
                    dataFilters, eventsFrom, eventsTo, offset, limit).loadEvents();
        });
    }

    private EventType findEventType(final long applicationId, final String eventTypeName) {
        return querydslSupport.execute((connection, configuration) -> {
            QEventType qEventType = QEventType.eventType;
            return new SQLQuery(connection, configuration)
                    .from(qEventType)
                    .where(qEventType.applicationId.eq(applicationId).and(qEventType.name.eq(eventTypeName)))
                    .uniqueResult(ConstructorExpression.create(EventType.class,
                            qEventType.eventTypeId,
                            qEventType.name,
                            qEventType.applicationId));
        });
    }

    @Override
    public AuditApplication getApplication(final String applicationName) {

        Objects.requireNonNull(applicationName, "applicationName cannot be null");

        AuditApplication cachedAuditApplication = auditApplicationCache.get(applicationName);
        if (cachedAuditApplication != null) {
            return cachedAuditApplication;
        }

        return transactionHelper.required(() -> {

            AuditApplication auditApplication = selectApplication(applicationName);
            if (auditApplication != null) {
                auditApplicationCache.put(applicationName, auditApplication);
            }

            return auditApplication;
        });
    }

    @Override
    public List<AuditApplication> getApplications() {
        return querydslSupport.execute((connection, configuration) -> {
            QApplication qApplication = QApplication.application;
            return new SQLQuery(connection, configuration)
                    .from(qApplication)
                    .listResults(ConstructorExpression.create(AuditApplication.class,
                            qApplication.applicationId,
                            qApplication.applicationName,
                            qApplication.resourceId))
                    .getResults();
        });
    }

    @Override
    public List<Application> getApplications() {
        return querydslSupport.execute((connection, configuration) -> {
            QApplication qApplication = QApplication.application;
            return new SQLQuery(connection, configuration)
                    .from(qApplication)
                    .listResults(ConstructorExpression.create(Application.class,
                            qApplication.applicationId,
                            qApplication.applicationName,
                            qApplication.resourceId)).getResults();
        });
    }

    @Override
    public AuditEventType getAuditEventType(final String eventTypeName) {
        return getAuditEventType(auditApplicationName, eventTypeName);
    }

    @Override
    public AuditEventType getAuditEventType(final String applicationName, final String eventTypeName) {

        Objects.requireNonNull(applicationName, "applicationName cannot be null");
        Objects.requireNonNull(eventTypeName, "eventTypeName cannot be null");

        AuditEventType cachedAuditEventType = auditEventTypeCache.get(new AuditEventTypeKey(applicationName,
                eventTypeName));
        if (cachedAuditEventType != null) {
            return cachedAuditEventType;
        }

        return transactionHelper.required(() -> {

            AuditEventType auditEventType = selectAuditEventType(applicationName, eventTypeName);
            if (auditEventType != null) {
                auditEventTypeCache.put(new AuditEventTypeKey(applicationName, eventTypeName), auditEventType);
            }

            return auditEventType;
        });
    }

    @Override
    public List<AuditEventType> getAuditEventTypes() {
        return getAuditEventTypes(auditApplicationName);
    }

    @Override
    public List<AuditEventType> getAuditEventTypes(final String applicationName) {
        return querydslSupport.execute((connection, configuration) -> {

            QEventType qEventType = QEventType.eventType;
            QApplication qApplication = QApplication.application;

            return new SQLQuery(connection, configuration)
                    .from(qEventType)
                    .innerJoin(qApplication).on(qEventType.applicationId.eq(qApplication.applicationId))
                    .where(qApplication.applicationName.eq(applicationName))
                    .list(ConstructorExpression.create(AuditEventType.class,
                            qEventType.eventTypeId,
                            qEventType.name,
                            qEventType.resourceId));
        });
    }

    @Override
    public EventUi getEventById(final long eventId, final String... dataFields) {
        return querydslSupport.execute((connection, configuration) -> {
            QEventData qEventData = QEventData.eventData;
            SingleEventLoader singleEventLoader = new SingleEventLoader(connection, configuration);
            if ((dataFields == null) || (dataFields.length == 0)) {
                return singleEventLoader.loadEvent(eventId, BooleanTemplate.TRUE);
            } else {
                return singleEventLoader.loadEvent(eventId, qEventData.eventDataName.in(dataFields));
            }
        });
    }

    @Override
    public EventType getEventTypeByNameForApplication(final long applicationId, final String eventName)
            throws IllegalArgumentException {
        Objects.requireNonNull(eventName, "eventName cannot be null");
        return querydslSupport.execute((connection, configuration) -> {
            QEventType qEventType = QEventType.eventType;
            return new SQLQuery(connection, configuration)
                    .from(qEventType)
                    .where(qEventType.applicationId.eq(applicationId))
                    .where(qEventType.name.eq(eventName))
                    .uniqueResult(ConstructorExpression.create(EventType.class,
                            qEventType.eventTypeId,
                            qEventType.name,
                            qEventType.applicationId));
        });
    }

    @Override
    public List<EventType> getEventTypesByApplication(final long applicationId) {
        return querydslSupport.execute((connection, configuration) -> {
            QEventType qEventType = QEventType.eventType;
            return new SQLQuery(connection, configuration)
                    .from(qEventType)
                    .where(qEventType.applicationId.eq(applicationId))
                    .list(ConstructorExpression.create(EventType.class,
                            qEventType.eventTypeId,
                            qEventType.name,
                            qEventType.applicationId));
        });
    }

    @Override
    public Application getOrCreateApplication(final String applicationName) {
        return Optional.ofNullable(findApplicationByName(applicationName)).orElseGet(
                () -> createApplication(applicationName));
    }

    @Override
    public EventType getOrCreateEventType(final String applicationName, final String eventTypeName) {
        Objects.requireNonNull(applicationName, "applicationName cannot be null");
        Objects.requireNonNull(eventTypeName, "eventTypeName cannot be null");
        return transactionHelper.required(() -> {

            Application app = requireAppByName(applicationName);
            return Optional.ofNullable(findEventType(app.getApplicationId(), eventTypeName))
                    .orElseGet(() -> createEventType(app, eventTypeName));

        });
    }

    @Override
    public EventType[] getOrCreateEventTypesForApplication(final String applicationName, final String[] eventTypeNames) {
        Objects.requireNonNull(applicationName, "applicationName cannot be null");
        Objects.requireNonNull(eventTypeNames, "eventTypeNames cannot be null");
        return transactionHelper.required(() -> {
            requireAppByName(applicationName);
            EventType[] rval = new EventType[eventTypeNames.length];
            int idx = 0;
            for (String typeName : eventTypeNames) {
                rval[idx++] = getOrCreateEventType(applicationName, typeName);
            }
            return rval;
        });
    }

    @Override
    public List<FieldWithType> getResultFieldsWithTypes(final Long[] selectedAppId, final Long[] selectedEventTypeId) {
        return querydslSupport.execute((connection, configuration) -> {

            QEvent qEvent = QEvent.event;
            QEventType qEventType = QEventType.eventType;
            QEventData qEventData = QEventData.eventData;

            List<Tuple> rawResult = new SQLQuery(connection, configuration)
                    .from(qEvent)
                    .join(qEventData).on(qEvent.eventId.eq(qEventData.eventId))
                    .join(qEventType).on(qEvent.eventTypeId.eq(qEventType.eventTypeId))
                    .where(qEventType.applicationId.in(Arrays.asList(selectedAppId))
                            .and(qEventType.eventTypeId.in(Arrays.asList(selectedEventTypeId))))
                    .distinct()
                    .list(qEventData.eventDataName, qEventData.eventDataType);

            return rawResult.stream().map((row) ->
                    new FieldWithType(
                            row.get(qEventData.eventDataName),
                            EventDataType.valueOf(row.get(qEventData.eventDataType)),
                            null))
                    .collect(Collectors.toList());
        });
    }

    @Override
    public void logEvent(final AuditEvent event) {
        transactionHelper.required(() -> {
            EventType eventType = getOrCreateEventType(event.getApplicationName(), event.getName());
            return new EventPersister(transactionHelper, querydslSupport, eventType.getId(), event).get();
        });
    }

    private Application requireAppByName(final String applicationName) {
        return Optional
                .ofNullable(findApplicationByName(applicationName))
                .orElseThrow(() -> new IllegalArgumentException("application [" + applicationName + "] does not exist"));
    }

    public void setQuerydslSupport(final QuerydslSupport querydslSupport) {
        this.querydslSupport = querydslSupport;
    }

    public void setResourceService(final ResourceService resourceService) {
        this.resourceService = resourceService;
    }

    public void setTransactionHelper(final TransactionHelper transactionHelper) {
        this.transactionHelper = transactionHelper;
    }

}
