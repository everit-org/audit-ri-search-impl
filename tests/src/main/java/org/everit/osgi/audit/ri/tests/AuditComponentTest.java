/**
 * This file is part of Everit - Audit Reference Implementation Tests.
 *
 * Everit - Audit Reference Implementation Tests is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Everit - Audit Reference Implementation Tests is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Everit - Audit Reference Implementation Tests.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.everit.osgi.audit.ri.tests;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.everit.osgi.audit.dto.AuditEvent;
import org.everit.osgi.audit.dto.AuditEventType;
import org.everit.osgi.audit.dto.EventData;
import org.everit.osgi.audit.dto.EventDataType;
import org.everit.osgi.audit.ri.dto.AuditApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QEvent;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;
import org.everit.osgi.audit.ri.schema.qdsl.QEventType;
import org.everit.osgi.dev.testrunner.TestRunnerConstants;
import org.everit.osgi.querydsl.support.QuerydslSupport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.mysema.query.QueryException;
import com.mysema.query.sql.SQLQuery;
import com.mysema.query.sql.dml.SQLDeleteClause;
import com.mysema.query.types.ConstructorExpression;
import com.mysema.query.types.Operator;

@Component(name = "AuditComponentTest", immediate = true, configurationFactory = false,
        policy = ConfigurationPolicy.OPTIONAL)
@Properties({
        @Property(name = TestRunnerConstants.SERVICE_PROPERTY_TESTRUNNER_ENGINE_TYPE, value = "junit4"),
        @Property(name = TestRunnerConstants.SERVICE_PROPERTY_TEST_ID, value = "auditTest"),
        @Property(name = "auditComponent.target"),
        @Property(name = "querydslSupport.target")
})
@Service(AuditComponentTest.class)
public class AuditComponentTest {

    private static final String APPNAME = "appname";

    @Reference(bind = "setQuerydslSupport")
    private QuerydslSupport querydslSupport;

    @Reference(bind = "setAuditComponent")
    private AuditService auditComponent;

    @After
    public void cleanupDatabase() {
        querydslSupport.execute((connection, configuration) -> {
            new SQLDeleteClause(connection, configuration, QEventData.eventData).execute();
            new SQLDeleteClause(connection, configuration, QEvent.event).execute();
            new SQLDeleteClause(connection, configuration, QEventType.eventType).execute();
            new SQLDeleteClause(connection, configuration, QApplication.application).execute();
            return null;
        });
    }

    @Test(expected = QueryException.class)
    public void createApplicationConstraintViolation() {
        createDefaultApp();
        createDefaultApp();
    }

    @Test(expected = NullPointerException.class)
    public void createApplicationFail() {
        auditComponent.createApplication(null);
    }

    @Test
    public void createApplicationSuccess() {
        createDefaultApp();
        QApplication qApplication = QApplication.application;
        querydslSupport.execute((connection, configuration) -> {
            Application result = new SQLQuery(connection, configuration)
                    .from(qApplication)
                    .where(qApplication.applicationName.eq(APPNAME))
                    .uniqueResult(ConstructorExpression.create(Application.class,
                            qApplication.applicationId,
                            qApplication.applicationName,
                            qApplication.resourceId));
            Assert.assertEquals(APPNAME, result.getAppName());
            Assert.assertNotNull(result.getResourceId());
            return null;
        });
    }

    private Application createDefaultApp() {
        return auditComponent.createApplication(APPNAME);
    }

    @Test
    public void createEventType() {
        Application app = createDefaultApp();
        EventType actual = auditComponent.getOrCreateEventType(APPNAME, "login");
        Assert.assertNotNull(actual);
        Assert.assertEquals("login", actual.getName());
        Assert.assertEquals(app.getApplicationId(), actual.getApplicationId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createEventTypeNonexistentApp() {
        auditComponent.getOrCreateEventType("nonexistent", "login");
    }

    @Test(expected = NullPointerException.class)
    public void findApplicationByNameFilure() {
        auditComponent.findApplicationByName(null);
    }

    @Test
    public void findApplicationByNameSuccess() {
        createDefaultApp();
        Application actual = auditComponent.findApplicationByName(APPNAME);
        Assert.assertNotNull(actual);
    }

    @Test
    public void findEvents() {
        Application app = createDefaultApp();
        Long[] appIds = new Long[] { app.getApplicationId() };
        EventType[] evtTypes = auditComponent.getOrCreateEventTypesForApplication(app.getAppName(), new String[] {
                "evtType0",
                "evtType1", "evtType2" });
        auditComponent.logEvent(new AuditEvent("evtType0", APPNAME, new EventData[] { new EventData("strData", "aaa"),
                new EventData("intData", 10) }));
        auditComponent.logEvent(new AuditEvent("evtType0", APPNAME, new EventData[] { new EventData("strData", "aaa"),
                new EventData("intData", 20) }));
        auditComponent.logEvent(new AuditEvent("evtType1", APPNAME, new EventData[] { new EventData("strData", "bbb"),
                new EventData("textData", false, "longtext") }));
        auditComponent
                .logEvent(new AuditEvent("evtType2", APPNAME, new EventData[] { new EventData("strData", "ccc") }));
        Long[] eventTypeIds = new Long[] { evtTypes[0].getId(), evtTypes[1].getId() };
        List<String> dataFields = Arrays.asList("strData", "intData");
        List<DataFilter> dataFilters = Arrays.asList(new DataFilter(Operator.GT, new EventData("intData", 15)));
        List<EventUi> actual = auditComponent.findEvents(appIds, eventTypeIds, dataFields, dataFilters,
                null, null, 0, 100);
        Assert.assertNotNull(actual);
        Assert.assertEquals(2, actual.size());
        EventUi firstResult = actual.get(0);
        Assert.assertEquals("evtType1", firstResult.getName());
        Assert.assertEquals("bbb", firstResult.getEventData().get("strData").getTextValue());
        Assert.assertNull(firstResult.getEventData().get("textData"));
        EventUi secondResult = actual.get(1);
        Assert.assertEquals("evtType0", secondResult.getName());
        Assert.assertEquals("aaa", secondResult.getEventData().get("strData").getTextValue());
        Assert.assertEquals(20.0, secondResult.getEventData().get("intData").getNumberValue(), 0.1);
    }

    @Test
    public void findEventsEmptyResult() {
        long appId = createDefaultApp().getApplicationId();
        List<EventUi> actual = auditComponent.findEvents(new Long[] { new Long(appId) }, null, null, null,
                null, null, 0, 10000);
        Assert.assertNotNull(actual);
        Assert.assertEquals(0, actual.size());
    }

    @Test
    public void getApplications() {
        List<Application> actual = auditComponent.getApplications();
        Assert.assertEquals(0, actual.size());
        auditComponent.createApplication("app1");
        auditComponent.createApplication("app2");
        actual = auditComponent.getApplications();
        Assert.assertEquals(2, actual.size());
    }

    @Test
    public void getEventById() {
        createDefaultApp();
        long eventId = logDefaultEvent();
        EventUi event = auditComponent.getEventById(eventId);
        Assert.assertNotNull(event);
        Assert.assertEquals(eventId, event.getId().longValue());
        Assert.assertEquals("login", event.getName());
        Assert.assertEquals(APPNAME, event.getApplicationName());
        Assert.assertNotNull(event.getSaveTimeStamp());
        Assert.assertNotNull(event.getEventData());
        Assert.assertEquals(2, event.getEventData().size());
        EventData hostData = event.getEventData().get("host");
        Assert.assertEquals("example.org", hostData.getTextValue());
        Assert.assertEquals(EventDataType.STRING, hostData.getEventDataType());
        EventData cpuLoadData = event.getEventData().get("cpuLoad");
        Assert.assertEquals(10.75, cpuLoadData.getNumberValue(), 0.01);
        Assert.assertEquals(EventDataType.NUMBER, cpuLoadData.getEventDataType());
    }

    @Test
    public void getEventByIdNoData() {
        createDefaultApp();
        AuditEvent event = new AuditEvent("login", APPNAME, new EventData[] {});
        auditComponent.logEvent(event);
        querydslSupport.execute((connection, configuration) -> {
            QEvent qEvent = QEvent.event;
            long eventId = new SQLQuery(connection, configuration)
                    .from(qEvent)
                    .orderBy(qEvent.eventId.desc())
                    .limit(1)
                    .uniqueResult(qEvent.eventId);
            EventUi result = auditComponent.getEventById(eventId);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getEventData().isEmpty());
            return null;
        });
    }

    @Test
    public void getEventByIdNotFound() {
        Assert.assertNull(auditComponent.getEventById(-1));
    }

    @Test
    public void getEventType() {
        createDefaultApp();
        EventType firstEvtType = auditComponent.getOrCreateEventType(APPNAME, "login");
        EventType secondEvtType = auditComponent.getOrCreateEventType(APPNAME, "login");
        Assert.assertEquals(firstEvtType.getId(), secondEvtType.getId());
    }

    @Test
    public void getEventTypeByNameForApplicationFail() {
        createDefaultApp();
        Assert.assertNull(auditComponent.getEventTypeByNameForApplication(0L, "nonexistent"));
    }

    @Test(expected = NullPointerException.class)
    public void getEventTypeByNameForApplicationNullEventName() {
        auditComponent.getEventTypeByNameForApplication(0, null);
    }

    @Test
    public void getEventTypeByNameForApplicationSuccess() {
        Application app = auditComponent.createApplication(APPNAME);
        auditComponent.getOrCreateEventType(APPNAME, "login");
        auditComponent.getOrCreateEventType(APPNAME, "logout");
        EventType actual = auditComponent.getEventTypeByNameForApplication(app.getApplicationId(), "login");
        Assert.assertNotNull(actual);
        Assert.assertEquals(app.getApplicationId(), actual.getApplicationId());
        Assert.assertEquals("login", actual.getName());
    }

    @Test
    public void getEventTypesByApplication() {
        Application app = auditComponent.createApplication(APPNAME);
        auditComponent.getOrCreateEventType(APPNAME, "login");
        auditComponent.getOrCreateEventType(APPNAME, "logout");
        List<EventType> events = auditComponent.getEventTypesByApplication(app.getApplicationId());
        Assert.assertNotNull(events);
        Assert.assertEquals(2, events.size());
    }

    @Test
    public void getOrCreateApplication() {
        Application newApp = auditComponent.getOrCreateApplication(APPNAME);
        Assert.assertNotNull(newApp);
        Application existingApp = auditComponent.getOrCreateApplication(APPNAME);
        Assert.assertNotNull(existingApp);
        Assert.assertEquals(newApp.getApplicationId(), existingApp.getApplicationId());
    }

    @Test(expected = NullPointerException.class)
    public void getOrCreateApplicationNullName() {
        auditComponent.getOrCreateApplication(null);
    }

    @Test(expected = NullPointerException.class)
    public void getOrCreateEventTypeNullAppName() {
        auditComponent.getOrCreateEventType(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void getOrCreateEventTypeNullEventName() {
        auditComponent.getOrCreateEventType(APPNAME, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getOrCreateEventTypesForAppNonexistentApplication() {
        auditComponent.getOrCreateEventTypesForApplication("nonexistent", new String[] {});
    }

    @Test(expected = NullPointerException.class)
    public void getOrCreateEventTypesForAppNullAppName() {
        auditComponent.getOrCreateEventTypesForApplication(null, null);
    }

    @Test(expected = NullPointerException.class)
    public void getOrCreateEventTypesForAppNullEventTypeNames() {
        auditComponent.getOrCreateEventTypesForApplication(APPNAME, null);
    }

    @Test
    public void getOrCreateEventTypesForAppSuccess() {
        Application app = auditComponent.createApplication(APPNAME);
        auditComponent.getOrCreateEventType(APPNAME, "login");
        auditComponent.getOrCreateEventType(APPNAME, "logout");
        auditComponent.getOrCreateEventTypesForApplication(APPNAME, new String[] { "addComment", "balanceUpdate",
                "viewProduct" });
        List<EventType> actual = auditComponent.getEventTypesByApplication(app.getApplicationId());
        Assert.assertEquals(5, actual.size());
    }

    private long logDefaultEvent() {
        EventData[] eventDataArray = new EventData[] {
                new EventData("host", "example.org"),
                new EventData("cpuLoad", 10.75)
        };
        AuditEvent event = new AuditEvent("login", APPNAME, eventDataArray);
        auditComponent.logEvent(event);
        return querydslSupport.execute((connection, configuration) -> {
            QEvent qEvent = QEvent.event;
            return new SQLQuery(connection, configuration)
                    .from(qEvent)
                    .orderBy(qEvent.eventId.desc())
                    .limit(1)
                    .uniqueResult(qEvent.eventId);
        });
    }

    @Test
    public void logEvent() {
        createDefaultApp();
        logDefaultEvent();
        querydslSupport.execute((connection, configuration) -> {
            QEvent qEvent = QEvent.event;
            Long eventId = new SQLQuery(connection, configuration)
                    .from(qEvent).limit(1)
                    .uniqueResult(ConstructorExpression.create(Long.class, qEvent.eventId));
            Assert.assertNotNull(eventId);
            QEventData qEventData = QEventData.eventData;
            long dataCount = new SQLQuery(connection, configuration)
                    .from(qEventData)
                    .where(qEventData.eventId.eq(eventId))
                    .count();
            Assert.assertEquals(2, dataCount);
            return null;
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void logEventMissingApplication() {
        EventData[] eventDataArray = new EventData[] {};
        AuditEvent evt = new AuditEvent("login", APPNAME, eventDataArray);
        auditComponent.logEvent(evt);
    }

    @Test
    public void notFindApplicationByName() {
        Assert.assertNull(auditComponent.findApplicationByName("nonexistent"));
    }

    @Test
    public void readEventAllDataFields() {
        createDefaultApp();
        long eventId = logDefaultEvent();
        EventUi event = auditComponent.getEventById(eventId, "host", "cpuLoad");
        Assert.assertNotNull(event);
        Assert.assertEquals(eventId, event.getId().longValue());
        Assert.assertEquals("login", event.getName());
        Assert.assertEquals(APPNAME, event.getApplicationName());
        Assert.assertNotNull(event.getSaveTimeStamp());
        Assert.assertNotNull(event.getEventData());
        Assert.assertEquals(2, event.getEventData().size());
        EventData hostData = event.getEventData().get("host");
        Assert.assertEquals("example.org", hostData.getTextValue());
        Assert.assertEquals(EventDataType.STRING, hostData.getEventDataType());
        EventData cpuLoadData = event.getEventData().get("cpuLoad");
        Assert.assertEquals(10.75, cpuLoadData.getNumberValue(), 0.01);
        Assert.assertEquals(EventDataType.NUMBER, cpuLoadData.getEventDataType());
    }

    @Test
    public void readEventMissingData() {
        createDefaultApp();
        long eventId = logDefaultEvent();
        EventUi event = auditComponent.getEventById(eventId, "nonexistentDataType");
        Assert.assertNotNull(event);
        Assert.assertEquals(eventId, event.getId().longValue());
        Assert.assertEquals("login", event.getName());
        Assert.assertEquals(APPNAME, event.getApplicationName());
        Assert.assertNotNull(event.getSaveTimeStamp());
        Assert.assertNotNull(event.getEventData());
        Assert.assertEquals(0, event.getEventData().size());
    }

    @Test
    public void readEventOneDataField() {
        createDefaultApp();
        long eventId = logDefaultEvent();
        EventUi event = auditComponent.getEventById(eventId, "host");
        Assert.assertNotNull(event);
        Assert.assertEquals(eventId, event.getId().longValue());
        Assert.assertEquals("login", event.getName());
        Assert.assertEquals(APPNAME, event.getApplicationName());
        Assert.assertNotNull(event.getSaveTimeStamp());
        Assert.assertNotNull(event.getEventData());
        Assert.assertEquals(1, event.getEventData().size());
        EventData hostData = event.getEventData().get("host");
        Assert.assertEquals("example.org", hostData.getTextValue());
        Assert.assertEquals(EventDataType.STRING, hostData.getEventDataType());
    }

    public void setAuditComponent(final AuditService auditComponent) {
        this.auditComponent = auditComponent;
    }

    public void setQuerydslSupport(final QuerydslSupport querydslSupport) {
        this.querydslSupport = querydslSupport;
    }

    @Test
    public void testGetApplication() {
        createAuditApplication();
        AuditApplication actual = auditApplicationManager.getApplication(TEST_APPLICATION_NAME);
        Assert.assertNotNull(actual);
        AuditApplication cachedApplication = auditApplicationManager.getApplication(TEST_APPLICATION_NAME);
        Assert.assertEquals(actual, cachedApplication);
    }

    @Test(expected = NullPointerException.class)
    public void testGetApplicationByNameFailure() {
        auditApplicationManager.getApplication(null);
    }

    @Test
    public void testGetApplications() {
        List<AuditApplication> actual = auditApplicationManager.getApplications();
        int initialSize = actual.size();
        auditApplicationManager.createApplication(resourceService.createResource(), "app1");
        auditApplicationManager.createApplication(resourceService.createResource(), "app2");
        actual = auditApplicationManager.getApplications();
        Assert.assertEquals(initialSize + 2, actual.size());
    }

    @Test
    public void testGetEventTypeByName() {
        AuditEventType loginAuditEventType = auditEventTypeManager.getOrCreateAuditEventTypes("login", "logout").get(0);
        AuditEventType auditEventType = auditEventTypeManager.getAuditEventType("login");
        Assert.assertNotNull(auditEventType);
        Assert.assertEquals(loginAuditEventType.getId(), auditEventType.getId());
        Assert.assertEquals(loginAuditEventType.getResourceId(), auditEventType.getResourceId());
        Assert.assertEquals(loginAuditEventType.getName(), auditEventType.getName());

        AuditEventType cachedAuditEventType = auditEventTypeManager.getAuditEventType("login");
        Assert.assertEquals(auditEventType, cachedAuditEventType);

    }

    @Test
    public void testGetEventTypeByNameFail() {
        Assert.assertNull(auditEventTypeManager.getAuditEventType(UUID.randomUUID().toString()));
    }

    @Test(expected = NullPointerException.class)
    public void testGetEventTypeByNameNPE() {
        auditEventTypeManager.getAuditEventType(null);
    }

    @Test
    public void testNullGetApplicationByName() {
        Assert.assertNull(auditApplicationManager.getApplication("nonexistent"));
    }

}
