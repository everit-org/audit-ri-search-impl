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

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;

import org.everit.osgi.audit.ri.conf.search.api.EventUi;
import org.everit.osgi.audit.ri.schema.qdsl.QApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QEvent;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;
import org.everit.osgi.audit.ri.schema.qdsl.QEventType;

import com.mysema.query.Tuple;
import com.mysema.query.sql.Configuration;
import com.mysema.query.sql.SQLQuery;
import com.mysema.query.sql.SQLSubQuery;
import com.mysema.query.types.expr.BooleanExpression;

public class SingleEventLoader {

    private final Connection connection;

    private final Configuration configuration;

    private final QApplication qApplication = QApplication.application;

    private final QEventType qEventType = QEventType.eventType;

    private final QEvent qEvent = QEvent.event;

    private QEventData qEventData = QEventData.eventData;

    private SQLQuery query;

    public SingleEventLoader(final Connection connection, final Configuration configuration) {
        this.connection = connection;
        this.configuration = configuration;
    }

    private QEventData addFilteredEventDataSubquery(final BooleanExpression eventDataPred) {
        QEventData qEventData = QEventData.eventData;
        query.leftJoin(new SQLSubQuery()
                .from(qEventData)
                .where(eventDataPred)
                .list(qEventData.eventId,
                        qEventData.eventDataName,
                        qEventData.eventDataType,
                        qEventData.numberValue,
                        qEventData.stringValue,
                        qEventData.textValue,
                        qEventData.timestampValue,
                        qEventData.binaryValue), qEventData).on(qEvent.eventId.eq(qEventData.eventId));
        return qEventData;
    }

    private void joinEventData(final BooleanExpression eventDataPred) {
        if (eventDataPred == null) {
            query.leftJoin(qEventData).on(qEvent.eventId.eq(qEventData.eventId));
        } else {
            qEventData = addFilteredEventDataSubquery(eventDataPred);
        }
    }

    public EventUi loadEvent(final long eventId) {
        List<Tuple> singleEventResult = singleEventQuery(eventId, null);
        return mapToEvent(singleEventResult);
    }

    public EventUi loadEvent(final long eventId, final BooleanExpression eventDataPred) {
        List<Tuple> singleEventResult = singleEventQuery(eventId, eventDataPred);
        return mapToEvent(singleEventResult);
    }

    private EventUi mapToEvent(final List<Tuple> result) {
        Iterator<Tuple> resultIt = result.iterator();
        if (!resultIt.hasNext()) {
            return null;
        }
        Tuple firstRow = resultIt.next();
        EventUi.Builder builder = new EventUi.Builder()
                .eventId(firstRow.get(qEvent.eventId))
                .typeName(firstRow.get(qEventType.name))
                .appName(firstRow.get(qApplication.applicationName))
                .saveTimestamp(firstRow.get(qEvent.saveTimestamp).toInstant());
        if (firstRow.get(qEventData.eventDataType) == null) { // no event data belongs to the event
            return builder.build();
        }
        EventDataRowMapper rowDataMapper = new EventDataRowMapper(qEventData);
        rowDataMapper.addEventDataForRow(builder, firstRow);
        while (resultIt.hasNext()) {
            Tuple row = resultIt.next();
            rowDataMapper.addEventDataForRow(builder, row);
        }
        return builder.build();
    }

    private List<Tuple> singleEventQuery(
            final long eventId,
            final BooleanExpression eventDataPred) {
        query = new SQLQuery(connection, configuration)
                .from(qEvent)
                .join(qEventType).on(qEvent.eventTypeId.eq(qEventType.eventTypeId))
                .join(qApplication).on(qApplication.applicationId.eq(qEventType.applicationId));
        joinEventData(eventDataPred);
        query.where(qEvent.eventId.eq(eventId));
        return query.list(
                qApplication.applicationName,
                qEventType.name,
                qEvent.eventId,
                qEvent.saveTimestamp,
                qEventData.eventDataName,
                qEventData.eventDataType,
                qEventData.numberValue,
                qEventData.stringValue,
                qEventData.textValue,
                qEventData.timestampValue,
                qEventData.binaryValue);
    }

}
