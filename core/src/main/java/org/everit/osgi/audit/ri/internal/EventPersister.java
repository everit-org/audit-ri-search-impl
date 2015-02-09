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

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.function.Supplier;

import javax.sql.rowset.serial.SerialBlob;

import org.everit.osgi.audit.dto.AuditEvent;
import org.everit.osgi.audit.dto.EventData;
import org.everit.osgi.audit.ri.schema.qdsl.QEvent;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;
import org.everit.osgi.querydsl.support.QuerydslSupport;
import org.everit.osgi.transaction.helper.api.TransactionHelper;

import com.mysema.query.sql.dml.SQLInsertClause;

public class EventPersister implements Supplier<Void> {

    private final QuerydslSupport querydslSupport;

    private final AuditEvent event;

    private final long eventTypeId;

    private final TransactionHelper transactionHelper;

    public EventPersister(final TransactionHelper transactionHelper, final QuerydslSupport querydslSupport,
            final long eventTypeId, final AuditEvent event) {
        this.transactionHelper = transactionHelper;
        this.querydslSupport = querydslSupport;
        this.eventTypeId = eventTypeId;
        this.event = event;
    }

    private void addEventDataValue(
            final SQLInsertClause insert, final QEventData qEventData, final EventData eventData) {
        switch (eventData.getEventDataType()) {
        case NUMBER:
            insert.set(qEventData.numberValue, eventData.getNumberValue());
            break;
        case STRING:
            insert.set(qEventData.stringValue, eventData.getTextValue());
            break;
        case TEXT:
            insert.set(qEventData.textValue, eventData.getTextValue());
            break;
        case BINARY:
            try {
                insert.set(qEventData.binaryValue, new SerialBlob(eventData.getBinaryValue()));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            break;
        case TIMESTAMP:
            insert.set(qEventData.timestampValue, Timestamp.from(eventData.getTimestampValue()));
            break;
        }
    }

    @Override
    public Void get() {
        return transactionHelper.required(() -> {
            return querydslSupport.execute((connection, configuration) -> {

                QEvent qEvent = QEvent.event;

                long eventId = new SQLInsertClause(connection, configuration, qEvent)
                        .set(qEvent.saveTimestamp, Timestamp.from(event.getSaveTimeStamp()))
                        .set(qEvent.eventTypeId, eventTypeId)
                        .executeWithKey(qEvent.eventId);

                for (EventData eventData : event.getEventDataArray()) {
                    QEventData qEventData = QEventData.eventData;
                    SQLInsertClause insert = new SQLInsertClause(connection, configuration, qEventData)
                            .set(qEventData.eventId, eventId)
                            .set(qEventData.eventDataName, eventData.getName())
                            .set(qEventData.eventDataType, eventData.getEventDataType().toString());
                    addEventDataValue(insert, qEventData, eventData);
                    insert.execute();
                }
                return null;
            });
        });
    }

}
