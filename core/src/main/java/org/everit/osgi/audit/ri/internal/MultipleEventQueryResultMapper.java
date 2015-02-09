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
/**
 * This file is part of org.everit.osgi.audit.ri.conf.internal.
 *
 * org.everit.osgi.audit.ri.conf.internal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * org.everit.osgi.audit.ri.conf.internal is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with org.everit.osgi.audit.ri.conf.internal.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.everit.osgi.audit.ri.internal;

import java.util.ArrayList;
import java.util.List;

import org.everit.osgi.audit.ri.conf.search.api.EventUi;
import org.everit.osgi.audit.ri.schema.qdsl.QApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QEvent;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;
import org.everit.osgi.audit.ri.schema.qdsl.QEventType;

import com.mysema.query.Tuple;

public class MultipleEventQueryResultMapper {

    private final List<Tuple> rawResult;

    private final QApplication qApplication = QApplication.application;

    private final QEventType qEventType = QEventType.eventType;

    private final QEvent qEvent;

    private final QEventData qEventData;

    public MultipleEventQueryResultMapper(final List<Tuple> rawResult, final QEvent qEvent, final QEventData qEventData) {
        this.rawResult = rawResult;
        this.qEvent = qEvent;
        this.qEventData = qEventData;
    }

    List<EventUi> mapToEvents() {
        List<EventUi> rval = new ArrayList<EventUi>();
        Long prevEventId = null;
        EventUi.Builder underConstruction = null;
        EventDataRowMapper rowDataMapper = new EventDataRowMapper(qEventData);
        for (Tuple row : rawResult) {
            Long eventId = row.get(qEvent.eventId);
            if ((prevEventId == null) || !eventId.equals(prevEventId)) {
                if (underConstruction != null) {
                    rval.add(underConstruction.build());
                }
                underConstruction = new EventUi.Builder()
                        .eventId(row.get(qEvent.eventId))
                        .typeName(row.get(qEventType.name))
                        .appName(row.get(qApplication.applicationName))
                        .saveTimestamp(row.get(qEvent.saveTimestamp).toInstant());
                prevEventId = eventId;
            }
            rowDataMapper.addEventDataForRow(underConstruction, row);
        }
        return rval;
    }

}
