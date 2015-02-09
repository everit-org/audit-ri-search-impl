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

import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.everit.osgi.audit.dto.EventDataType;
import org.everit.osgi.audit.ri.conf.search.api.EventUi.Builder;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;

import com.mysema.query.Tuple;

public class EventDataRowMapper {

    private final QEventData evtDataAlias;

    public EventDataRowMapper(final QEventData evtDataAlias) {
        this.evtDataAlias = evtDataAlias;
    }

    private void addBlobData(final Builder builder, final String dataName, final Tuple row) {
        Blob blob = row.get(evtDataAlias.binaryValue);
        try {
            try {
                builder.binaryData(dataName, blob.getBytes(0, (int) blob.length()));
            } finally {
                blob.free();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void addEventDataForRow(final Builder builder, final Tuple row) {
        Objects.requireNonNull(builder, "builder cannot be null");
        Objects.requireNonNull(row, "row cannot be null");
        String type = Optional.ofNullable(row.get(evtDataAlias.eventDataType))
                .orElseThrow(() -> new IllegalArgumentException("row has null value for eventData.eventDataType"));
        String dataName = row.get(evtDataAlias.eventDataName);
        if (type.equals(EventDataType.BINARY.toString())) {
            addBlobData(builder, dataName, row);
        } else if (type.equals(EventDataType.STRING.toString())) {
            builder.stringData(dataName, row.get(evtDataAlias.stringValue));
        } else if (type.equals(EventDataType.TEXT.toString())) {
            builder.textData(dataName, row.get(evtDataAlias.textValue));
        } else if (type.equals(EventDataType.NUMBER.toString())) {
            builder.numberData(dataName, row.get(evtDataAlias.numberValue));
        } else if (type.equals(EventDataType.TIMESTAMP)) {
            Timestamp timestamp = row.get(evtDataAlias.timestampValue);
            builder.timestampData(dataName, Instant.ofEpochSecond(timestamp.getTime() / 1000, timestamp.getNanos()));
        } else {
            throw new IllegalStateException("unknown event data type: " + type);
        }
    }

}
