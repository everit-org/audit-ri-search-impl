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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.everit.osgi.audit.dto.EventData;
import org.everit.osgi.audit.ri.conf.search.api.DataFilter;
import org.everit.osgi.audit.ri.conf.search.api.EventUi;
import org.everit.osgi.audit.ri.conf.search.api.Operator;
import org.everit.osgi.audit.ri.schema.qdsl.QApplication;
import org.everit.osgi.audit.ri.schema.qdsl.QEvent;
import org.everit.osgi.audit.ri.schema.qdsl.QEventData;
import org.everit.osgi.audit.ri.schema.qdsl.QEventType;

import com.mysema.query.Tuple;
import com.mysema.query.sql.Configuration;
import com.mysema.query.sql.SQLQuery;
import com.mysema.query.sql.SQLSubQuery;
import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Ops;
import com.mysema.query.types.expr.BooleanExpression;

public class ComplexEventLoader {

    private static final Map<Operator, com.mysema.query.types.Operator<Boolean>> operatorMapping = new HashMap<>();

    static {
        operatorMapping.put(Operator.EQ, Ops.EQ);
        operatorMapping.put(Operator.LT, Ops.LT);
        operatorMapping.put(Operator.GT, Ops.GT);
        operatorMapping.put(Operator.STARTS_WITH, Ops.STARTS_WITH);
    }

    private final Connection connection;

    private final Configuration configuration;

    private SQLQuery query;

    private QEvent evtSubqueryAlias;

    private QEventData evtDataSubqueryAlias;

    private final Collection<Long> selectedAppIds;

    private final Collection<Long> selectedEventTypeIds;

    private final Optional<List<String>> dataFields;

    private final Optional<List<DataFilter>> dataFilters;

    private final Instant eventsFrom;

    private final Instant eventsTo;

    private final long offset;

    private final long limit;

    private final QEventType qEventType = QEventType.eventType;

    private final QApplication qApplication = QApplication.application;

    public ComplexEventLoader(final Connection connection, final Configuration configuration,
            final Long[] selectedAppIds,
            final Long[] selectedEventTypeIds,
            final List<String> dataFields,
            final List<DataFilter> dataFilters,
            final Instant eventsFrom, final Instant eventsTo,
            final long offset, final long limit) {
        this.connection = connection;
        this.configuration = configuration;
        this.selectedAppIds = Arrays.asList(Objects.requireNonNull(selectedAppIds, "selectedAppIds cannot be null"));
        this.selectedEventTypeIds = selectedEventTypeIds == null ? null : Arrays.asList(selectedEventTypeIds);
        this.dataFilters = Optional.ofNullable(dataFilters);
        this.dataFields = Optional.ofNullable(dataFields);
        this.eventsFrom = eventsFrom;
        this.eventsTo = eventsTo;
        this.offset = offset;
        this.limit = limit;
    }

    private void addOrderBy() {
        query = query.orderBy(evtSubqueryAlias.saveTimestamp.desc(), evtSubqueryAlias.eventId.asc());
    }

    private void buildEventDataSubquery() {
        QEventData qEventData = QEventData.eventData;
        SQLSubQuery subQuery = new SQLSubQuery().from(qEventData);
        subQuery = subQuery.where(buildEventDataSubqueryPredicate());
        query = query.leftJoin(subQuery.list(
                qEventData.eventId,
                // localization.getLocalizedValue(evtData.eventDataName, locale),
                qEventData.eventDataName,
                qEventData.eventDataType,
                qEventData.numberValue,
                qEventData.stringValue,
                qEventData.textValue,
                qEventData.binaryValue,
                qEventData.timestampValue), evtDataSubqueryAlias = QEventData.eventData)
                .on(evtSubqueryAlias.eventId.eq(evtDataSubqueryAlias.eventId));
        // query = query.leftJoin(evtDataSubqueryAlias = evtData)
        // .on(evtSubqueryAlias.eventId.eq(evtDataSubqueryAlias.eventId));
    }

    private BooleanExpression buildEventDataSubqueryPredicate() {
        QEventData qEventData = QEventData.eventData;
        BooleanExpression fieldPredicate = dataFields
                .map((fields) -> qEventData.eventDataName.in(fields))
                .orElseGet(() -> Expressions.predicate(Ops.EQ, Expressions.constant(1), Expressions.constant(1)));
        return dataFilters.orElseGet(Collections::emptyList)
                .stream()
                .map(this::buildPredicateForFilter)
                .reduce(fieldPredicate, (pred1, pred2) -> pred1.and(pred2));
    }

    private BooleanExpression buildEventSubqueryPredicate() {
        QEvent qEvent = QEvent.event;
        QEventType qEventType = QEventType.eventType;
        BooleanExpression rval = qEventType.applicationId.in(selectedAppIds);
        if (selectedEventTypeIds != null) {
            rval = rval.and(qEvent.eventTypeId.in(selectedEventTypeIds));
        }
        if ((eventsFrom != null) && (eventsTo != null)) {
            rval = rval.and(qEvent.saveTimestamp.between(Timestamp.from(eventsFrom), Timestamp.from(eventsTo)));
        } else if (eventsFrom != null) {
            rval = rval.and(qEvent.saveTimestamp.gt(Timestamp.from(eventsFrom)));
        } else if (eventsTo != null) {
            rval = rval.and(qEvent.saveTimestamp.lt(Timestamp.from(eventsTo)));
        }
        return rval;
    }

    private void buildFromClause() {
        QEvent qEvent = QEvent.event;
        QEventType qEventType = QEventType.eventType;
        SQLSubQuery subQuery = new SQLSubQuery().from(qEvent)
                .leftJoin(qEventType).on(qEvent.eventTypeId.eq(qEventType.eventTypeId))
                .where(buildEventSubqueryPredicate())
                .offset(offset)
                .limit(limit);
        query = query.from(subQuery.list(qEvent.eventId, qEvent.saveTimestamp, qEvent.eventTypeId),
                evtSubqueryAlias = QEvent.event);
    }

    private BooleanExpression buildPredicateForFilter(final DataFilter dataFilter) {
        QEventData qEventData = QEventData.eventData;
        BooleanExpression pred = null;
        Expression<?> field;
        Object value;
        EventData operands = dataFilter.getOperands();
        switch (operands.getEventDataType()) {
        case NUMBER:
            field = qEventData.numberValue;
            value = operands.getNumberValue();
            break;
        case STRING:
            field = qEventData.stringValue;
            value = operands.getTextValue();
            break;
        case TEXT:
            field = qEventData.textValue;
            value = operands.getTextValue();
            break;
        case TIMESTAMP:
            field = qEventData.timestampValue;
            value = Timestamp.from(operands.getTimestampValue());
            break;
        case BINARY:
        default:
            throw new IllegalArgumentException("unsupported operator: " + dataFilter.getOperator());
        }
        pred = Expressions.predicate(operatorMapping.get(dataFilter.getOperator()), field, Expressions.constant(value));
        return qEventData.eventDataName.ne(operands.getName()).or(pred);
    }

    private void buildQuery() {
        query = new SQLQuery(connection, configuration);
        buildFromClause();
        joinAppAndEventType();
        buildEventDataSubquery();
        addOrderBy();
    }

    private void joinAppAndEventType() {
        query = query.leftJoin(qEventType).on(evtSubqueryAlias.eventTypeId.eq(qEventType.eventTypeId));
        query = query.leftJoin(qApplication).on(qEventType.applicationId.eq(qApplication.applicationId));
    }

    public List<EventUi> loadEvents() {
        buildQuery();
        List<Tuple> result = query.list(qApplication.applicationName,
                qEventType.name,
                evtSubqueryAlias.eventId,
                evtSubqueryAlias.saveTimestamp,
                evtDataSubqueryAlias.eventDataName,
                evtDataSubqueryAlias.eventDataType,
                evtDataSubqueryAlias.numberValue,
                evtDataSubqueryAlias.stringValue,
                evtDataSubqueryAlias.textValue,
                evtDataSubqueryAlias.timestampValue,
                evtDataSubqueryAlias.binaryValue);
        return new MultipleEventQueryResultMapper(result, evtSubqueryAlias, evtDataSubqueryAlias).mapToEvents();
    }

}
