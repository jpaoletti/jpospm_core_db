/*
 * jPOS Project [http://jpos.org]
 * Copyright (C) 2000-2011 Alejandro P. Revilla
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpos.ee.pm.core.monitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hibernate.SQLQuery;
import org.jpos.ee.DB;

public class SQLMonitorSource extends MonitorSource {

    private String query;
    private String lastLineQuery;
    private Integer idColumn;

    @Override
    public void init() {
        setQuery(getConfig("query"));
        setLastLineQuery(getConfig("last-line-query"));
        setIdColumn(Integer.parseInt(getConfig("id-column", "0")));
    }

    @Override
    public List<MonitorLine> getLinesFrom(Object actual) throws Exception {
        final List<MonitorLine> result = new ArrayList<MonitorLine>();
        final DB db = new DB();
        db.open();
        try {
            String sql = getLastLineQuery().trim();
            sql = sql.replaceAll("\\$actual", (actual == null) ? "" : actual.toString());
            final SQLQuery c = db.session().createSQLQuery(sql);
            final List<?> l = c.list();
            for (Iterator<?> iterator = l.iterator(); iterator.hasNext();) {
                final Object item = iterator.next();
                final MonitorLine line = new MonitorLine();

                if (item instanceof Object[]) {
                    final Object[] objects = (Object[]) item;
                    line.setId(objects[getIdColumn()]);
                    line.setValue(objects);
                } else {
                    line.setId(item);
                    final Object[] objects = {item};
                    line.setValue(objects);
                }

                result.add(line);
            }
        } finally {
            db.close();
        }
        return result;
    }

    @Override
    public MonitorLine getLastLine() throws Exception {
        final MonitorLine result = new MonitorLine();
        final DB db = new DB();
        db.open();
        try {
            final SQLQuery c = db.session().createSQLQuery(getLastLineQuery().trim());
            c.setMaxResults(1);
            final Object item = c.uniqueResult();
            if (item instanceof Object[]) {
                final Object[] objects = (Object[]) item;
                result.setId(objects[getIdColumn()]);
                result.setValue(objects);
            } else {
                result.setId(item);
                final Object[] objects = {item};
                result.setValue(objects);
            }
        } finally {
            db.close();
        }
        return result;
    }

    /**
     * @param query the query to set
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * @return the query
     */
    public String getQuery() {
        return query;
    }

    /**
     * @param lastLineQuery the lastLineQuery to set
     */
    public void setLastLineQuery(String lastLineQuery) {
        this.lastLineQuery = lastLineQuery;
    }

    /**
     * @return the lastLineQuery
     */
    public String getLastLineQuery() {
        return lastLineQuery;
    }

    /**
     * @param idColumn the idColumn to set
     */
    public void setIdColumn(Integer idColumn) {
        this.idColumn = idColumn;
    }

    /**
     * @return the idColumn
     */
    public Integer getIdColumn() {
        return idColumn;
    }
}
