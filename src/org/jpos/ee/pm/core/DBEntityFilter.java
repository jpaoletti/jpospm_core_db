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
package org.jpos.ee.pm.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.hibernate.Criteria;

import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Junction;
import org.hibernate.criterion.Restrictions;

public class DBEntityFilter extends EntityFilter {

    private List<Criterion> filters;
    private Entity entity;

    public DBEntityFilter() {
        super();
        this.setFilters(new ArrayList<Criterion>());
    }

    @Override
    public void clear() {
        filters.clear();
    }

    @Override
    public void process(Entity entity) {
        this.entity = entity;
    }

    protected Criterion getCompareCriterion(String fieldid, String fieldProperty, List<Object> values) {
        Object value_0 = values.get(0);
        switch (getFilterOperation(fieldid)) {
            case LIKE:
                if (value_0 instanceof String) {
                    return Restrictions.ilike(fieldProperty, "%" + value_0 + "%");
                } else {
                    return Restrictions.eq(fieldProperty, value_0);
                }
            case BETWEEN:
                return Restrictions.between(fieldProperty, value_0, values.get(1));
            case GE:
                return Restrictions.ge(fieldProperty, value_0);
            case GT:
                return Restrictions.gt(fieldProperty, value_0);
            case LE:
                return Restrictions.le(fieldProperty, value_0);
            case LT:
                return Restrictions.lt(fieldProperty, value_0);
            case NE:
                return Restrictions.not(Restrictions.eq(fieldProperty, value_0));
            default:
                return Restrictions.eq(fieldProperty, value_0);
        }
    }

    public final void setFilters(List<Criterion> filters) {
        this.filters = filters;
    }

    public Criteria applyFilters(Criteria criteria, List<String> aliases) throws PMException {
        Criteria tmpCriteria = criteria;
        //First we create all the needed aliases
        for (Entry<String, List<Object>> entry : getFilterValues().entrySet()) {
            final Field field = entity.getFieldById(entry.getKey());
            if (field == null) {
                throw new PMException("Undefined field " + entry.getKey());
            }
            final List<Object> values = entry.getValue();
            if (values.get(0) != null) {
                final String[] splitorder = field.getProperty().split("[.]");
                for (int i = 0; i < splitorder.length - 1; i++) {
                    final String s = splitorder[i];
                    if (!aliases.contains(s)) {
                        tmpCriteria = tmpCriteria.createAlias(s, s);
                        aliases.add(s);
                    }
                }
            }
        }
        //Then we add the restrictions
        Junction c = null;
        switch (getBehavior()) {
            case OR:
                c = Restrictions.disjunction();
                break;
            default:
                c = Restrictions.conjunction();
        }
        for (Entry<String, List<Object>> entry : getFilterValues().entrySet()) {
            final Field field = entity.getFieldById(entry.getKey());
            final List<Object> values = entry.getValue();
            if (values.get(0) != null) {
                c.add(getCompareCriterion(field.getId(), field.getProperty(), values));
            }
        }
        tmpCriteria.add(c);
        return tmpCriteria;
    }
}
