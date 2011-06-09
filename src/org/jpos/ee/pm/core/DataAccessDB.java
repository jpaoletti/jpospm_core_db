/*
 * jPOS Project [http://jpos.org]
 * Copyright (C) 2000-2010 Alejandro P. Revilla
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
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.jpos.ee.Constants;
import org.jpos.ee.DB;

public class DataAccessDB implements DataAccess, Constants {

    @Override
    public Object getItem(PMContext ctx, String property, String value) throws PMException {
        try {
            DB db = getDb(ctx);
            Criteria c = db.session().createCriteria(Class.forName(getEntity(ctx).getClazz()));
            c.setMaxResults(1);
            c.add(Restrictions.sqlRestriction(property + "=" + value));
            return c.uniqueResult();
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    protected DB getDb(PMContext ctx) {
        return (DB) ctx.get(DBPersistenceManager.PM_DB);
    }

    private Entity getEntity(PMContext ctx) throws PMException {
        if (ctx.get(PM_ENTITY) == null) {
            return ctx.getEntity();
        } else {
            return (Entity) ctx.get(PM_ENTITY);
        }
    }

    @Override
    public List<?> list(PMContext ctx, EntityFilter filter, Integer from, Integer count) throws PMException {
        //We use the filter only if the entity we use is the container one.
        Criteria list = createCriteria(ctx, getEntity(ctx), filter);
        if (count != null) {
            list.setMaxResults(count);
        }
        if (from != null) {
            list.setFirstResult(from);
        }
        return list.list();
    }

    @Override
    public void delete(PMContext ctx, Object object) throws PMException {
        DB db = getDb(ctx);
        db.session().delete(object);
    }

    @Override
    public void update(PMContext ctx, Object object) throws PMException {
        DB db = getDb(ctx);
        db.session().update(object);
    }

    @Override
    public void add(PMContext ctx, Object object) throws PMException {
        try {
            DB db = getDb(ctx);
            db.session().save(object);
        } catch (org.hibernate.exception.ConstraintViolationException e) {
            throw new PMException("constraint.violation.exception");
        }
    }

    @Override
    public Long count(PMContext ctx) throws PMException {
        EntityFilter filter = ctx.getEntityContainer().getFilter();
        Criteria count = createCriteria(ctx, getEntity(ctx), filter);
        count.setProjection(Projections.rowCount());
        count.setMaxResults(1);
        return (Long) count.uniqueResult();
    }

    protected Criteria createCriteria(PMContext ctx, Entity entity, EntityFilter filter) throws PMException {
        final List<String> aliases = new ArrayList<String>();
        Criteria c;
        DB db = getDb(ctx);
        try {
            c = db.session().createCriteria(Class.forName(entity.getClazz()));
        } catch (ClassNotFoundException e) {
            ctx.getErrors().add(new PMMessage(ENTITY, "class.not.found"));
            throw new PMException();
        }

        String order = null;
        try {
            order = (ctx.getString(PM_LIST_ORDER) != null) ? entity.getFieldById(ctx.getString(PM_LIST_ORDER)).getProperty() : null;
        } catch (Exception e) {
        }
        boolean asc = (ctx.get(PM_LIST_ASC) == null) ? true : (Boolean) ctx.get(PM_LIST_ASC);
        if (order != null) {
            final String[] splitorder = order.split("[.]");
            for (int i = 0; i < splitorder.length - 1; i++) {
                final String so = splitorder[i];
                if (!aliases.contains(so)) {
                    c = c.createAlias(so, so);
                    aliases.add(so);
                }
            }
            if (asc) {
                c.addOrder(Order.asc(order));
            } else {
                c.addOrder(Order.desc(order));
            }
        }
        if (entity.getListfilter() != null) {
            final Object lf = entity.getListfilter().getListFilter(ctx);
            if (lf instanceof Criterion) {
                c.add((Criterion) lf);
            } else if (lf instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) lf;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (entry.getValue() instanceof String) {
                        final String alias = entry.getKey();
                        if (!aliases.contains(alias)) {
                            c = c.createAlias(alias, (String) entry.getValue());
                            aliases.add(alias);
                        }
                    } else if (entry.getValue() instanceof Criterion) {
                        c.add((Criterion) entry.getValue());
                    }
                }
            }
        }

        if (filter != null) {
            c = ((DBEntityFilter) filter).applyFilters(c, aliases);
        }
        //Weak entities must filter the parent
        if (entity.isWeak()) {
            if (ctx.getEntityContainer(true) != null && ctx.getEntityContainer().getOwner() != null) {
                if (ctx.getEntityContainer().getOwner().getId().equals(entity.getOwner().getEntityId())) {
                    if (ctx.getEntityContainer().getOwner().getSelected() != null) {
                        final Object instance = ctx.getEntityContainer().getOwner().getSelected().getInstance();
                        final String localProperty = entity.getOwner().getLocalProperty();
                        c.add(Restrictions.eq(localProperty, instance));
                    }
                }
            }
        }

        return c;
    }

    @Override
    public Object refresh(PMContext ctx, Object o) throws PMException {
        DB db = getDb(ctx);
        final Object merged = db.session().merge(o);
        db.session().refresh(merged);
        return merged;
    }

    @Override
    public EntityFilter createFilter(PMContext ctx) throws PMException {
        return new DBEntityFilter();
    }
}
