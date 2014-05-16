/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

/**
 * A reference to a procedure parameter
 */
public class ProcParameterRef extends CatalogType {

    int m_index;

    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        super.setBaseValues(catalog, parent, path, name);
        m_fields.put("index", m_index);
        m_fields.put("parameter", null);
    }

    public void update() {
        m_index = (Integer) m_fields.get("index");
    }

    /** GETTER: The index within the set */
    public int getIndex() {
        return m_index;
    }

    /** GETTER: The parameter being referenced */
    public ProcParameter getParameter() {
        Object o = getField("parameter");
        if (o instanceof UnresolvedInfo) {
            UnresolvedInfo ui = (UnresolvedInfo) o;
            ProcParameter retval = (ProcParameter) m_catalog.getItemForRef(ui.path);
            assert(retval != null);
            m_fields.put("parameter", retval);
            return retval;
        }
        return (ProcParameter) o;
    }

    /** SETTER: The index within the set */
    public void setIndex(int value) {
        m_index = value; m_fields.put("index", value);
    }

    /** SETTER: The parameter being referenced */
    public void setParameter(ProcParameter value) {
        m_fields.put("parameter", value);
    }

}
