package org.jivesoftware.openfire.group;

import org.apache.commons.lang3.StringUtils;
import org.jivesoftware.database.DbConnectionManager;
import org.jivesoftware.openfire.event.GroupEventDispatcher;
import org.jivesoftware.util.JiveGlobals;
import org.jivesoftware.util.PersistableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * An implementation of the {@link PersistableMap} to manage group properties with
 * an External Database. It's supposed to be used when {@link JDBCGroupProvider} is
 * used as the Group provider. In this case the {@link JDBCGroupProvider} will internally
 * rely on this class to manage the properties of the groups. Without any parameter set,
 * this class will behave like a classical HashMap but if you set the needed properties
 * as described below, then this class will behave like {@link DefaultGroupPropertyMap}
 * and will try to reflect any change to the Group properties into the backing database.
 * The properties to set are (with an SQL example):
 * <ul>
 *     <li>{@code jdbcGroupPropertyProvider.deletePropertySQL = DELETE FROM ofGroupProp WHERE groupName=? AND name=?}</li>
 *     <li>{@code jdbcGroupPropertyProvider.deleteAllPropertiesSQL = DELETE FROM ofGroupProp WHERE groupName=?}</li>
 *     <li>{@code jdbcGroupPropertyProvider.updatePropertySQL = UPDATE ofGroupProp SET propValue=? WHERE name=? AND groupName=?}</li>
 *     <li>{@code jdbcGroupPropertyProvider.insertPropertySQL = INSERT INTO ofGroupProp (groupName, name, propValue) VALUES (?, ?, ?)}</li>
 * </ul>
 *
 * This class will use the connectionProvider defined for the class {@link JDBCGroupProvider}.
 *
 * @param <K> Property key
 * @param <V> Property value
 */

public class JDBCGroupPropertyMap<K,V> extends PersistableMap<K,V> {

    private static final long serialVersionUID = -2418089922308584164L;
    private static final Logger logger = LoggerFactory.getLogger(JDBCGroupPropertyMap.class);


    private static final String DEL_PROP = "jdbcGroupPropertyProvider.deletePropertySQL";
    private static final String DEL_ALL_PROP = "jdbcGroupPropertyProvider.deleteAllPropertiesSQL";
    private static final String UPDATE_PROP = "jdbcGroupPropertyProvider.updatePropertySQL";
    private static final String INSERT_PROP = "jdbcGroupPropertyProvider.insertPropertySQL";

    private String deletePropertySQL;
    private String deleteAllPropertiesSQL;
    private String updatePropertySQL;
    private String insertPropertySQL;

    private Group group;

    private String connectionString;

    /**
     * Group properties map constructor; requires associated {@link Group} instance
     * @param group The group that owns these properties
     */
    public JDBCGroupPropertyMap(Group group) {
        this.group = group;

        // Migrate properties if they were in openfire.xml
        JiveGlobals.migrateProperty(DEL_PROP);
        JiveGlobals.migrateProperty(DEL_ALL_PROP);
        JiveGlobals.migrateProperty(UPDATE_PROP);
        JiveGlobals.migrateProperty(INSERT_PROP);

        // Get the values
        deletePropertySQL = JiveGlobals.getProperty(DEL_PROP);
        deleteAllPropertiesSQL = JiveGlobals.getProperty(DEL_ALL_PROP);
        updatePropertySQL = JiveGlobals.getProperty(UPDATE_PROP);
        insertPropertySQL = JiveGlobals.getProperty(INSERT_PROP);
    }

    public JDBCGroupPropertyMap(Group group, String conString) {
        this.connectionString = conString;
        new JDBCGroupPropertyMap<>(group);
    }

    private Connection getConnection() throws SQLException {
        if (StringUtils.isNotBlank(connectionString)) {
            return DriverManager.getConnection(connectionString);
        }
        return DbConnectionManager.getConnection();
    }

    /**
     * Custom method to put properties into the map, optionally without
     * triggering persistence. This is used when the map is being
     * initially loaded from the database.
     *
     * @param key The property name
     * @param value The property value
     * @param persist True if the changes should be persisted to the database
     * @return The original value or null if the property did not exist
     */
    @Override
    public V put(K key, V value, boolean persist) {
        V originalValue = super.put(key, value);
        // we only support persistence for <String, String>
        if (persist && key instanceof String && value instanceof String) {
            if (logger.isDebugEnabled())
                logger.debug("Persisting group property [" + key + "]: " + value);
            if (originalValue instanceof String) { // existing property
                updateProperty((String)key, (String)value, (String)originalValue);
            } else {
                insertProperty((String)key, (String)value);
            }
        }
        return originalValue;
    }

    @Override
    public V put(K key, V value) {
        if (value == null) { // treat null value as "remove"
            return remove(key);
        } else {
            return put(key, value, true);
        }
    }

    @Override
    public V remove(Object key) {
        V result = super.remove(key);
        if (key instanceof String) {
            deleteProperty((String)key);
        }
        return result;
    }

    @Override
    public void clear() {
        super.clear();
        deleteAllProperties();
    }

    @Override
    public Set<K> keySet() {
        // custom class needed here to handle key.remove()
        return new PersistenceAwareKeySet<K>(super.keySet());
    }

    @Override
    public Collection<V> values() {
        // custom class needed here to suppress value.remove()
        return Collections.unmodifiableCollection(super.values());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        // custom class needed here to handle entrySet mutators
        return new PersistenceAwareEntrySet<Entry<K,V>>(super.entrySet());
    }

    /**
     * Persistence-aware {@link Set} for group property keys. This class returns
     * a custom iterator that can handle property removal.
     */
    private class PersistenceAwareKeySet<E> extends AbstractSet<K> {

        private Set<K> delegate;

        /**
         * Sole constructor; requires wrapped {@link Set} for delegation
         * @param delegate A collection of keys from the map
         */
        public PersistenceAwareKeySet(Set<K> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Iterator<K> iterator() {
            return new KeyIterator<E>(delegate.iterator());
        }

        @Override
        public int size() {
            return delegate.size();
        }
    }

    /**
     * This iterator updates the database when a property key is removed.
     */
    private class KeyIterator<E> implements Iterator<K> {

        private Iterator<K> delegate;
        private K current;

        /**
         * Sole constructor; requires wrapped {@link Iterator} for delegation
         * @param delegate An iterator for all the keys from the map
         */
        public KeyIterator(Iterator<K> delegate) {
            this.delegate = delegate;
        }

        /**
         * Delegated to corresponding method in the backing {@link Iterator}
         */
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        /**
         * Delegated to corresponding method in the backing {@link Iterator}
         */
        @Override
        public K next() {
            current = delegate.next();
            return current;
        }

        /**
         * Removes the property corresponding to the current key from
         * the underlying map. Also applies update to the database.
         */
        @Override
        public void remove() {
            delegate.remove();
            if (current instanceof String) {
                deleteProperty((String)current);
            }
            current = null;
        }
    }

    /**
     * Persistence-aware {@link Set} for group properties (as {@link Entry})
     */
    private class PersistenceAwareEntrySet<E> implements Set<Entry<K, V>> {

        private Set<Entry<K, V>> delegate;

        /**
         * Sole constructor; requires wrapped {@link Set} for delegation
         * @param delegate A collection of entries ({@link Entry}) from the map
         */
        public PersistenceAwareEntrySet(Set<Entry<K, V>> delegate) {
            this.delegate = delegate;
        }

        /**
         * Returns a custom iterator for the entries in the backing map
         */
        @Override
        public Iterator<Entry<K, V>> iterator() {
            return new EntryIterator<Entry<K,V>>(delegate.iterator());
        }

        /**
         * Removes the given key from the backing map, and applies the
         * corresponding update to the database.
         *
         * @param o A {@link Entry} within this set
         * @return True if the set contained the given key
         */
        @Override
        public boolean remove(Object o) {
            boolean propertyExists = delegate.remove(o);
            if (propertyExists) {
                deleteProperty((String)((Entry<K,V>)o).getKey());
            }
            return propertyExists;
        }

        /**
         * Removes all the elements in the set, and applies the
         * corresponding update to the database.
         */
        @Override
        public void clear() {
            delegate.clear();
            deleteAllProperties();
        }

        // these methods are problematic (and not really necessary),
        // so they are not implemented

        /**
         * @throws UnsupportedOperationException
         */
        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        /**
         * @throws UnsupportedOperationException
         */
        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        // per docs for {@link Map.entrySet}, these methods are not supported

        /**
         * @throws UnsupportedOperationException
         */
        @Override
        public boolean add(Entry<K, V> o) {
            return delegate.add(o);
        }

        /**
         * @throws UnsupportedOperationException
         */
        @Override
        public boolean addAll(Collection<? extends Entry<K, V>> c) {
            return delegate.addAll(c);
        }

        // remaining {@link Set} methods can be delegated safely

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        @Override
        public int size() {
            return delegate.size();
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        @Override
        public boolean contains(Object o) {
            return delegate.contains(o);
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        @Override
        public Object[] toArray() {
            return delegate.toArray();
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        @Override
        public <T> T[] toArray(T[] a) {
            return delegate.toArray(a);
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        @Override
        public boolean containsAll(Collection<?> c) {
            return delegate.containsAll(c);
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        public boolean equals(Object o) {
            return delegate.equals(o);
        }

        /**
         * Delegated to corresponding method in the backing {@link Set}
         */
        public int hashCode() {
            return delegate.hashCode();
        }
    }

    /**
     * Remove group property from the database when the {@code Iterator.remove}
     * method is invoked via the {@code Map.entrySet} set
     */
    private class EntryIterator<E> implements Iterator<Entry<K, V>> {

        private Iterator<Entry<K,V>> delegate;
        private EntryWrapper<E> current;

        /**
         * Sole constructor; requires wrapped {@link Iterator} for delegation
         * @param delegate An iterator for all the keys from the map
         */
        public EntryIterator(Iterator<Entry<K,V>> delegate) {
            this.delegate = delegate;
        }
        /**
         * Delegated to corresponding method in the backing {@link Iterator}
         */
        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        /**
         * Delegated to corresponding method in the backing {@link Iterator}
         */
        @Override
        public Entry<K,V> next() {
            current = new EntryWrapper<>(delegate.next());
            return current;
        }

        /**
         * Removes the property corresponding to the current key from
         * the underlying map. Also applies update to the database.
         */
        @Override
        public void remove() {
            delegate.remove();
            K key = current.getKey();
            if (key instanceof String) {
                deleteProperty((String)key);
            }
            current = null;
        }
    }

    /**
     * Update the database when a group property is updated
     */
    private class EntryWrapper<E> implements Entry<K,V> {
        private Entry<K,V> delegate;

        /**
         * Sole constructor; requires wrapped {@link Entry} for delegation
         * @param delegate The corresponding entry from the map
         */
        public EntryWrapper(Entry<K,V> delegate) {
            this.delegate = delegate;
        }

        /**
         * Delegated to corresponding method in the backing {@link Entry}
         */
        @Override
        public K getKey() {
            return delegate.getKey();
        }

        /**
         * Delegated to corresponding method in the backing {@link Entry}
         */
        @Override
        public V getValue() {
            return delegate.getValue();
        }

        /**
         * Set the value of the property corresponding to this entry. This
         * method also updates the database as needed depending on the new
         * property value. A null value will cause the property to be deleted
         * from the database.
         *
         * @param value The new property value
         * @return The old value of the corresponding property
         */
        @Override
        public V setValue(V value) {
            V oldValue = delegate.setValue(value);
            K key = delegate.getKey();
            if (key instanceof String) {
                if (value instanceof String) {
                    if (oldValue == null) {
                        insertProperty((String) key, (String) value);
                    } else if (!value.equals(oldValue)) {
                        updateProperty((String)key,(String)value, (String)oldValue);
                    }
                } else {
                    deleteProperty((String)key);
                }
            }
            return oldValue;
        }
    }

    /**
     * Persist a new group property to the database for the current group
     *
     * @param key Property name
     * @param value Property value
     */
    private synchronized void insertProperty(String key, String value) {
        if (StringUtils.isNotBlank(insertPropertySQL)) {
            Connection con = null;
            PreparedStatement pstmt = null;
            try {
                con = getConnection();
                pstmt = con.prepareStatement(insertPropertySQL);
                pstmt.setString(1, group.getName());
                pstmt.setString(2, key);
                pstmt.setString(3, value);
                pstmt.executeUpdate();
            }
            catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
            finally {
                DbConnectionManager.closeConnection(pstmt, con);
            }
        }
        Map<String, Object> event = new HashMap<>();
        event.put("propertyKey", key);
        event.put("type", "propertyAdded");
        GroupEventDispatcher.dispatchEvent(group,
            GroupEventDispatcher.EventType.group_modified, event);
    }

    /**
     * Update the value of an existing group property for the current group
     *
     * @param key Property name
     * @param value Property value
     * @param originalValue Original property value
     */
    private synchronized void updateProperty(String key, String value, String originalValue) {
        if (StringUtils.isNotBlank(updatePropertySQL)) {
            Connection con = null;
            PreparedStatement pstmt = null;
            try {
                con = getConnection();
                pstmt = con.prepareStatement(updatePropertySQL);
                pstmt.setString(1, value);
                pstmt.setString(2, key);
                pstmt.setString(3, group.getName());
                pstmt.executeUpdate();
            }
            catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
            finally {
                DbConnectionManager.closeConnection(pstmt, con);
            }
        }
        Map<String, Object> event = new HashMap<>();
        event.put("propertyKey", key);
        event.put("type", "propertyModified");
        event.put("originalValue", originalValue);
        GroupEventDispatcher.dispatchEvent(group,
            GroupEventDispatcher.EventType.group_modified, event);
    }

    /**
     * Delete a group property from the database for the current group
     *
     * @param key Property name
     */
    private synchronized void deleteProperty(String key) {
        if (StringUtils.isNotBlank(deletePropertySQL)) {
            Connection con = null;
            PreparedStatement pstmt = null;
            try {
                con = getConnection();
                pstmt = con.prepareStatement(deletePropertySQL);
                pstmt.setString(1, group.getName());
                pstmt.setString(2, key);
                pstmt.executeUpdate();
            }
            catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
            finally {
                DbConnectionManager.closeConnection(pstmt, con);
            }
        }
        Map<String, Object> event = new HashMap<>();
        event.put("type", "propertyDeleted");
        event.put("propertyKey", key);
        GroupEventDispatcher.dispatchEvent(group,
            GroupEventDispatcher.EventType.group_modified, event);
    }

    /**
     * Delete all properties from the database for the current group
     */
    private synchronized void deleteAllProperties() {
        if (StringUtils.isNotBlank(deleteAllPropertiesSQL)) {
            Connection con = null;
            PreparedStatement pstmt = null;
            try {
                con = getConnection();
                pstmt = con.prepareStatement(deleteAllPropertiesSQL);
                pstmt.setString(1, group.getName());
                pstmt.executeUpdate();
            }
            catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
            finally {
                DbConnectionManager.closeConnection(pstmt, con);
            }
        }
        Map<String, Object> event = new HashMap<>();
        event.put("type", "propertyDeleted");
        event.put("propertyKey", "*");
        GroupEventDispatcher.dispatchEvent(group,
            GroupEventDispatcher.EventType.group_modified, event);
    }
}
