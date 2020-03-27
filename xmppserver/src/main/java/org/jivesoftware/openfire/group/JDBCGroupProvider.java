/*
 * Copyright (C) 2005-2008 Jive Software. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jivesoftware.openfire.group;

import org.apache.commons.lang3.StringUtils;
import org.jivesoftware.database.DbConnectionManager;
import org.jivesoftware.database.ExternalDbConnectionManager;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.util.JiveGlobals;
import org.jivesoftware.util.PersistableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.JID;

import java.sql.*;
import java.util.*;

/**
 * The JDBC group provider allows you to use an external database to define the make up of groups.
 * It is best used with the JDBCAuthProvider to provide integration between your external system and
 * Openfire.  All data is treated as read-only so any set operations will result in an exception.
 *
 * To enable this provider, set the following in the system properties:
 *
 * <ul>
 * <li>{@code provider.group.className = org.jivesoftware.openfire.group.JDBCGroupProvider}</li>
 * </ul>
 *
 * Then you need to set the <b>driver properties</b>. Check the documentation of the class {@link ExternalDbConnectionManager}
 * to see what properties you <b>must</b> set. <br />
 * <br />
 * The properties for the SQL statements are:
 * <ul>
 * <li>{@code jdbcGroupProvider.groupCountSQL = SELECT count(*) FROM myGroups}</li>
 * <li>{@code jdbcGroupProvider.allGroupsSQL = SELECT groupName FROM myGroups}</li>
 * <li>{@code jdbcGroupProvider.userGroupsSQL = SELECT groupName FORM myGroupUsers WHERE username=?}</li>
 * <li>{@code jdbcGroupProvider.descriptionSQL = SELECT groupDescription FROM myGroups WHERE groupName=?}</li>
 * <li>{@code jdbcGroupProvider.loadMembersSQL = SELECT username FORM myGroupUsers WHERE groupName=? AND isAdmin='N'}</li>
 * <li>{@code jdbcGroupProvider.loadAdminsSQL = SELECT username FORM myGroupUsers WHERE groupName=? AND isAdmin='Y'}</li>
 * </ul>
 *
 * In order to use the configured JDBC connection provider do not use a JDBC
 * connection string, set the following property
 *
 * <ul>
 * <li>{@code jdbcGroupProvider.useConnectionProvider = true}</li>
 * </ul>
 *
 * You can also define Group properties to be used from the external database. Below are the default
 * requests as an example:
 * <ul>
 *     <li>{@code jdbcGroupPropertyProvider.grouplistContainersSQL = SELECT groupName FROM ofGroupProp WHERE name='sharedRoster.groupList' AND propValue LIKE ?}</li>
 *     <li>{@code jdbcGroupPropertyProvider.publicGroupsSQL = SELECT groupName FROM ofGroupProp WHERE name='sharedRoster.showInRoster' AND propValue='everybody'}</li>
 *     <li>{@code jdbcGroupPropertyProvider.groupsForPropSQL = SELECT groupName FROM ofGroupProp WHERE name=? AND propValue=?}</li>
 *     <li>{@code jdbcGroupPropertyProvider.loadSharedGroupsSQL = SELECT groupName FROM ofGroupProp WHERE name='sharedRoster.showInRoster' AND propValue IS NOT NULL AND propValue <> 'nobody'}</li>
 *     <li>{@code jdbcGroupPropertyProvider.loadPropertiesSQL = SELECT name, propValue FROM ofGroupProp WHERE groupName=?}</li>
 *     <li>{@code jdbcGroupPropertyProvider.deletePropertySQL = "DELETE FROM ofGroupProp WHERE groupName=? AND name=?"}</li>
 *     <li>{@code jdbcGroupPropertyProvider.deleteAllPropertiesSQL = "DELETE FROM ofGroupProp WHERE groupName=?"}</li>
 *     <li>{@code jdbcGroupPropertyProvider.updatePropertySQL = "UPDATE ofGroupProp SET propValue=? WHERE name=? AND groupName=?"}</li>
 *     <li>{@code jdbcGroupPropertyProvider.insertPropertySQL = "INSERT INTO ofGroupProp (groupName, name, propValue) VALUES (?, ?, ?)"}</li>
 * </ul>
 *
 * If you want to manually force Group properties to be read-only, set the following propertie to true:
 * <ul>
 *     <li>{@code jdbcGroupPropertyProvider.groupPropertyReadonly = true}</li></li>
 * </ul>
 *
 * @author David Snopek
 */
public class JDBCGroupProvider extends AbstractGroupProvider {

    private static final Logger Log = LoggerFactory.getLogger(JDBCGroupProvider.class);

    private String groupCountSQL;
    private String descriptionSQL;
    private String allGroupsSQL;
    private String userGroupsSQL;
    private String loadMembersSQL;
    private String loadAdminsSQL;

    private boolean useConnectionProvider;

    // Keys to use for the SQL properties to manipulate Group Properties
    private static final String GRPLIST_CONTAINER = "jdbcGroupPropertyProvider.grouplistContainersSQL";
    private static final String PUB_GROUP = "jdbcGroupPropertyProvider.publicGroupsSQL";
    private static final String GROUPS_FOR_PROP = "jdbcGroupPropertyProvider.groupsForPropSQL";
    private static final String LOAD_SHARED_GROUPS = "jdbcGroupPropertyProvider.loadSharedGroupsSQL";
    private static final String LOAD_PROPERTIES = "jdbcGroupPropertyProvider.loadPropertiesSQL";
    private static final String DEL_PROP = "jdbcGroupPropertyProvider.deletePropertySQL";
    private static final String DEL_ALL_PROP = "jdbcGroupPropertyProvider.deleteAllPropertiesSQL";
    private static final String UPDATE_PROP = "jdbcGroupPropertyProvider.updatePropertySQL";
    private static final String INSERT_PROP = "jdbcGroupPropertyProvider.insertPropertySQL";
    private static final String GRO_PROP_RO = "jdbcGroupPropertyProvider.groupPropertyReadonly";
    private String grouplistContainersSQL;
    private String publicGroupsSQL;
    private String groupsForPropSQL;
    private String loadSharedGroupsSQL;
    private String loadPropertiesSQL;
    private String deletePropertySQL;
    private String deleteAllPropertiesSQL;
    private String updatePropertySQL;
    private String insertPropertySQL;
    private boolean groupPropReadonly;

    private XMPPServer server = XMPPServer.getInstance();

    // Connections to the external database
    private ExternalDbConnectionManager exDb;

    /**
     * Constructor of the JDBCGroupProvider class.
     */
    public JDBCGroupProvider() {
        // Convert XML based provider setup to Database based
        JiveGlobals.migrateProperty("jdbcGroupProvider.groupCountSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.allGroupsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.userGroupsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.descriptionSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.loadMembersSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.loadAdminsSQL");

        JiveGlobals.migrateProperty(GRPLIST_CONTAINER);
        JiveGlobals.migrateProperty(PUB_GROUP);
        JiveGlobals.migrateProperty(GROUPS_FOR_PROP);
        JiveGlobals.migrateProperty(LOAD_SHARED_GROUPS);
        JiveGlobals.migrateProperty(LOAD_PROPERTIES);
        JiveGlobals.migrateProperty(DEL_PROP);
        JiveGlobals.migrateProperty(DEL_ALL_PROP);
        JiveGlobals.migrateProperty(UPDATE_PROP);
        JiveGlobals.migrateProperty(INSERT_PROP);
        JiveGlobals.migrateProperty(GRO_PROP_RO);

        useConnectionProvider = JiveGlobals.getBooleanProperty("jdbcGroupProvider.useConnectionProvider");

        if (!useConnectionProvider) {
            exDb = ExternalDbConnectionManager.getInstance();
        }

        // Load SQL statements
        groupCountSQL = JiveGlobals.getProperty("jdbcGroupProvider.groupCountSQL");
        allGroupsSQL = JiveGlobals.getProperty("jdbcGroupProvider.allGroupsSQL");
        userGroupsSQL = JiveGlobals.getProperty("jdbcGroupProvider.userGroupsSQL");
        descriptionSQL = JiveGlobals.getProperty("jdbcGroupProvider.descriptionSQL");
        loadMembersSQL = JiveGlobals.getProperty("jdbcGroupProvider.loadMembersSQL");
        loadAdminsSQL = JiveGlobals.getProperty("jdbcGroupProvider.loadAdminsSQL");

        // If any of those is blank, then the methods implementation will rely on the default method from
        // the super-class
        grouplistContainersSQL = JiveGlobals.getProperty(GRPLIST_CONTAINER);
        publicGroupsSQL = JiveGlobals.getProperty(PUB_GROUP);
        groupsForPropSQL = JiveGlobals.getProperty(GROUPS_FOR_PROP);
        loadSharedGroupsSQL = JiveGlobals.getProperty(LOAD_SHARED_GROUPS);
        loadPropertiesSQL = JiveGlobals.getProperty(LOAD_PROPERTIES);

        // If any of those is blank, then we have to set it to its default value. See class DefaultGroupPropertyMap.java
        deletePropertySQL = JiveGlobals.getProperty(DEL_PROP,"DELETE FROM ofGroupProp WHERE groupName=? AND name=?");
        deleteAllPropertiesSQL = JiveGlobals.getProperty(DEL_ALL_PROP,"DELETE FROM ofGroupProp WHERE groupName=?");
        updatePropertySQL = JiveGlobals.getProperty(UPDATE_PROP,"UPDATE ofGroupProp SET propValue=? WHERE name=? AND groupName=?");
        insertPropertySQL = JiveGlobals.getProperty(INSERT_PROP,"INSERT INTO ofGroupProp (groupName, name, propValue) VALUES (?, ?, ?)");

        // Check if the group properties has been manually set to read-only
        groupPropReadonly = JiveGlobals.getBooleanProperty(GRO_PROP_RO, false);
    }

    private Connection getConnection() throws SQLException {
        if (useConnectionProvider) {
            return DbConnectionManager.getConnection();
        } else {
            return exDb.getConnection();
        }
    }

    /**
     * In this implementation, the group properties are expected to be writable to the backend unless the server property
     * "jdbcGroupPropertyProvider.groupPropertyReadonly" has been explicitly set to 'true'.
     * @return return false or true if "jdbcGroupPropertyProvider.groupPropertyReadonly" is true
     */
    @Override
    public boolean arePropertiesReadOnly() {
        return groupPropReadonly;
    }

    @Override
    public Group getGroup(String name) throws GroupNotFoundException {
        String description = null;

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(descriptionSQL);
            pstmt.setString(1, name);
            rs = pstmt.executeQuery();
            if (!rs.next()) {
                throw new GroupNotFoundException("Group with name "
                    + name + " not found.");
            }
            description = rs.getString(1);
        }
        catch (SQLException e) {
            Log.error(e.getMessage(), e);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        Collection<JID> members = getMembers(name, false);
        Collection<JID> administrators = getMembers(name, true);
        return new Group(name, description, members, administrators);
    }

    private Collection<JID> getMembers(String groupName, boolean adminsOnly) {
        List<JID> members = new ArrayList<>();

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            if (adminsOnly) {
                if (loadAdminsSQL == null) {
                    return members;
                }
                pstmt = con.prepareStatement(loadAdminsSQL);
            }
            else {
                pstmt = con.prepareStatement(loadMembersSQL);
            }

            pstmt.setString(1, groupName);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String user = rs.getString(1);
                if (user != null) {
                    JID userJID;
                    if (user.contains("@")) {
                        userJID = new JID(user);
                    }
                    else {
                        userJID = server.createJID(user, null);
                    }
                    members.add(userJID);
                }
            }
        }
        catch (SQLException e) {
            Log.error(e.getMessage(), e);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return members;
    }

    @Override
    public int getGroupCount() {
        int count = 0;
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(groupCountSQL);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
            }
        }
        catch (SQLException e) {
            Log.error(e.getMessage(), e);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return count;
    }

    @Override
    public Collection<String> getGroupNames() {
        List<String> groupNames = new ArrayList<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(allGroupsSQL);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                groupNames.add(rs.getString(1));
            }
        }
        catch (SQLException e) {
            Log.error(e.getMessage(), e);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }

    @Override
    public Collection<String> getGroupNames(int start, int num) {
        List<String> groupNames = new ArrayList<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = DbConnectionManager.createScrollablePreparedStatement(con, allGroupsSQL);
            rs = pstmt.executeQuery();
            DbConnectionManager.scrollResultSet(rs, start);
            int count = 0;
            while (rs.next() && count < num) {
                groupNames.add(rs.getString(1));
                count++;
            }
        }
        catch (SQLException e) {
            Log.error(e.getMessage(), e);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }

    @Override
    public Collection<String> getGroupNames(JID user) {
        List<String> groupNames = new ArrayList<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(userGroupsSQL);
            pstmt.setString(1, server.isLocal(user) ? user.getNode() : user.toString());
            rs = pstmt.executeQuery();
            while (rs.next()) {
                groupNames.add(rs.getString(1));
            }
        }
        catch (SQLException e) {
            Log.error(e.getMessage(), e);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }

    @Override
    public Collection<String> getVisibleGroupNames(String userGroup) {
        // If no SQL was defined for this method we stick with the default implementation
        if (StringUtils.isBlank(grouplistContainersSQL)) {
            return super.getVisibleGroupNames(userGroup);
        }

        Set<String> groupNames = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(grouplistContainersSQL);
            pstmt.setString(1, "%" + userGroup + "%");
            rs = pstmt.executeQuery();
            while (rs.next()) {
                groupNames.add(rs.getString(1));
            }
        }
        catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }

    @Override
    public Collection<String> getPublicSharedGroupNames() {
        // If no SQL was defined for this method we stick with the default implementation
        if (StringUtils.isBlank(publicGroupsSQL)) {
            return super.getPublicSharedGroupNames();
        }

        Set<String> groupNames = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(publicGroupsSQL);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                groupNames.add(rs.getString(1));
            }
        }
        catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }

    @Override
    public Collection<String> search(String key, String value) {
        // If no SQL was defined for this method we stick with the default implementation
        if (StringUtils.isBlank(groupsForPropSQL)) {
            return super.search(key, value);
        }

        Set<String> groupNames = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(groupsForPropSQL);
            pstmt.setString(1, key);
            pstmt.setString(2, value);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                groupNames.add(rs.getString(1));
            }
        }
        catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }


    @Override
    public Collection<String> getSharedGroupNames() {
        // If no SQL was defined for this method we stick with the default implementation
        if (StringUtils.isBlank(loadSharedGroupsSQL)) {
            return super.getSharedGroupNames();
        }

        Collection<String> groupNames = new HashSet<>();
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(loadSharedGroupsSQL);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                groupNames.add(rs.getString(1));
            }
        }
        catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return groupNames;
    }

    /**
     * Returns a custom {@link Map} that updates the database whenever
     * a property value is added, changed, or deleted.
     *
     * @param group The target group
     * @return The properties for the given group
     */
    @Override
    public PersistableMap<String,String> loadProperties(Group group) {
        // If no SQL was defined for this method we stick with the default implementation
        if (StringUtils.isBlank(loadPropertiesSQL)) {
            return super.loadProperties(group);
        }

        // custom map implementation persists group property changes
        // whenever one of the standard mutator methods are called
        String name = group.getName();
        PersistableMap<String,String> result;
        if (useConnectionProvider) {
            // We use the default connectionProvider
            result = new DefaultGroupPropertyMap<>(group, deletePropertySQL, deleteAllPropertiesSQL, updatePropertySQL,
                insertPropertySQL, groupPropReadonly);
        } else {
            // We use the connectionProvider specifically configured for the JDBCGroupProvider
            result = new DefaultGroupPropertyMap<>(group, true, deletePropertySQL, deleteAllPropertiesSQL,
                updatePropertySQL, insertPropertySQL, groupPropReadonly);
        }
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            pstmt = con.prepareStatement(loadPropertiesSQL);
            pstmt.setString(1, name);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String key = rs.getString(1);
                String value = rs.getString(2);
                if (key != null) {
                    if (value == null) {
                        result.remove(key);
                        Log.warn("Deleted null property " + key + " for group: " + name);
                    } else {
                        result.put(key, value, false); // skip persistence during load
                    }
                }
                else { // should not happen, but ...
                    Log.warn("Ignoring null property key for group: " + name);
                }
            }
        }
        catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        }
        finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return result;
    }

}
