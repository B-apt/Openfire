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
 * Then you need to set your driver, connection string and SQL statements:
 *
 * <ul>
 * <li>{@code jdbcProvider.driver = com.mysql.jdbc.Driver}</li>
 * <li>{@code jdbcProvider.connectionString = jdbc:mysql://localhost/dbname?user=username&amp;password=secret}</li>
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
 * You can also define properties to load <b>GroupProperties</b> from your external database (below are the default
 * requests as examples). If you don't define them, then the default implementation of the methods will be used
 * (see {@link AbstractGroupProvider}).
 * <ul>
 *     <li>{@code jdbcProvider.grouplistContainersSQL = SELECT groupName FROM ofGroupProp WHERE name='sharedRoster.groupList' AND propValue LIKE ?}</li>
 *     <li>{@code jdbcProvider.publicGroupsSQL = SELECT groupName FROM ofGroupProp WHERE name='sharedRoster.showInRoster' AND propValue='everybody'}</li>
 *     <li>{@code jdbcProvider.groupsForPropSQL = SELECT groupName FROM ofGroupProp WHERE name=? AND propValue=?}</li>
 *     <li>{@code jdbcProvider.loadSharedGroupsSQL = SELECT groupName FROM ofGroupProp WHERE name='sharedRoster.showInRoster' AND propValue IS NOT NULL AND propValue <> 'nobody'}</li>
 *     <li>{@code jdbcGroupProvider.loadPropertiesSQL = SELECT name, propValue FROM ofGroupProp WHERE groupName=?}</li>
 * </ul>
 * <u>Note:</u> If you want to use the possiblity to load GroupProperties from your external database, then check
 * the class {@link JDBCGroupPropertyMap} which allow you to reflect Group properties change into the database.
 *
 * @author David Snopek
 */
public class JDBCGroupProvider extends AbstractGroupProvider {

    private static final Logger Log = LoggerFactory.getLogger(JDBCGroupProvider.class);

    private String connectionString;

    private String groupCountSQL;
    private String descriptionSQL;
    private String allGroupsSQL;
    private String userGroupsSQL;
    private String loadMembersSQL;
    private String loadAdminsSQL;
    private String grouplistContainersSQL;
    private String publicGroupsSQL;
    private String groupsForPropSQL;
    private String loadSharedGroupsSQL;
    private String loadPropertiesSQL;
    private boolean useConnectionProvider;

    private XMPPServer server = XMPPServer.getInstance();  

    /**
     * Constructor of the JDBCGroupProvider class.
     */
    public JDBCGroupProvider() {
        // Convert XML based provider setup to Database based
        JiveGlobals.migrateProperty("jdbcProvider.driver");
        JiveGlobals.migrateProperty("jdbcProvider.connectionString");
        JiveGlobals.migrateProperty("jdbcGroupProvider.groupCountSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.allGroupsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.userGroupsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.descriptionSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.loadMembersSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.loadAdminsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.grouplistContainersSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.publicGroupsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.groupsForPropSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.loadSharedGroupsSQL");
        JiveGlobals.migrateProperty("jdbcGroupProvider.loadPropertiesSQL");

        useConnectionProvider = JiveGlobals.getBooleanProperty("jdbcGroupProvider.useConnectionProvider");

        if (!useConnectionProvider) {
            // Load the JDBC driver and connection string.
            String jdbcDriver = JiveGlobals.getProperty("jdbcProvider.driver");
            try {
                Class.forName(jdbcDriver).newInstance();
            }
            catch (Exception e) {
                Log.error("Unable to load JDBC driver: " + jdbcDriver, e);
                return;
            }
            connectionString = JiveGlobals.getProperty("jdbcProvider.connectionString");
        }

        // Load SQL statements
        groupCountSQL = JiveGlobals.getProperty("jdbcGroupProvider.groupCountSQL");
        allGroupsSQL = JiveGlobals.getProperty("jdbcGroupProvider.allGroupsSQL");
        userGroupsSQL = JiveGlobals.getProperty("jdbcGroupProvider.userGroupsSQL");
        descriptionSQL = JiveGlobals.getProperty("jdbcGroupProvider.descriptionSQL");
        loadMembersSQL = JiveGlobals.getProperty("jdbcGroupProvider.loadMembersSQL");
        loadAdminsSQL = JiveGlobals.getProperty("jdbcGroupProvider.loadAdminsSQL");
        grouplistContainersSQL = JiveGlobals.getProperty("jdbcGroupProvider.grouplistContainersSQL");
        publicGroupsSQL = JiveGlobals.getProperty("jdbcGroupProvider.publicGroupsSQL");
        groupsForPropSQL = JiveGlobals.getProperty("jdbcGroupProvider.groupsForPropSQL");
        loadSharedGroupsSQL = JiveGlobals.getProperty("jdbcGroupProvider.loadSharedGroupsSQL");
        loadPropertiesSQL = JiveGlobals.getProperty("jdbcGroupProvider.loadPropertiesSQL");
    }

    private Connection getConnection() throws SQLException {
        if (useConnectionProvider)
            return DbConnectionManager.getConnection();
        return DriverManager.getConnection(connectionString);
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
            result = new JDBCGroupPropertyMap<>(group);
        } else {
            // We use the connectionProvider specifically configured for the JDBCGroupProvider
            result = new JDBCGroupPropertyMap<>(group, connectionString);
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
